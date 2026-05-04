const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../models/database');
const config = require('../config');
const { computeChecksum, detectBinary } = require('../utils/fileUtils');
const logger = require('../utils/logger');
const { runCleanup } = require('../utils/cleanup');
const { broadcast } = require('../utils/notifications');
const { notifyFileSubscribers } = require('./subscriptionController');

async function listFiles(req, res) {
  const db = getDb();
  const { search, page = 1, limit = 50, folderId } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);

  let query = `
    SELECT f.*, u.display_name as creator_name,
      (SELECT COUNT(*) FROM versions v WHERE v.file_id = f.id) as version_count,
      (SELECT v2.created_at FROM versions v2 WHERE v2.file_id = f.id ORDER BY v2.version_number DESC LIMIT 1) as last_modified,
      (SELECT v3.size FROM versions v3 WHERE v3.file_id = f.id ORDER BY v3.version_number DESC LIMIT 1) as current_size,
      fo.name as folder_name,
      pfo.name as parent_folder_name
    FROM files f
    LEFT JOIN users u ON f.created_by = u.id
    LEFT JOIN folders fo ON f.folder_id = fo.id
    LEFT JOIN folders pfo ON fo.parent_id = pfo.id
    WHERE f.is_deleted = 0
  `;

  const params = [];
  if (search) {
    query += ' AND (f.name LIKE ? OR f.path LIKE ? OR f.description LIKE ?)';
    params.push(`%${search}%`, `%${search}%`, `%${search}%`);
  }
  if (folderId) {
    query += ' AND f.folder_id = ?';
    params.push(folderId);
  }

  const total = db.prepare(`SELECT COUNT(*) as cnt FROM (${query})`).get(...params).cnt;

  query += ' ORDER BY f.updated_at DESC LIMIT ? OFFSET ?';
  params.push(parseInt(limit), offset);

  const files = db.prepare(query).all(...params);

  res.json({
    data: files.map(f => {
      let folderPath = null;
      if (f.folder_name) {
        folderPath = f.parent_folder_name
          ? `${f.parent_folder_name} / ${f.folder_name}`
          : f.folder_name;
      }
      const tags = db.prepare(`
        SELECT t.id, t.name, t.color FROM tags t
        JOIN file_tags ft ON ft.tag_id = t.id
        WHERE ft.file_id = ?
      `).all(f.id);
      const sub = db.prepare('SELECT id FROM file_subscriptions WHERE file_id = ? AND user_id = ?').get(f.id, req.user.id);
      return {
        id: f.id,
        name: f.name,
        path: f.path,
        description: f.description,
        folderId: f.folder_id || null,
        folderName: f.folder_name || null,
        folderPath,
        versionCount: f.version_count,
        lastModified: f.last_modified,
        currentSize: f.current_size,
        createdBy: f.creator_name,
        createdById: f.created_by,
        createdAt: f.created_at,
        lockedBy: f.locked_by || null,
        lockedAt: f.locked_at || null,
        lockReason: f.lock_reason || null,
        tags,
        isSubscribed: !!sub,
      };
    }),
    total,
    page: parseInt(page),
    limit: parseInt(limit),
  });
}

async function uploadFile(req, res) {
  if (!req.file) {
    return res.status(400).json({ message: 'No file uploaded' });
  }

  const db = getDb();
  const { commitMessage, description, filePath = '/', folderId, fileId, customFileName } = req.body;
  const originalName = customFileName
    ? customFileName.trim()
    : Buffer.from(req.file.originalname, 'latin1').toString('utf8');

  // Compute normalizedPath: derive from folder if folderId given
  let normalizedPath = filePath.startsWith('/') ? filePath : '/' + filePath;
  if (folderId) {
    const folder = db.prepare(`
      SELECT f.name as folder_name, pf.name as line_name
      FROM folders f
      LEFT JOIN folders pf ON f.parent_id = pf.id
      WHERE f.id = ? AND f.is_deleted = 0
    `).get(folderId);
    if (!folder) {
      fs.unlinkSync(req.file.path);
      return res.status(404).json({ message: 'Folder not found' });
    }
    normalizedPath = folder.line_name
      ? `/${folder.line_name}/${folder.folder_name}`
      : `/${folder.folder_name}`;
  }

  // If fileId is provided, look up by stable UUID (upload new version to a specific file)
  let file;
  if (fileId) {
    file = db.prepare("SELECT * FROM files WHERE id = ? AND is_deleted = 0").get(fileId);
    if (!file) {
      try { fs.unlinkSync(req.file.path); } catch (e) { logger.warn('Temp file cleanup failed', { error: e.message }); }
      return res.status(404).json({ message: 'File not found for update' });
    }
  } else {
    // Fallback: find by name + path (original behavior)
    file = db
      .prepare("SELECT * FROM files WHERE name = ? AND path = ? AND is_deleted = 0")
      .get(originalName, normalizedPath);
  }

  // Check lock status for existing file
  if (file && file.locked_by && file.locked_by !== req.user.id) {
    try { fs.unlinkSync(req.file.path); } catch {}
    const locker = db.prepare('SELECT display_name FROM users WHERE id = ?').get(file.locked_by);
    return res.status(423).json({
      message: 'File is locked by another user',
      lockedBy: locker?.display_name || file.locked_by,
      lockedAt: file.locked_at,
      lockReason: file.lock_reason,
    });
  }

  let isNewFile = false;
  if (!file) {
    // Create new file record
    file = {
      id: uuidv4(),
      name: originalName,
      path: normalizedPath,
      description: description || null,
      created_by: req.user.id,
      folder_id: folderId || null,
    };
    db.prepare(`
      INSERT INTO files (id, name, path, description, created_by, folder_id)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(file.id, file.name, file.path, file.description, file.created_by, file.folder_id);
    isNewFile = true;
  } else if (description !== undefined) {
    db.prepare("UPDATE files SET description = ?, updated_at = datetime('now') || 'Z' WHERE id = ?")
      .run(description, file.id);
  }

  // Get next version number
  const lastVersion = db
    .prepare('SELECT version_number FROM versions WHERE file_id = ? ORDER BY version_number DESC LIMIT 1')
    .get(file.id);
  const versionNumber = lastVersion ? lastVersion.version_number + 1 : 1;

  // Compute checksum
  const checksum = await computeChecksum(req.file.path);

  // Check if content is identical to last version
  if (!isNewFile) {
    const lastVer = db
      .prepare('SELECT checksum FROM versions WHERE file_id = ? ORDER BY version_number DESC LIMIT 1')
      .get(file.id);
    if (lastVer && lastVer.checksum === checksum) {
      // Cleanup the temp uploaded file
      fs.unlinkSync(req.file.path);
      return res.status(409).json({ message: 'File content is identical to the latest version' });
    }
  }

  // Determine binary type
  const isBinary = detectBinary(req.file.path, req.file.mimetype);

  // Move file to versioned storage
  const versionId = uuidv4();
  const storageDir = path.join(config.uploadDir, file.id);
  if (!fs.existsSync(storageDir)) {
    fs.mkdirSync(storageDir, { recursive: true });
  }
  const storagePath = path.join(storageDir, `v${versionNumber}_${versionId}`);
  fs.copyFileSync(req.file.path, storagePath);
  try { fs.unlinkSync(req.file.path); } catch {}

  // Create version record
  db.prepare(`
    INSERT INTO versions (id, file_id, version_number, storage_path, size, checksum, mime_type, is_binary, commit_message, uploaded_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    versionId, file.id, versionNumber, storagePath,
    req.file.size, checksum,
    req.file.mimetype || null,
    isBinary ? 1 : 0,
    commitMessage || null,
    req.user.id
  );

  // Update file updated_at
  db.prepare("UPDATE files SET updated_at = datetime('now') || 'Z' WHERE id = ?").run(file.id);

  // Log activity
  const action = isNewFile ? 'add_file' : 'update_file';
  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details)
    VALUES (?, ?, ?, 'file', ?, ?, ?)
  `).run(
    uuidv4(), req.user.id, action, file.id, originalName,
    JSON.stringify({ versionNumber, commitMessage })
  );

  logger.info('File uploaded', { fileId: file.id, fileName: originalName, version: versionNumber, user: req.user.id });

  // Auto-unlock if file was locked by the uploader
  if (!isNewFile && file.locked_by === req.user.id) {
    db.prepare("UPDATE files SET locked_by = NULL, locked_at = NULL, lock_reason = NULL WHERE id = ?").run(file.id);
    db.prepare(`
      INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
      VALUES (?, ?, 'unlock_file', 'file', ?, ?)
    `).run(uuidv4(), req.user.id, file.id, originalName);
  }

  // Broadcast notification
  broadcast({
    type: isNewFile ? 'file_added' : 'file_updated',
    fileId: file.id,
    fileName: originalName,
    userName: req.user.display_name,
    timestamp: new Date().toISOString(),
  });

  // Notify subscribers (if updating existing file)
  if (!isNewFile) {
    const uploaderName = req.user.display_name || req.user.username;
    notifyFileSubscribers(
      file.id,
      originalName,
      req.user.id,
      `File updated: ${originalName}`,
      `${uploaderName} uploaded version v${versionNumber}${commitMessage ? ': ' + commitMessage : ''}`
    );
  }

  // Run cleanup asynchronously
  setImmediate(() => {
    try { runCleanup(); } catch (err) { logger.warn('Post-upload cleanup failed', { error: err.message }); }
  });

  res.status(isNewFile ? 201 : 200).json({
    message: isNewFile ? 'File added successfully' : 'File updated successfully',
    file: { id: file.id, name: originalName, path: normalizedPath },
    version: { id: versionId, versionNumber, isBinary: !!isBinary },
  });
}

async function getFile(req, res) {
  const db = getDb();
  const file = db.prepare(`
    SELECT f.*, u.display_name as creator_name,
      fo.name as folder_name, fo.type as folder_type,
      pfo.id as line_id, pfo.name as line_name
    FROM files f
    LEFT JOIN users u ON f.created_by = u.id
    LEFT JOIN folders fo ON f.folder_id = fo.id
    LEFT JOIN folders pfo ON fo.parent_id = pfo.id
    WHERE f.id = ? AND f.is_deleted = 0
  `).get(req.params.id);

  if (!file) return res.status(404).json({ message: 'File not found' });

  const versions = db.prepare(`
    SELECT v.id, v.version_number, v.size, v.checksum, v.mime_type, v.is_binary,
           v.commit_message, v.created_at, u.display_name as uploaded_by_name, u.id as uploaded_by_id
    FROM versions v
    LEFT JOIN users u ON v.uploaded_by = u.id
    WHERE v.file_id = ?
    ORDER BY v.version_number DESC
  `).all(file.id);

  let folderPath = null;
  if (file.folder_name) {
    folderPath = file.line_name
      ? `${file.line_name} / ${file.folder_name}`
      : file.folder_name;
  }

  // Fetch locker display name if locked
  let lockedByName = null;
  if (file.locked_by) {
    const locker = db.prepare('SELECT display_name FROM users WHERE id = ?').get(file.locked_by);
    lockedByName = locker?.display_name || null;
  }

  // Fetch tags for this file
  const fileTags = db.prepare(`
    SELECT t.id, t.name, t.color FROM tags t
    JOIN file_tags ft ON ft.tag_id = t.id
    WHERE ft.file_id = ?
  `).all(file.id);

  // Check if current user is subscribed
  const sub = db.prepare('SELECT id FROM file_subscriptions WHERE file_id = ? AND user_id = ?').get(file.id, req.user.id);

  res.json({
    id: file.id,
    name: file.name,
    path: file.path,
    description: file.description,
    folderId: file.folder_id || null,
    folderName: file.folder_name || null,
    folderPath,
    lineId: file.line_id || null,
    lineName: file.line_name || null,
    createdBy: file.creator_name,
    createdAt: file.created_at,
    updatedAt: file.updated_at,
    lockedBy: file.locked_by || null,
    lockedByName,
    lockedAt: file.locked_at || null,
    lockReason: file.lock_reason || null,
    tags: fileTags,
    isSubscribed: !!sub,
    versions: versions.map(v => ({
      id: v.id,
      versionNumber: v.version_number,
      size: v.size,
      checksum: v.checksum,
      mimeType: v.mime_type,
      isBinary: !!v.is_binary,
      commitMessage: v.commit_message,
      uploadedBy: v.uploaded_by_name,
      uploadedById: v.uploaded_by_id,
      createdAt: v.created_at,
    })),
  });
}

async function deleteFile(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT * FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  // Only admin can delete any file; engineer/user can only delete their own files
  const canDelete = req.user.role === 'admin' ||
    (['engineer', 'user'].includes(req.user.role) && file.created_by === req.user.id);
  if (!canDelete) {
    return res.status(403).json({ message: 'Not authorized to delete this file' });
  }

  // Delete physical version files from disk
  const versions = db.prepare('SELECT storage_path FROM versions WHERE file_id = ?').all(file.id);
  for (const v of versions) {
    try { fs.unlinkSync(v.storage_path); } catch (err) {
      logger.warn('Failed to delete version file', { path: v.storage_path, error: err.message });
    }
  }
  // Remove the file's storage directory
  const storageDir = path.join(config.uploadDir, file.id);
  try { fs.rmSync(storageDir, { recursive: true }); } catch (err) {
    logger.warn('Failed to remove storage directory', { dir: storageDir, error: err.message });
  }

  // Delete child records in correct FK order
  // 1. Delete version_comments for all versions of this file
  const versionIds = db.prepare('SELECT id FROM versions WHERE file_id = ?').all(file.id);
  for (const v of versionIds) {
    db.prepare('DELETE FROM version_comments WHERE version_id = ?').run(v.id);
  }
  // 2. Delete file_tags
  db.prepare('DELETE FROM file_tags WHERE file_id = ?').run(file.id);
  // 3. Delete file_subscriptions
  db.prepare('DELETE FROM file_subscriptions WHERE file_id = ?').run(file.id);
  // 4. Delete version records
  db.prepare('DELETE FROM versions WHERE file_id = ?').run(file.id);

  db.prepare(`
    UPDATE files SET is_deleted = 1, deleted_by = ?, deleted_at = datetime('now') || 'Z', updated_at = datetime('now') || 'Z'
    WHERE id = ?
  `).run(req.user.id, file.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'delete_file', 'file', ?, ?)
  `).run(uuidv4(), req.user.id, file.id, file.name);

  logger.info('File deleted', { fileId: file.id, fileName: file.name, userId: req.user.id });

  // Broadcast notification
  broadcast({
    type: 'file_deleted',
    fileId: file.id,
    fileName: file.name,
    userName: req.user.display_name,
    timestamp: new Date().toISOString(),
  });

  res.json({ message: 'File deleted' });
}

async function getActivityLog(req, res) {
  const db = getDb();
  const { page = 1, limit = 50, fileId } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);

  let query = `
    SELECT a.*, u.display_name as user_name, u.username, u.avatar_url
    FROM activity_log a
    LEFT JOIN users u ON a.user_id = u.id
  `;
  const params = [];

  if (fileId) {
    query += ' WHERE a.entity_id = ?';
    params.push(fileId);
  }

  const total = db.prepare(`SELECT COUNT(*) as cnt FROM (${query})`).get(...params).cnt;

  query += ' ORDER BY a.created_at DESC LIMIT ? OFFSET ?';
  params.push(parseInt(limit), offset);

  const logs = db.prepare(query).all(...params);

  res.json({
    data: logs.map(l => ({
      id: l.id,
      userId: l.user_id,
      userName: l.user_name,
      username: l.username,
      avatarUrl: l.avatar_url,
      action: l.action,
      entityType: l.entity_type,
      entityId: l.entity_id,
      entityName: l.entity_name,
      details: l.details ? JSON.parse(l.details) : null,
      createdAt: l.created_at,
    })),
    total,
    page: parseInt(page),
    limit: parseInt(limit),
  });
}

async function getDashboardStats(req, res) {
  const db = getDb();

  const stats = {
    totalFiles: db.prepare('SELECT COUNT(*) as cnt FROM files WHERE is_deleted = 0').get().cnt,
    totalVersions: db.prepare(
      'SELECT COUNT(*) as cnt FROM versions v INNER JOIN files f ON v.file_id = f.id WHERE f.is_deleted = 0'
    ).get().cnt,
    totalUsers: db.prepare('SELECT COUNT(*) as cnt FROM users WHERE is_active = 1').get().cnt,
    totalSize: db.prepare(
      'SELECT COALESCE(SUM(v.size), 0) as total FROM versions v INNER JOIN files f ON v.file_id = f.id WHERE f.is_deleted = 0'
    ).get().total || 0,
  };

  const recentActivity = db.prepare(`
    SELECT a.*, u.display_name as user_name, u.avatar_url
    FROM activity_log a
    LEFT JOIN users u ON a.user_id = u.id
    ORDER BY a.created_at DESC
    LIMIT 10
  `).all();

  res.json({
    stats,
    recentActivity: recentActivity.map(l => ({
      id: l.id,
      userName: l.user_name,
      avatarUrl: l.avatar_url,
      action: l.action,
      entityName: l.entity_name,
      createdAt: l.created_at,
    })),
  });
}

async function exportActivityLog(req, res) {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Admin access required' });
  }

  const db = getDb();
  const { from, to, userId, fileId } = req.query;

  let query = `
    SELECT a.created_at, u.display_name as user_name, a.action, a.entity_type, a.entity_name, a.details
    FROM activity_log a
    LEFT JOIN users u ON a.user_id = u.id
    WHERE 1=1
  `;
  const params = [];

  if (from) { query += ' AND a.created_at >= ?'; params.push(from); }
  if (to) { query += ' AND a.created_at <= ?'; params.push(to); }
  if (userId) { query += ' AND a.user_id = ?'; params.push(userId); }
  if (fileId) { query += ' AND a.entity_id = ?'; params.push(fileId); }

  query += ' ORDER BY a.created_at DESC';

  const rows = db.prepare(query).all(...params);

  const actionMap = {
    add_file: 'Add File', update_file: 'Update File', delete_file: 'Delete File',
    restore_version: 'Restore Version', lock_file: 'Lock File', unlock_file: 'Unlock File',
    create_user: 'Create User', update_user: 'Update User', delete_user: 'Delete User',
    login: 'Login', logout: 'Logout', subscribe_file: 'Subscribe', unsubscribe_file: 'Unsubscribe',
    add_comment: 'Add Comment', create_tag: 'Create Tag', delete_tag: 'Delete Tag', add_file_tags: 'Add Tags',
  };

  const escapeCSV = (val) => {
    if (val === null || val === undefined) return '';
    const str = String(val);
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
      return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
  };

  const headers = ['Thời gian', 'Người dùng', 'Hành động', 'Loại đối tượng', 'Tên đối tượng', 'Chi tiết'];
  const lines = [headers.map(escapeCSV).join(',')];

  for (const row of rows) {
    const details = row.details ? (() => { try { return JSON.stringify(JSON.parse(row.details)); } catch { return row.details; } })() : '';
    lines.push([
      escapeCSV(row.created_at),
      escapeCSV(row.user_name),
      escapeCSV(actionMap[row.action] || row.action),
      escapeCSV(row.entity_type),
      escapeCSV(row.entity_name),
      escapeCSV(details),
    ].join(','));
  }

  const dateStr = new Date().toISOString().split('T')[0];
  const csv = '\uFEFF' + lines.join('\r\n');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="activity_log_${dateStr}.csv"`);
  res.send(csv);
}

async function lockFile(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT * FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  // Check if already locked by someone else (admin can override)
  if (file.locked_by && file.locked_by !== req.user.id && req.user.role !== 'admin') {
    const locker = db.prepare('SELECT display_name FROM users WHERE id = ?').get(file.locked_by);
    return res.status(423).json({
      message: 'File is already locked by another user',
      lockedBy: locker?.display_name || file.locked_by,
      lockedAt: file.locked_at,
      lockReason: file.lock_reason,
    });
  }

  const { reason } = req.body;
  db.prepare(`
    UPDATE files SET locked_by = ?, locked_at = datetime('now') || 'Z', lock_reason = ? WHERE id = ?
  `).run(req.user.id, reason || null, file.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details)
    VALUES (?, ?, 'lock_file', 'file', ?, ?, ?)
  `).run(uuidv4(), req.user.id, file.id, file.name, JSON.stringify({ reason: reason || null }));

  broadcast({
    type: 'file_locked',
    fileId: file.id,
    fileName: file.name,
    userName: req.user.display_name,
    timestamp: new Date().toISOString(),
  });

  logger.info('File locked', { fileId: file.id, userId: req.user.id });
  res.json({ message: 'File locked successfully' });
}

async function unlockFile(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT * FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  if (!file.locked_by) {
    return res.status(400).json({ message: 'File is not locked' });
  }

  // Only the locker or admin can unlock
  if (file.locked_by !== req.user.id && req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Only the locker or an admin can unlock this file' });
  }

  db.prepare(`
    UPDATE files SET locked_by = NULL, locked_at = NULL, lock_reason = NULL WHERE id = ?
  `).run(file.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'unlock_file', 'file', ?, ?)
  `).run(uuidv4(), req.user.id, file.id, file.name);

  broadcast({
    type: 'file_unlocked',
    fileId: file.id,
    fileName: file.name,
    userName: req.user.display_name,
    timestamp: new Date().toISOString(),
  });

  logger.info('File unlocked', { fileId: file.id, userId: req.user.id });
  res.json({ message: 'File unlocked successfully' });
}

async function renameFile(req, res) {
  const db = getDb();
  const { name } = req.body;
  if (!name || !name.trim()) {
    return res.status(400).json({ message: 'File name is required' });
  }

  const file = db.prepare('SELECT * FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  const canRename = req.user.role === 'admin' ||
    ['engineer', 'user'].includes(req.user.role);
  if (!canRename) {
    return res.status(403).json({ message: 'Not authorized to rename this file' });
  }

  const newName = name.trim();
  db.prepare(`UPDATE files SET name = ?, updated_at = datetime('now') || 'Z' WHERE id = ?`)
    .run(newName, file.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details)
    VALUES (?, ?, 'rename_file', 'file', ?, ?, ?)
  `).run(uuidv4(), req.user.id, file.id, newName, JSON.stringify({ oldName: file.name, newName }));

  logger.info('File renamed', { fileId: file.id, oldName: file.name, newName, userId: req.user.id });

  broadcast({
    type: 'file_renamed',
    fileId: file.id,
    oldName: file.name,
    newName,
    userName: req.user.display_name,
    timestamp: new Date().toISOString(),
  });

  res.json({ message: 'File renamed successfully', file: { id: file.id, name: newName } });
}

module.exports = { listFiles, uploadFile, getFile, deleteFile, getActivityLog, getDashboardStats, lockFile, unlockFile, exportActivityLog, renameFile };