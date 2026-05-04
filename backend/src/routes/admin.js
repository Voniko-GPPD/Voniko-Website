const express = require('express');
const path = require('path');
const fs = require('fs');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const Database = require('better-sqlite3');
const bcrypt = require('bcryptjs');
const { authenticateToken, requireAdmin } = require('../middleware/auth');
const { runBackup, listBackups, deleteBackup, getDirSize, createZipBackup, listZipBackups } = require('../utils/backup');
const { getDb } = require('../models/database');
const config = require('../config');
const logger = require('../utils/logger');

// Validate backup name and resolve its path safely
function resolveBackupPath(name) {
  if (!name || !/^backup_[\d_T-]+$/.test(name)) {
    const err = new Error('Invalid backup name');
    err.status = 400;
    throw err;
  }
  const backupPath = path.join(config.backupDir, name);
  const resolvedPath = path.resolve(backupPath);
  const resolvedBase = path.resolve(config.backupDir);
  if (!resolvedPath.startsWith(resolvedBase + path.sep)) {
    const err = new Error('Invalid backup path');
    err.status = 400;
    throw err;
  }
  if (!fs.existsSync(backupPath)) {
    const err = new Error('Backup not found');
    err.status = 404;
    throw err;
  }
  return resolvedPath;
}

// Resolve and validate a physical file path within the backup uploads directory
function resolveBackupFilePath(backupPath, fileId, storagePath) {
  const srcFileName = path.basename(storagePath);
  const srcPath = path.join(backupPath, 'uploads', fileId, srcFileName);
  const resolvedSrc = path.resolve(srcPath);
  if (!resolvedSrc.startsWith(path.resolve(backupPath) + path.sep)) {
    const err = new Error('Invalid file path in backup');
    err.status = 400;
    throw err;
  }
  return srcPath;
}

// POST /api/admin/backup — trigger manual backup
router.post('/backup', authenticateToken, requireAdmin, (req, res) => {
  try {
    const result = runBackup();
    logger.info('Manual backup triggered', { user: req.user.id, backup: result.name });
    res.json({
      message: 'Backup created successfully',
      backup: {
        name: result.name,
        size: result.size,
        createdAt: result.createdAt,
      },
    });
  } catch (err) {
    logger.error('Manual backup failed', { error: err.message });
    res.status(500).json({ message: 'Backup failed', error: err.message });
  }
});

// GET /api/admin/backups — list available backups
router.get('/backups', authenticateToken, requireAdmin, (req, res) => {
  try {
    const backups = listBackups();
    res.json({
      data: backups.map(b => ({
        name: b.name,
        size: b.size,
        createdAt: b.createdAt,
      })),
    });
  } catch (err) {
    logger.error('Failed to list backups', { error: err.message });
    res.status(500).json({ message: 'Failed to list backups', error: err.message });
  }
});

// DELETE /api/admin/backups/:name — delete a specific backup
router.delete('/backups/:name', authenticateToken, requireAdmin, (req, res) => {
  try {
    deleteBackup(req.params.name);
    logger.info('Backup deleted', { user: req.user.id, backup: req.params.name });
    res.json({ message: 'Backup deleted successfully' });
  } catch (err) {
    if (err.message === 'Backup not found') {
      return res.status(404).json({ message: 'Backup not found' });
    }
    if (err.message === 'Invalid backup name' || err.message === 'Invalid backup path') {
      return res.status(400).json({ message: err.message });
    }
    logger.error('Failed to delete backup', { error: err.message });
    res.status(500).json({ message: 'Failed to delete backup', error: err.message });
  }
});

// GET /api/admin/backups/:name/files — list files in backup with comparison to current DB
router.get('/backups/:name/files', authenticateToken, requireAdmin, (req, res) => {
  let backupDb;
  try {
    const backupPath = resolveBackupPath(req.params.name);
    const backupDbPath = path.join(backupPath, 'plc_control.db');
    if (!fs.existsSync(backupDbPath)) {
      return res.status(404).json({ message: 'Backup database not found' });
    }

    backupDb = new Database(backupDbPath, { readonly: true });
    const mainDb = getDb();

    // Check if the backup database has the files table (older backups may be incomplete)
    const hasFilesTable = backupDb.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='files'"
    ).get();
    if (!hasFilesTable) {
      return res.status(422).json({ message: 'Backup database schema is incomplete (missing files table)' });
    }

    const backupFiles = backupDb.prepare(
      'SELECT * FROM files WHERE is_deleted = 0 ORDER BY path, name'
    ).all();

    const files = backupFiles.map(f => {
      const versions = backupDb.prepare(
        `SELECT v.id, v.version_number, v.size, v.commit_message, v.created_at,
                u.display_name as uploaded_by_name
         FROM versions v
         LEFT JOIN users u ON v.uploaded_by = u.id
         WHERE v.file_id = ?
         ORDER BY v.version_number ASC`
      ).all(f.id);

      const totalSize = versions.reduce((sum, v) => sum + (v.size || 0), 0);

      // Compare with current system
      const currentFile = mainDb.prepare(
        'SELECT id, is_deleted FROM files WHERE name = ? AND path = ?'
      ).get(f.name, f.path);

      let existsInCurrent = 'no';
      let currentVersionCount = 0;
      if (currentFile) {
        existsInCurrent = currentFile.is_deleted ? 'deleted' : 'yes';
        if (!currentFile.is_deleted) {
          const cnt = mainDb.prepare(
            'SELECT COUNT(*) as cnt FROM versions WHERE file_id = ?'
          ).get(currentFile.id);
          currentVersionCount = cnt ? cnt.cnt : 0;
        }
      }

      return {
        id: f.id,
        name: f.name,
        path: f.path,
        description: f.description,
        isDeleted: false,
        versionCount: versions.length,
        totalSize,
        versions: versions.map(v => ({
          id: v.id,
          versionNumber: v.version_number,
          size: v.size,
          commitMessage: v.commit_message,
          createdAt: v.created_at,
          uploadedBy: v.uploaded_by_name,
        })),
        existsInCurrent,
        currentVersionCount,
      };
    });

    res.json({ backupName: req.params.name, files });
  } catch (err) {
    if (err.status === 400) return res.status(400).json({ message: err.message });
    if (err.status === 404) return res.status(404).json({ message: err.message });
    logger.error('Failed to read backup files', { error: err.message });
    res.status(500).json({ message: 'Failed to read backup files', error: err.message });
  } finally {
    if (backupDb) backupDb.close();
  }
});

// POST /api/admin/backups/:name/restore-file — restore a file from backup into main system
router.post('/backups/:name/restore-file', authenticateToken, requireAdmin, async (req, res) => {
  let backupDb;
  try {
    const { fileId, versionId, adminPassword } = req.body;
    if (!fileId || !versionId || !adminPassword) {
      return res.status(400).json({ message: 'fileId, versionId, and adminPassword are required' });
    }

    // Verify admin password
    const mainDb = getDb();
    const adminUser = mainDb.prepare('SELECT * FROM users WHERE id = ?').get(req.user.id);
    const isValid = await bcrypt.compare(adminPassword, adminUser.password_hash);
    if (!isValid) {
      return res.status(401).json({ message: 'Invalid admin password' });
    }

    const backupPath = resolveBackupPath(req.params.name);
    const backupDbPath = path.join(backupPath, 'plc_control.db');
    if (!fs.existsSync(backupDbPath)) {
      return res.status(404).json({ message: 'Backup database not found' });
    }

    backupDb = new Database(backupDbPath, { readonly: true });

    const backupVersion = backupDb.prepare('SELECT * FROM versions WHERE id = ?').get(versionId);
    if (!backupVersion) {
      return res.status(404).json({ message: 'Version not found in backup' });
    }

    const backupFile = backupDb.prepare('SELECT * FROM files WHERE id = ?').get(fileId);
    if (!backupFile) {
      return res.status(404).json({ message: 'File not found in backup' });
    }

    // Resolve source file path from backup uploads directory
    const srcPath = resolveBackupFilePath(backupPath, fileId, backupVersion.storage_path);

    if (!fs.existsSync(srcPath)) {
      return res.status(404).json({ message: 'Physical file not found in backup' });
    }

    // Find or create file in current DB
    let currentFile = mainDb.prepare(
      "SELECT * FROM files WHERE name = ? AND path = ? AND is_deleted = 0"
    ).get(backupFile.name, backupFile.path);

    const now = new Date().toISOString();

    if (!currentFile) {
      const newFileId = uuidv4();
      mainDb.prepare(`
        INSERT INTO files (id, name, path, description, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(newFileId, backupFile.name, backupFile.path, backupFile.description, req.user.id, now, now);
      currentFile = mainDb.prepare('SELECT * FROM files WHERE id = ?').get(newFileId);
    }

    // Get next version number
    const lastVersion = mainDb.prepare(
      'SELECT version_number FROM versions WHERE file_id = ? ORDER BY version_number DESC LIMIT 1'
    ).get(currentFile.id);
    const newVersionNumber = lastVersion ? lastVersion.version_number + 1 : 1;

    // Copy physical file to current uploads directory
    const destDir = path.join(config.uploadDir, currentFile.id);
    if (!fs.existsSync(destDir)) {
      fs.mkdirSync(destDir, { recursive: true });
    }
    const newVersionId = uuidv4();
    const destPath = path.join(destDir, `v${newVersionNumber}_${newVersionId}`);
    fs.copyFileSync(srcPath, destPath);

    // Create new version record
    const commitMsg = `Restored from backup ${req.params.name} (original v${backupVersion.version_number})`;
    mainDb.prepare(`
      INSERT INTO versions (id, file_id, version_number, storage_path, size, checksum, mime_type, is_binary, commit_message, uploaded_by, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      newVersionId, currentFile.id, newVersionNumber, destPath,
      backupVersion.size, backupVersion.checksum,
      backupVersion.mime_type || null,
      backupVersion.is_binary || 0,
      commitMsg,
      req.user.id,
      now
    );

    // Update file updated_at
    mainDb.prepare("UPDATE files SET updated_at = ? WHERE id = ?").run(now, currentFile.id);

    // Log activity
    mainDb.prepare(`
      INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details)
      VALUES (?, ?, 'restore_from_backup', 'file', ?, ?, ?)
    `).run(
      uuidv4(), req.user.id, currentFile.id, backupFile.name,
      JSON.stringify({ backupName: req.params.name, versionNumber: newVersionNumber, sourceVersionId: versionId })
    );

    logger.info('File restored from backup', {
      fileId: currentFile.id, fileName: backupFile.name,
      backupName: req.params.name, userId: req.user.id,
    });

    res.json({
      message: 'File restored from backup successfully',
      file: { id: currentFile.id, name: backupFile.name, path: backupFile.path },
      version: { id: newVersionId, versionNumber: newVersionNumber },
    });
  } catch (err) {
    if (err.status === 400) return res.status(400).json({ message: err.message });
    if (err.status === 404) return res.status(404).json({ message: err.message });
    logger.error('Failed to restore file from backup', { error: err.message });
    res.status(500).json({ message: 'Failed to restore file from backup', error: err.message });
  } finally {
    if (backupDb) backupDb.close();
  }
});

// POST /api/admin/backups/:name/download-file — download a file from backup
router.post('/backups/:name/download-file', authenticateToken, requireAdmin, (req, res) => {
  let backupDb;
  try {
    const { fileId, versionId } = req.body;
    if (!fileId || !versionId) {
      return res.status(400).json({ message: 'fileId and versionId are required' });
    }

    const backupPath = resolveBackupPath(req.params.name);
    const backupDbPath = path.join(backupPath, 'plc_control.db');
    if (!fs.existsSync(backupDbPath)) {
      return res.status(404).json({ message: 'Backup database not found' });
    }

    backupDb = new Database(backupDbPath, { readonly: true });

    const backupVersion = backupDb.prepare('SELECT * FROM versions WHERE id = ?').get(versionId);
    if (!backupVersion) {
      return res.status(404).json({ message: 'Version not found in backup' });
    }

    const backupFile = backupDb.prepare('SELECT * FROM files WHERE id = ?').get(fileId);
    if (!backupFile) {
      return res.status(404).json({ message: 'File not found in backup' });
    }

    backupDb.close();
    backupDb = null;

    const srcPath = resolveBackupFilePath(backupPath, fileId, backupVersion.storage_path);

    if (!fs.existsSync(srcPath)) {
      return res.status(404).json({ message: 'Physical file not found in backup' });
    }

    const downloadName = backupFile.name || 'download';
    const fileStat = fs.statSync(srcPath);
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(downloadName)}"`);
    if (backupVersion.mime_type) {
      res.setHeader('Content-Type', backupVersion.mime_type);
    } else {
      res.setHeader('Content-Type', 'application/octet-stream');
    }
    res.setHeader('Content-Length', fileStat.size);

    const stream = fs.createReadStream(srcPath);
    stream.on('error', (streamErr) => {
      logger.error('Failed to stream backup download', {
        error: streamErr.message,
        backupName: req.params.name,
        fileId,
        versionId,
      });
      if (!res.headersSent) {
        res.status(500).json({ message: 'Failed to download file from backup' });
      } else {
        res.destroy(streamErr);
      }
    });
    stream.pipe(res);
  } catch (err) {
    if (backupDb) { try { backupDb.close(); } catch (closeErr) { logger.warn('Failed to close backup DB', { error: closeErr.message }); } backupDb = null; }
    if (err.status === 400) return res.status(400).json({ message: err.message });
    if (err.status === 404) return res.status(404).json({ message: err.message });
    logger.error('Failed to download file from backup', { error: err.message });
    res.status(500).json({ message: 'Failed to download file from backup', error: err.message });
  }
});

// POST /api/admin/zip-backup — trigger a manual ZIP backup of the data directory
router.post('/zip-backup', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const result = await createZipBackup();
    logger.info('Manual ZIP backup triggered', { user: req.user.id, zip: result.name });
    res.json({
      message: 'ZIP backup created successfully',
      backup: {
        name: result.name,
        size: result.size,
        createdAt: result.createdAt,
        path: result.path,
      },
    });
  } catch (err) {
    logger.error('Manual ZIP backup failed', { error: err.message });
    res.status(500).json({ message: 'ZIP backup failed', error: err.message });
  }
});

// GET /api/admin/zip-backups — list available ZIP backups
router.get('/zip-backups', authenticateToken, requireAdmin, (req, res) => {
  try {
    const zips = listZipBackups();
    res.json({
      data: zips.map((z) => ({ name: z.name, size: z.size, createdAt: z.createdAt, path: z.path })),
    });
  } catch (err) {
    logger.error('Failed to list ZIP backups', { error: err.message });
    res.status(500).json({ message: 'Failed to list ZIP backups', error: err.message });
  }
});

module.exports = router;
