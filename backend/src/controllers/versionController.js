const path = require('path');
const fs = require('fs');
const { getDb } = require('../models/database');
const { createUnifiedDiff } = require('../utils/diff');
const { extractTextFromFile, isOfficeFile } = require('../utils/officeExtractor');
const logger = require('../utils/logger');

async function getVersion(req, res) {
  const db = getDb();
  const version = db.prepare(`
    SELECT v.*, u.display_name as uploaded_by_name
    FROM versions v
    LEFT JOIN users u ON v.uploaded_by = u.id
    WHERE v.id = ?
  `).get(req.params.id);

  if (!version) return res.status(404).json({ message: 'Version not found' });

  res.json({
    id: version.id,
    fileId: version.file_id,
    versionNumber: version.version_number,
    size: version.size,
    checksum: version.checksum,
    mimeType: version.mime_type,
    isBinary: !!version.is_binary,
    commitMessage: version.commit_message,
    uploadedBy: version.uploaded_by_name,
    createdAt: version.created_at,
  });
}

async function downloadVersion(req, res) {
  const db = getDb();
  const version = db.prepare(`
    SELECT v.*, f.name as file_name
    FROM versions v
    LEFT JOIN files f ON v.file_id = f.id
    WHERE v.id = ?
  `).get(req.params.id);

  if (!version) return res.status(404).json({ message: 'Version not found' });

  if (!fs.existsSync(version.storage_path)) {
    return res.status(410).json({ message: 'Version file not found on disk' });
  }

  const downloadName = version.file_name || 'download';
  const fileStat = fs.statSync(version.storage_path);

  res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(downloadName)}"`);
  res.setHeader('Content-Type', version.mime_type || 'application/octet-stream');
  res.setHeader('Content-Length', fileStat.size);

  const stream = fs.createReadStream(version.storage_path);
  stream.on('error', (err) => {
    logger.error('Failed to stream version download', { error: err.message, versionId: version.id });
    if (!res.headersSent) {
      res.status(500).json({ message: 'Failed to download file' });
    } else {
      res.destroy(err);
    }
  });
  stream.pipe(res);
}

async function diffVersions(req, res) {
  const { fromId, toId } = req.query;
  if (!fromId || !toId) {
    return res.status(400).json({ message: 'fromId and toId required' });
  }

  const db = getDb();
  const fromVersion = db.prepare('SELECT * FROM versions WHERE id = ?').get(fromId);
  const toVersion = db.prepare('SELECT * FROM versions WHERE id = ?').get(toId);

  if (!fromVersion || !toVersion) {
    return res.status(404).json({ message: 'One or both versions not found' });
  }

  // Both must belong to the same file
  if (fromVersion.file_id !== toVersion.file_id) {
    return res.status(400).json({ message: 'Versions must belong to the same file' });
  }

  // For binary files, try Office text extraction first
  if (fromVersion.is_binary || toVersion.is_binary) {
    const fromMime = fromVersion.mime_type || '';
    const toMime = toVersion.mime_type || '';

    if (isOfficeFile(fromMime) && isOfficeFile(toMime)) {
      try {
        const [fromText, toText] = await Promise.all([
          extractTextFromFile(fromVersion.storage_path, fromMime),
          extractTextFromFile(toVersion.storage_path, toMime),
        ]);

        if (fromText !== null && toText !== null) {
          const diff = createUnifiedDiff(
            fromText,
            toText,
            `v${fromVersion.version_number}`,
            `v${toVersion.version_number}`
          );
          return res.json({
            isBinary: false,
            isOfficeExtracted: true,
            from: {
              id: fromVersion.id,
              versionNumber: fromVersion.version_number,
              size: fromVersion.size,
              createdAt: fromVersion.created_at,
            },
            to: {
              id: toVersion.id,
              versionNumber: toVersion.version_number,
              size: toVersion.size,
              createdAt: toVersion.created_at,
            },
            diff,
          });
        }
      } catch (err) {
        logger.warn('Office text extraction failed, falling back to binary diff', { error: err.message });
      }
    }

    // Fallback: pure binary metadata comparison
    return res.json({
      isBinary: true,
      from: {
        id: fromVersion.id,
        versionNumber: fromVersion.version_number,
        size: fromVersion.size,
        checksum: fromVersion.checksum,
        createdAt: fromVersion.created_at,
      },
      to: {
        id: toVersion.id,
        versionNumber: toVersion.version_number,
        size: toVersion.size,
        checksum: toVersion.checksum,
        createdAt: toVersion.created_at,
      },
    });
  }

  // Text diff
  try {
    const fromContent = fs.readFileSync(fromVersion.storage_path, 'utf8');
    const toContent = fs.readFileSync(toVersion.storage_path, 'utf8');

    const diff = createUnifiedDiff(
      fromContent,
      toContent,
      `v${fromVersion.version_number}`,
      `v${toVersion.version_number}`
    );

    res.json({
      isBinary: false,
      from: {
        id: fromVersion.id,
        versionNumber: fromVersion.version_number,
        size: fromVersion.size,
        createdAt: fromVersion.created_at,
      },
      to: {
        id: toVersion.id,
        versionNumber: toVersion.version_number,
        size: toVersion.size,
        createdAt: toVersion.created_at,
      },
      diff,
    });
  } catch (err) {
    logger.error('Diff failed', { error: err.message });
    res.status(500).json({ message: 'Failed to compute diff' });
  }
}

async function restoreVersion(req, res) {
  const { v4: uuidv4 } = require('uuid');
  const config = require('../config');
  const { computeChecksum, detectBinary } = require('../utils/fileUtils');
  const { broadcast } = require('../utils/notifications');
  const { notifyFileSubscribers } = require('./subscriptionController');

  const db = getDb();
  const version = db.prepare('SELECT * FROM versions WHERE id = ?').get(req.params.id);
  if (!version) return res.status(404).json({ message: 'Version not found' });

  const file = db.prepare('SELECT * FROM files WHERE id = ? AND is_deleted = 0').get(version.file_id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  // Check if already latest version
  const latestVersion = db
    .prepare('SELECT id, checksum, version_number FROM versions WHERE file_id = ? ORDER BY version_number DESC LIMIT 1')
    .get(file.id);

  if (latestVersion && latestVersion.checksum === version.checksum) {
    return res.status(409).json({ message: 'This version is already the latest' });
  }

  // Copy the version file to a new version
  const latestVersionRow = db
    .prepare('SELECT MAX(version_number) as max_ver FROM versions WHERE file_id = ?')
    .get(file.id);
  const newVersionNumber = (latestVersionRow?.max_ver ?? 0) + 1;
  const newVersionId = uuidv4();
  const storageDir = path.join(config.uploadDir, file.id);
  const newStoragePath = path.join(storageDir, `v${newVersionNumber}_${newVersionId}`);

  fs.copyFileSync(version.storage_path, newStoragePath);

  db.prepare(`
    INSERT INTO versions (id, file_id, version_number, storage_path, size, checksum, mime_type, is_binary, commit_message, uploaded_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    newVersionId, file.id, newVersionNumber, newStoragePath,
    version.size, version.checksum, version.mime_type, version.is_binary,
    `Restored from v${version.version_number}`,
    req.user.id
  );

  db.prepare("UPDATE files SET updated_at = datetime('now') || 'Z' WHERE id = ?").run(file.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details)
    VALUES (?, ?, 'restore_version', 'file', ?, ?, ?)
  `).run(
    uuidv4(), req.user.id, file.id, file.name,
    JSON.stringify({ restoredFrom: version.version_number, newVersion: newVersionNumber })
  );

  broadcast({
    type: 'version_restored',
    fileId: file.id,
    fileName: file.name,
    userName: req.user.display_name,
    timestamp: new Date().toISOString(),
  });

  // Notify subscribers
  notifyFileSubscribers(
    file.id,
    file.name,
    req.user.id,
    `File restored: ${file.name}`,
    `${req.user.display_name || req.user.username} restored v${version.version_number} as v${newVersionNumber}`
  );

  res.json({
    message: `Restored to v${version.version_number} as new v${newVersionNumber}`,
    version: { id: newVersionId, versionNumber: newVersionNumber },
  });
}

async function previewVersion(req, res) {
  const db = getDb();
  const version = db.prepare(`
    SELECT v.*, f.name as file_name
    FROM versions v
    LEFT JOIN files f ON v.file_id = f.id
    WHERE v.id = ?
  `).get(req.params.id);

  if (!version) return res.status(404).json({ message: 'Version not found' });

  const mime = version.mime_type || '';
  const isPreviewable = mime.startsWith('image/') || mime.startsWith('video/') || mime.startsWith('audio/');
  if (!isPreviewable) {
    return res.status(415).json({ message: 'Unsupported Media Type: not previewable' });
  }

  if (!fs.existsSync(version.storage_path)) {
    return res.status(410).json({ message: 'Version file not found on disk' });
  }

  const fileStat = fs.statSync(version.storage_path);

  // Support Range requests for video/audio streaming
  const range = req.headers.range;
  if (range && (mime.startsWith('video/') || mime.startsWith('audio/'))) {
    const fileSize = fileStat.size;
    const parts = range.replace(/bytes=/, '').split('-');
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

    if (isNaN(start) || isNaN(end) || start < 0 || end >= fileSize || start > end) {
      res.setHeader('Content-Range', `bytes */${fileSize}`);
      return res.status(416).json({ message: 'Range Not Satisfiable' });
    }

    const chunkSize = end - start + 1;

    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${fileSize}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': chunkSize,
      'Content-Type': mime,
      'Content-Disposition': `inline; filename="${encodeURIComponent(version.file_name || 'preview')}"`,
    });
    const stream = fs.createReadStream(version.storage_path, { start, end });
    stream.on('error', (err) => {
      logger.error('Failed to stream preview', { error: err.message, versionId: version.id });
      if (!res.headersSent) res.status(500).json({ message: 'Failed to stream preview' });
      else res.destroy(err);
    });
    stream.pipe(res);
  } else {
    res.setHeader('Content-Type', mime);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(version.file_name || 'preview')}"`);
    res.setHeader('Content-Length', fileStat.size);

    const stream = fs.createReadStream(version.storage_path);
    stream.on('error', (err) => {
      logger.error('Failed to stream preview', { error: err.message, versionId: version.id });
      if (!res.headersSent) res.status(500).json({ message: 'Failed to stream preview' });
      else res.destroy(err);
    });
    stream.pipe(res);
  }
}

module.exports = { getVersion, downloadVersion, diffVersions, restoreVersion, previewVersion };
