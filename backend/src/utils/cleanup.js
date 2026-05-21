const cron = require('node-cron');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');
const { getDb } = require('../models/database');
const logger = require('../utils/logger');

function runCleanup() {
  const db = getDb();
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - config.maxRetentionDays);
  const cutoffIso = cutoffDate.toISOString().replace('T', ' ').split('.')[0];

  // Get all file IDs
  const files = db.prepare('SELECT id FROM files').all();

  let deletedCount = 0;

  for (const file of files) {
    // Get versions ordered by version_number desc
    const versions = db
      .prepare('SELECT id, version_number, storage_path, created_at FROM versions WHERE file_id = ? ORDER BY version_number DESC')
      .all(file.id);

    const toDelete = [];

    for (let i = 0; i < versions.length; i++) {
      const v = versions[i];
      // Keep latest N versions, delete rest if over limit or too old
      if (i >= config.maxVersionsPerFile || v.created_at < cutoffIso) {
        toDelete.push(v);
      }
    }

    for (const v of toDelete) {
      // Delete physical file
      try {
        if (fs.existsSync(v.storage_path)) {
          fs.unlinkSync(v.storage_path);
        }
      } catch (err) {
        logger.warn('Failed to delete version file', { path: v.storage_path, error: err.message });
      }

      db.prepare('DELETE FROM versions WHERE id = ?').run(v.id);
      deletedCount++;
    }
  }

  if (deletedCount > 0) {
    logger.info('Cleanup completed', { deletedVersions: deletedCount });
  }

  // Clean up stale MDB cache files that have not been modified (refreshed by
  // dmp_service) in cacheRetentionDays (default: 365 days / 1 year).
  cleanStaleCacheFiles();
}

/**
 * Remove MDB cache files from the known cache directories if they have not
 * been modified in cacheRetentionDays days. The dmp_service updates mtime
 * every time a source MDB file changes, so an old mtime means the source
 * system has not been active for the retention period.
 */
function cleanStaleCacheFiles() {
  const retentionMs = config.cacheRetentionDays * 24 * 60 * 60 * 1000;
  const now = Date.now();

  const cacheDirs = [
    path.join(config.dataDir, 'dm2000_cache'),
    path.join(config.dataDir, 'dmpdata_cache'),
  ];

  for (const cacheDir of cacheDirs) {
    if (!fs.existsSync(cacheDir)) continue;
    let entries;
    try {
      entries = fs.readdirSync(cacheDir, { withFileTypes: true });
    } catch (err) {
      logger.warn('cleanStaleCacheFiles: cannot read cache dir', { dir: cacheDir, error: err.message });
      continue;
    }
    for (const entry of entries) {
      if (entry.isDirectory()) continue;
      const filePath = path.join(cacheDir, entry.name);
      try {
        const stat = fs.statSync(filePath);
        const ageMs = now - stat.mtimeMs;
        if (ageMs > retentionMs) {
          fs.unlinkSync(filePath);
          logger.info('Stale cache file removed', { path: filePath, ageDays: Math.floor(ageMs / 86400000) });
        }
      } catch (err) {
        logger.warn('cleanStaleCacheFiles: error checking/deleting file', { path: filePath, error: err.message });
      }
    }
  }
}

function scheduleCleanup() {
  // Run daily at 2 AM (cron syntax: minute hour day month weekday)
  cron.schedule('0 2 * * *', () => {
    logger.info('Running scheduled cleanup...');
    try {
      runCleanup();
    } catch (err) {
      logger.error('Cleanup failed', { error: err.message });
    }
  });
  logger.info('Cleanup scheduler started (daily at 02:00)');
}

module.exports = { scheduleCleanup, runCleanup };
