const cron = require('node-cron');
const path = require('path');
const fs = require('fs');
const archiver = require('archiver');
const Database = require('better-sqlite3');
const config = require('../config');
const logger = require('./logger');
const { getDb } = require('../models/database');
const {
  getVersionStorageFileName,
  resolveBackupVersionFilePath,
  resolveVersionStoragePathFromBackups,
  resolveVersionStoragePath,
} = require('./versionStorage');
const BACKUP_METADATA_FILE = 'backup-meta.json';

function isSameOrSubPath(candidate, target) {
  const resolvedCandidate = path.resolve(candidate);
  const resolvedTarget = path.resolve(target);
  return resolvedCandidate === resolvedTarget || resolvedCandidate.startsWith(`${resolvedTarget}${path.sep}`);
}

function getBackupDir() {
  return config.backupDir;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function copyDirRecursive(src, dest, excludePaths = []) {
  ensureDir(dest);
  const entries = fs.readdirSync(src, { withFileTypes: true });
  const resolvedExcludes = excludePaths.map((item) => path.resolve(item));
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    const resolvedSrcPath = path.resolve(srcPath);
    if (resolvedExcludes.some((excluded) => isSameOrSubPath(resolvedSrcPath, excluded))) {
      continue;
    }
    if (entry.isDirectory()) {
      copyDirRecursive(srcPath, destPath, excludePaths);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

function parseBackupCreatedAtFromName(name) {
  const match = /^backup_(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})$/.exec(name || '');
  if (!match) return null;
  const [, datePart, hour, minute, second] = match;
  const parsed = new Date(`${datePart}T${hour}:${minute}:${second}.000Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function readBackupMetadata(backupPath) {
  const metadataPath = path.join(backupPath, BACKUP_METADATA_FILE);
  if (!fs.existsSync(metadataPath)) return null;
  try {
    return JSON.parse(fs.readFileSync(metadataPath, 'utf8'));
  } catch (err) {
    logger.warn('Failed to read backup metadata', { path: metadataPath, error: err.message });
    return null;
  }
}

function writeBackupMetadata(backupPath, metadata) {
  const metadataPath = path.join(backupPath, BACKUP_METADATA_FILE);
  fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2));
}

function getFileSizeSafe(filePath) {
  try {
    return fs.statSync(filePath).size;
  } catch {
    return null;
  }
}

function versionFileSizeMatches(row, filePath) {
  if (!filePath) return false;
  const actualSize = getFileSizeSafe(filePath);
  if (actualSize === null) return false;

  const expectedSize = Number(row.size || 0);
  return expectedSize <= 0 || actualSize === expectedSize;
}

function copyManagedVersionFilesToBackup(backupPath) {
  const db = getDb();
  const rows = db.prepare(`
    SELECT v.id, v.file_id, v.version_number, v.storage_path, v.size
    FROM versions v
    INNER JOIN files f ON v.file_id = f.id
    WHERE f.is_deleted = 0
  `).all();
  let copied = 0;
  let missing = 0;
  let recoveredFromBackups = 0;
  const missingFiles = [];

  for (const row of rows) {
    let sourcePath = resolveVersionStoragePath(row);
    if (!versionFileSizeMatches(row, sourcePath)) {
      sourcePath = resolveVersionStoragePathFromBackups(
        row,
        backupPath,
        (candidate) => versionFileSizeMatches(row, candidate)
      );
      if (sourcePath) recoveredFromBackups += 1;
    }

    if (!sourcePath) {
      missing += 1;
      missingFiles.push({
        versionId: row.id,
        fileId: row.file_id,
        versionNumber: row.version_number,
        storagePath: row.storage_path,
      });
      logger.warn('Backup skipped missing version file', { versionId: row.id, storagePath: row.storage_path });
      continue;
    }

    const destDir = path.join(backupPath, 'uploads', row.file_id);
    ensureDir(destDir);
    fs.copyFileSync(sourcePath, path.join(destDir, getVersionStorageFileName(row)));
    copied += 1;
  }

  return { copied, missing, total: rows.length, recoveredFromBackups, missingFiles };
}

function addManagedVersionFilesToArchive(archive, archivedRoots = []) {
  const db = getDb();
  const rows = db.prepare(`
    SELECT v.id, v.file_id, v.version_number, v.storage_path, v.size
    FROM versions v
    INNER JOIN files f ON v.file_id = f.id
    WHERE f.is_deleted = 0
  `).all();
  for (const row of rows) {
    let sourcePath = resolveVersionStoragePath(row);
    if (!versionFileSizeMatches(row, sourcePath)) {
      sourcePath = resolveVersionStoragePathFromBackups(
        row,
        null,
        (candidate) => versionFileSizeMatches(row, candidate)
      );
    }
    if (!sourcePath) {
      logger.warn('ZIP backup skipped missing version file', { versionId: row.id, storagePath: row.storage_path });
      continue;
    }
    if (archivedRoots.some((root) => isSameOrSubPath(sourcePath, root))) continue;
    archive.file(sourcePath, { name: `uploads/${row.file_id}/${getVersionStorageFileName(row)}` });
  }
}

function runBackup() {
  const now = new Date();
  const timestamp = now.toISOString()
    .replace('T', '_')
    .replace(/:/g, '-')
    .split('.')[0];
  const backupName = `backup_${timestamp}`;
  const backupPath = path.join(getBackupDir(), backupName);

  ensureDir(backupPath);

  // Backup SQLite database — checkpoint WAL first so all data is in the main file
  const dbPath = path.join(config.dataDir, 'plc_control.db');
  if (fs.existsSync(dbPath)) {
    try {
      const db = getDb();
      db.pragma('wal_checkpoint(TRUNCATE)');
    } catch (err) {
      logger.warn('WAL checkpoint failed before backup', { error: err.message });
    }
  }

  const dataDir = path.resolve(config.dataDir);
  const backupDir = path.resolve(getBackupDir());
  const zipBackupDir = path.resolve(config.zipBackupDir);
  const uploadDir = path.resolve(config.uploadDir);

  // Copy the entire data directory except backup destinations to avoid recursion.
  if (fs.existsSync(dataDir)) {
    const entries = fs.readdirSync(dataDir, { withFileTypes: true });
    for (const entry of entries) {
      const srcPath = path.join(dataDir, entry.name);
      const resolvedSrc = path.resolve(srcPath);
      if (resolvedSrc === backupDir || resolvedSrc === zipBackupDir) continue;

      const destPath = path.join(backupPath, entry.name);
      if (entry.isDirectory()) {
        copyDirRecursive(srcPath, destPath, [backupDir, zipBackupDir]);
      } else {
        fs.copyFileSync(srcPath, destPath);
      }
    }
  }

  // If uploads live outside dataDir on this server, snapshot them explicitly so
  // managed file contents are present in the backup viewer/restore flow.
  if (!isSameOrSubPath(uploadDir, dataDir) && fs.existsSync(uploadDir)) {
    copyDirRecursive(uploadDir, path.join(backupPath, 'uploads'), [backupDir, zipBackupDir]);
  }

  const managedFiles = copyManagedVersionFilesToBackup(backupPath);
  if (managedFiles.missing > 0) {
    deleteDirRecursive(backupPath);
    const firstMissing = managedFiles.missingFiles[0];
    const detail = firstMissing
      ? ` First missing: fileId=${firstMissing.fileId}, version=${firstMissing.versionNumber}, storage=${firstMissing.storagePath}`
      : '';
    const err = new Error(
      `Backup aborted: ${managedFiles.missing}/${managedFiles.total} version files were not found in live uploads or existing backups.${detail}`
    );
    err.status = 409;
    err.missingFiles = managedFiles.missingFiles;
    throw err;
  }

  writeBackupMetadata(backupPath, {
    name: backupName,
    createdAt: now.toISOString(),
    sourceDataDir: dataDir,
    sourceUploadDir: uploadDir,
    managedFiles,
    format: 'full-data-snapshot-v1',
  });

  // Calculate backup size
  const size = getDirSize(backupPath);

  logger.info('Backup completed', { name: backupName, size });

  // Apply retention policy
  pruneBackups();

  return { name: backupName, path: backupPath, size, createdAt: now.toISOString() };
}

function getDirSize(dir) {
  let total = 0;
  if (!fs.existsSync(dir)) return total;
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      total += getDirSize(fullPath);
    } else {
      try {
        total += fs.statSync(fullPath).size;
      } catch {}
    }
  }
  return total;
}

function inspectBackupManagedFiles(backupPath) {
  const backupDbPath = path.join(backupPath, 'plc_control.db');
  if (!fs.existsSync(backupDbPath)) {
    return { backupStatus: 'unknown', expectedManagedSize: 0, physicalManagedSize: 0, missingVersionFiles: 0, versionCount: 0 };
  }

  let backupDb;
  try {
    backupDb = new Database(backupDbPath, { readonly: true });
    const hasVersions = backupDb.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='versions'").get();
    const hasFiles = backupDb.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='files'").get();
    if (!hasVersions || !hasFiles) {
      return { backupStatus: 'unknown', expectedManagedSize: 0, physicalManagedSize: 0, missingVersionFiles: 0, versionCount: 0 };
    }

    const rows = backupDb.prepare(`
      SELECT v.id, v.file_id, v.version_number, v.storage_path, v.size
      FROM versions v
      INNER JOIN files f ON v.file_id = f.id
      WHERE f.is_deleted = 0
    `).all();

    let expectedManagedSize = 0;
    let physicalManagedSize = 0;
    let missingVersionFiles = 0;
    for (const row of rows) {
      expectedManagedSize += row.size || 0;
      const found = resolveBackupVersionFilePath(backupPath, row);
      if (!found) {
        missingVersionFiles += 1;
        continue;
      }
      try {
        const actualSize = fs.statSync(found).size;
        physicalManagedSize += actualSize;
        if ((row.size || 0) > 0 && actualSize !== row.size) {
          missingVersionFiles += 1;
        }
      } catch {
        missingVersionFiles += 1;
      }
    }

    const backupStatus = missingVersionFiles === 0
      ? 'complete'
      : (missingVersionFiles === rows.length ? 'missing' : 'partial');
    return { backupStatus, expectedManagedSize, physicalManagedSize, missingVersionFiles, versionCount: rows.length };
  } catch (err) {
    logger.warn('Failed to inspect backup managed files', { path: backupPath, error: err.message });
    return { backupStatus: 'unknown', expectedManagedSize: 0, physicalManagedSize: 0, missingVersionFiles: 0, versionCount: 0 };
  } finally {
    if (backupDb) backupDb.close();
  }
}

function listBackups() {
  const backupDir = getBackupDir();
  ensureDir(backupDir);
  const entries = fs.readdirSync(backupDir, { withFileTypes: true });
  const backups = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.startsWith('backup_')) continue;
    const fullPath = path.join(backupDir, entry.name);
    const stat = fs.statSync(fullPath);
    const metadata = readBackupMetadata(fullPath);
    const size = getDirSize(fullPath);
    const managed = inspectBackupManagedFiles(fullPath);
    backups.push({
      name: entry.name,
      path: fullPath,
      size,
      ...managed,
      createdAt: metadata?.createdAt || parseBackupCreatedAtFromName(entry.name) || stat.birthtime.toISOString(),
    });
  }
  backups.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  return backups;
}

function pruneBackups() {
  const retention = config.backupRetention;
  const backups = listBackups();
  if (backups.length <= retention) return;
  const toDelete = backups.slice(retention);
  for (const backup of toDelete) {
    try {
      deleteDirRecursive(backup.path);
      logger.info('Old backup deleted', { name: backup.name });
    } catch (err) {
      logger.warn('Failed to delete old backup', { name: backup.name, error: err.message });
    }
  }
}

function deleteDirRecursive(dir) {
  if (!fs.existsSync(dir)) return;
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      deleteDirRecursive(fullPath);
    } else {
      fs.unlinkSync(fullPath);
    }
  }
  fs.rmdirSync(dir);
}

function deleteBackup(name) {
  // Validate name to prevent path traversal
  if (!name || !/^backup_[\d_T-]+$/.test(name)) {
    throw new Error('Invalid backup name');
  }
  const backupPath = path.join(getBackupDir(), name);
  const resolvedPath = path.resolve(backupPath);
  const resolvedBase = path.resolve(getBackupDir());
  if (!resolvedPath.startsWith(resolvedBase + path.sep)) {
    throw new Error('Invalid backup path');
  }
  if (!fs.existsSync(backupPath)) {
    throw new Error('Backup not found');
  }
  deleteDirRecursive(backupPath);
}

function scheduleBackup() {
  const schedule = config.backupSchedule;
  cron.schedule(schedule, () => {
    logger.info('Running scheduled backup...');
    try {
      runBackup();
    } catch (err) {
      logger.error('Backup failed', { error: err.message });
    }
  });
  logger.info(`Backup scheduler started (schedule: ${schedule})`);
}

// ── Weekly ZIP export ─────────────────────────────────────────────────────────
// Creates a ZIP archive of the entire data directory and stores it at the
// configured ZIP_BACKUP_DIR. This protects all server data (DB, uploads, caches,
// templates) from being lost when the project is updated/replaced.

/**
 * Create a ZIP archive of the entire data directory (excluding the backup
 * sub-directory itself to avoid redundancy) and save it to zipBackupDir.
 * Returns a promise that resolves with metadata about the created archive.
 */
function createZipBackup() {
  return new Promise((resolve, reject) => {
    const now = new Date();
    const timestamp = now.toISOString()
      .replace('T', '_')
      .replace(/:/g, '-')
      .split('.')[0];
    const zipName = `data_backup_${timestamp}.zip`;

    const zipDir = path.resolve(config.zipBackupDir);
    ensureDir(zipDir);

    const zipPath = path.join(zipDir, zipName);
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 6 } });

    output.on('close', () => {
      const size = archive.pointer();
      logger.info('ZIP backup created', { name: zipName, path: zipPath, size });
      pruneZipBackups();
      resolve({ name: zipName, path: zipPath, size, createdAt: now.toISOString() });
    });

    archive.on('error', (err) => {
      logger.error('ZIP backup archiver error', { error: err.message });
      reject(err);
    });

    archive.on('warning', (err) => {
      if (err.code === 'ENOENT') {
        logger.warn('ZIP backup warning (file not found, skipping)', { error: err.message });
      } else {
        reject(err);
      }
    });

    archive.pipe(output);

    // Checkpoint WAL before archiving the SQLite database
    const dbPath = path.join(config.dataDir, 'plc_control.db');
    if (fs.existsSync(dbPath)) {
      try {
        const db = getDb();
        db.pragma('wal_checkpoint(TRUNCATE)');
      } catch (err) {
        logger.warn('WAL checkpoint failed before ZIP backup', { error: err.message });
      }
    }

    const dataDir = path.resolve(config.dataDir);
    const backupSubDir = path.resolve(config.backupDir);
    const zipBackupSubDir = path.resolve(config.zipBackupDir);
    const uploadDir = path.resolve(config.uploadDir);

    // Add the entire data directory, excluding the directory-based backup
    // folder and the zip_backups folder to avoid redundancy.
    if (fs.existsSync(dataDir)) {
      const entries = fs.readdirSync(dataDir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dataDir, entry.name);
        const resolvedFull = path.resolve(fullPath);

        // Skip the directory-based backups sub-folder and the zip_backups folder
        if (resolvedFull === backupSubDir || resolvedFull === zipBackupSubDir) continue;

        if (entry.isDirectory()) {
          archive.directory(fullPath, entry.name);
        } else {
          archive.file(fullPath, { name: entry.name });
        }
      }
    }

    if (!isSameOrSubPath(uploadDir, dataDir) && fs.existsSync(uploadDir)) {
      archive.directory(uploadDir, 'uploads');
    }

    addManagedVersionFilesToArchive(archive, [dataDir, uploadDir]);

    archive.finalize();
  });
}

function listZipBackups() {
  const zipDir = path.resolve(config.zipBackupDir);
  if (!fs.existsSync(zipDir)) return [];
  const entries = fs.readdirSync(zipDir, { withFileTypes: true });
  const zips = [];
  for (const entry of entries) {
    if (entry.isDirectory() || !entry.name.startsWith('data_backup_') || !entry.name.endsWith('.zip')) continue;
    const fullPath = path.join(zipDir, entry.name);
    try {
      const stat = fs.statSync(fullPath);
      zips.push({ name: entry.name, path: fullPath, size: stat.size, createdAt: stat.birthtime.toISOString() });
    } catch (err) {
      logger.warn('listZipBackups: cannot stat file', { path: fullPath, error: err.message });
    }
  }
  zips.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  return zips;
}

function pruneZipBackups() {
  const retention = config.zipBackupRetention;
  const zips = listZipBackups();
  if (zips.length <= retention) return;
  const toDelete = zips.slice(retention);
  for (const zip of toDelete) {
    try {
      fs.unlinkSync(zip.path);
      logger.info('Old ZIP backup deleted', { name: zip.name });
    } catch (err) {
      logger.warn('Failed to delete old ZIP backup', { name: zip.name, error: err.message });
    }
  }
}

function scheduleZipBackup() {
  const schedule = config.zipBackupSchedule;
  cron.schedule(schedule, () => {
    logger.info('Running scheduled ZIP backup...');
    createZipBackup().catch((err) => {
      logger.error('ZIP backup failed', { error: err.message });
    });
  });
  logger.info(`ZIP backup scheduler started (schedule: ${schedule}, dest: ${config.zipBackupDir})`);
}

module.exports = {
  runBackup,
  listBackups,
  deleteBackup,
  scheduleBackup,
  getDirSize,
  createZipBackup,
  listZipBackups,
  scheduleZipBackup,
};
