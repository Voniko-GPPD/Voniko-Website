const fs = require('fs');
const path = require('path');
const config = require('../config');

function isSameOrSubPath(candidate, target) {
  const resolvedCandidate = path.resolve(candidate);
  const resolvedTarget = path.resolve(target);
  return resolvedCandidate === resolvedTarget || resolvedCandidate.startsWith(`${resolvedTarget}${path.sep}`);
}

function uniquePaths(paths) {
  const seen = new Set();
  const result = [];
  for (const item of paths) {
    if (!item) continue;
    const normalized = path.resolve(item);
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    result.push(item);
  }
  return result;
}

function safeExists(filePath) {
  try {
    return fs.existsSync(filePath);
  } catch {
    return false;
  }
}

function getCrossPlatformBaseName(filePath) {
  if (!filePath) return '';
  const winName = path.win32.basename(filePath);
  const posixName = path.posix.basename(filePath);
  return winName.length < posixName.length ? winName : posixName;
}

function getVersionStorageFileName(version) {
  const storedName = version?.storage_path ? getCrossPlatformBaseName(version.storage_path) : '';
  if (storedName && storedName !== '.' && storedName !== path.sep && storedName !== '\\') return storedName;
  if (version?.version_number && version?.id) return `v${version.version_number}_${version.id}`;
  return version?.id || '';
}

function getSearchRoots(version) {
  const roots = [
    config.uploadDir,
    path.resolve(config.uploadDir),
    path.join(config.dataDir, 'uploads'),
    path.resolve(config.dataDir, 'uploads'),
  ];

  if (version?.storage_path) {
    roots.push(path.dirname(version.storage_path));
    roots.push(path.resolve(path.dirname(version.storage_path)));
  }

  return uniquePaths(roots).filter((root) => safeExists(root));
}

function findFileByName(root, fileName, excludeRoots = []) {
  if (!root || !fileName || !safeExists(root)) return null;
  const resolvedExcludes = excludeRoots.map((item) => path.resolve(item));
  const stack = [root];

  while (stack.length) {
    const current = stack.pop();
    const resolvedCurrent = path.resolve(current);
    if (resolvedExcludes.some((excluded) => isSameOrSubPath(resolvedCurrent, excluded))) continue;

    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
      } else if (entry.name === fileName) {
        return fullPath;
      }
    }
  }

  return null;
}

function getVersionStorageCandidates(version) {
  const candidates = [];
  const storedPath = version?.storage_path || '';
  const fileId = version?.file_id || version?.fileId;
  const fileName = getVersionStorageFileName(version);

  if (storedPath) {
    candidates.push(storedPath);
    candidates.push(path.resolve(storedPath));
  }

  if (fileId && fileName) {
    candidates.push(path.join(config.uploadDir, fileId, fileName));
    candidates.push(path.resolve(config.uploadDir, fileId, fileName));
    candidates.push(path.join(config.dataDir, 'uploads', fileId, fileName));
    candidates.push(path.resolve(config.dataDir, 'uploads', fileId, fileName));
  }

  return uniquePaths(candidates);
}

function resolveVersionStoragePath(version) {
  const directMatch = getVersionStorageCandidates(version).find((candidate) => safeExists(candidate));
  if (directMatch) return directMatch;

  const fileName = getVersionStorageFileName(version);
  const excludeRoots = [config.backupDir, config.zipBackupDir];
  for (const root of getSearchRoots(version)) {
    const found = findFileByName(root, fileName, excludeRoots);
    if (found) return found;
  }

  return null;
}

function getBackupVersionFileCandidates(backupPath, version) {
  const fileId = version?.file_id || version?.fileId;
  const fileName = getVersionStorageFileName(version);
  const candidates = [];

  if (fileId && fileName) {
    candidates.push(path.join(backupPath, 'uploads', fileId, fileName));
  }

  const storedPath = version?.storage_path || '';
  if (storedPath) {
    const resolvedStored = path.resolve(storedPath);
    const dataDir = path.resolve(config.dataDir);
    if (isSameOrSubPath(resolvedStored, dataDir)) {
      candidates.push(path.join(backupPath, path.relative(dataDir, resolvedStored)));
    }

    const normalizedStored = storedPath.replace(/\\/g, '/');
    const marker = fileId && fileName ? `uploads/${fileId}/${fileName}` : null;
    if (marker && normalizedStored.includes(marker)) {
      candidates.push(path.join(backupPath, 'uploads', fileId, fileName));
    }
  }

  return uniquePaths(candidates).filter((candidate) => isSameOrSubPath(candidate, backupPath));
}

function resolveBackupVersionFilePath(backupPath, version) {
  const directMatch = getBackupVersionFileCandidates(backupPath, version).find((candidate) => safeExists(candidate));
  if (directMatch) return directMatch;
  return findFileByName(backupPath, getVersionStorageFileName(version)) || null;
}

function getBackupRoots(excludedBackupPath = null) {
  if (!safeExists(config.backupDir)) return [];

  const resolvedExcluded = excludedBackupPath ? path.resolve(excludedBackupPath) : null;
  const roots = [];

  let entries = [];
  try {
    entries = fs.readdirSync(config.backupDir, { withFileTypes: true });
  } catch {
    return roots;
  }

  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.startsWith('backup_')) continue;

    const fullPath = path.join(config.backupDir, entry.name);
    const resolvedFullPath = path.resolve(fullPath);
    if (resolvedExcluded && resolvedFullPath === resolvedExcluded) continue;

    let mtimeMs = 0;
    try {
      mtimeMs = fs.statSync(fullPath).mtimeMs;
    } catch {
      // Keep the path as a fallback source even if stat fails.
    }

    roots.push({ path: fullPath, mtimeMs });
  }

  return roots
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
    .map((item) => item.path);
}

function resolveVersionStoragePathFromBackups(version, excludedBackupPath = null, isValid = null) {
  for (const backupPath of getBackupRoots(excludedBackupPath)) {
    const found = resolveBackupVersionFilePath(backupPath, version);
    if (found && (!isValid || isValid(found))) return found;
  }

  return null;
}

module.exports = {
  getBackupVersionFileCandidates,
  getVersionStorageCandidates,
  getVersionStorageFileName,
  isSameOrSubPath,
  resolveBackupVersionFilePath,
  resolveVersionStoragePathFromBackups,
  resolveVersionStoragePath,
};
