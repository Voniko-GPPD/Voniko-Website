const fs = require('fs/promises');
const path = require('path');
const os = require('os');

async function withShadowCopy(sourcePath, callback) {
  if (typeof sourcePath !== 'string' || !sourcePath.trim()) {
    const error = new Error('Invalid sourcePath');
    error.status = 400;
    throw error;
  }

  if (!sourcePath.toLowerCase().endsWith('.mdb')) {
    const error = new Error('Only .mdb files are supported');
    error.status = 400;
    throw error;
  }

  if (!path.isAbsolute(sourcePath)) {
    const error = new Error('sourcePath must be absolute');
    error.status = 400;
    throw error;
  }

  const resolvedSourcePath = path.resolve(sourcePath);
  const configuredBase = process.env.DMP_MDB_DIR;
  if (configuredBase) {
    const resolvedBase = path.resolve(configuredBase);
    const insideBase = resolvedSourcePath === resolvedBase || resolvedSourcePath.startsWith(`${resolvedBase}${path.sep}`);
    if (!insideBase) {
      const error = new Error('MDB path is outside DMP_MDB_DIR');
      error.status = 400;
      throw error;
    }
  }

  const sourceStat = await fs.stat(resolvedSourcePath);
  if (!sourceStat.isFile()) {
    const error = new Error('Source MDB is not a file');
    error.status = 400;
    throw error;
  }

  const sourceName = path.basename(resolvedSourcePath, path.extname(resolvedSourcePath));
  const safeSourceName = sourceName.replace(/[^a-zA-Z0-9._-]/g, '_');
  const shadowName = `${safeSourceName}_shadow_${Date.now()}_${Math.random().toString(16).slice(2)}.mdb`;
  const shadowPath = path.join(os.tmpdir(), shadowName);

  await fs.copyFile(resolvedSourcePath, shadowPath);

  try {
    return await callback(shadowPath);
  } finally {
    await fs.unlink(shadowPath).catch(() => {});
  }
}

module.exports = { withShadowCopy };
