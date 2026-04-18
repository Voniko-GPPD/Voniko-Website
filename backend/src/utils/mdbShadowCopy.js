const fs = require('fs/promises');
const path = require('path');
const os = require('os');

async function withShadowCopy(sourcePath, callback) {
  const sourceName = path.basename(sourcePath, path.extname(sourcePath));
  const shadowName = `${sourceName}_shadow_${Date.now()}_${Math.random().toString(16).slice(2)}.mdb`;
  const shadowPath = path.join(os.tmpdir(), shadowName);

  await fs.copyFile(sourcePath, shadowPath);

  try {
    return await callback(shadowPath);
  } finally {
    await fs.unlink(shadowPath).catch(() => {});
  }
}

module.exports = { withShadowCopy };
