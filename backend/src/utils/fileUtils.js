const { createHash } = require('crypto');
const fs = require('fs');

function computeChecksum(filePath) {
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

function isBinaryBuffer(buffer) {
  const sampleSize = Math.min(buffer.length, 8000);
  for (let i = 0; i < sampleSize; i++) {
    const byte = buffer[i];
    if (byte === 0) return true;
    if (byte < 9 || (byte > 13 && byte < 32 && byte !== 27)) {
      const nullCount = buffer.slice(0, sampleSize).filter((b) => b === 0).length;
      if (nullCount > 0) return true;
    }
  }
  return false;
}

function isBinaryMime(mimeType) {
  if (!mimeType) return true;
  const textTypes = [
    'text/',
    'application/json',
    'application/xml',
    'application/javascript',
    'application/x-javascript',
    'text/csv',
  ];
  return !textTypes.some((t) => mimeType.startsWith(t));
}

function detectBinary(filePath, mimeType) {
  try {
    if (isBinaryMime(mimeType)) return true;
    const buf = fs.readFileSync(filePath);
    return isBinaryBuffer(buf);
  } catch {
    return true;
  }
}

module.exports = { computeChecksum, detectBinary };
