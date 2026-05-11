const STORAGE_KEY = 'pendingUploads';

export function getPendingUploads() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
  } catch {
    return [];
  }
}

export function addPendingUpload({ fileId, fileName, versionId, versionNumber }) {
  const pending = getPendingUploads();
  // Avoid duplicates for the same file
  const exists = pending.some(p => p.fileId === fileId);
  if (!exists) {
    pending.push({ fileId, fileName, versionId, versionNumber, downloadedAt: new Date().toISOString() });
    localStorage.setItem(STORAGE_KEY, JSON.stringify(pending));
  }
}

export function removePendingUpload(fileId) {
  const pending = getPendingUploads().filter(p => p.fileId !== fileId);
  localStorage.setItem(STORAGE_KEY, JSON.stringify(pending));
}

export function clearPendingUploads() {
  localStorage.removeItem(STORAGE_KEY);
}
