import api from './index';

/**
 * Send a CSV/Excel file to the backend and receive a barcode PDF blob.
 * @param {File} file  - The CSV or Excel file to process
 * @param {Function} [onDownloadProgress] - Optional Axios progress callback
 * @returns {Promise<Blob>}  The PDF blob
 */
export async function generateBarcodePdf(file, onDownloadProgress) {
  const formData = new FormData();
  formData.append('file', file);

  const response = await api.post('/barcode/generate', formData, {
    responseType: 'blob',
    headers: { 'Content-Type': 'multipart/form-data' },
    timeout: 5 * 60 * 1000, // 5 minutes
    ...(onDownloadProgress ? { onDownloadProgress } : {}),
  });

  return response.data;
}
