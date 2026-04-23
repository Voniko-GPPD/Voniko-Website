import axios from 'axios';

const api = axios.create({ baseURL: '/api/count-batteries' });

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('accessToken');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

/**
 * Upload an image and run battery AI detection.
 * @param {File} file - image file
 * @param {number} confidence - detection confidence threshold 0.1-1.0
 * @param {boolean} saveResult - persist record in DB
 * @param {string|null} poNumber - optional Purchase Order number
 */
export const predict = (file, confidence = 0.5, saveResult = true, poNumber = null) => {
  const form = new FormData();
  form.append('file', file);
  form.append('confidence', confidence);
  form.append('save_result', saveResult ? 'true' : 'false');
  if (poNumber) form.append('po_number', poNumber);
  return api.post('/predict', form);
};

/**
 * Fetch detection history with optional filters.
 * Returns response with X-Total-Count header.
 */
export const getHistory = (params = {}) =>
  api.get('/history', { params });

/** Get summary statistics */
export const getStats = () => api.get('/history/stats');

/** Download Excel export */
export const exportExcel = (params = {}) =>
  api.get('/history/export/excel', { params, responseType: 'blob' });

/** Delete a single record */
export const deleteRecord = (id) => api.delete(`/history/${id}`);

/** Delete multiple records */
export const deleteBatch = (ids) =>
  api.delete('/history/batch', { data: { record_ids: ids } });

/** Health check */
export const checkHealth = () => api.get('/health');
