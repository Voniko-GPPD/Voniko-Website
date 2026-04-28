import axios from 'axios';

const api = axios.create({ baseURL: '/api/battery' });

// Attach JWT from localStorage
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('accessToken');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export const getPorts = (stationId) => api.get('/ports', { params: { stationId } });
export const getStatus = (stationId) => api.get('/status', { params: { stationId } });
export const checkHealth = (stationId) => api.get('/health', { params: { stationId } });
export const getStations = () => api.get('/stations');
export const downloadReport = (stationId) =>
  api.get('/report/download', { params: { stationId }, responseType: 'blob' });

export const uploadTemplate = (formData) =>
  api.post('/upload-template', formData);

export const getTemplateInfo = () => api.get('/template-info');

export const downloadReportFromTemplate = (records) =>
  api.post('/download-report', { records }, { responseType: 'blob' });

export const uploadArchive = (formData) =>
  api.post('/upload-archive', formData);

export const getArchiveInfo = () => api.get('/archive-info');

export const downloadArchiveReport = (records) =>
  api.post('/download-archive-report', { records }, { responseType: 'blob' });

// Battery Types
export const getBatteryTypes = () => api.get('/types');
export const createBatteryType = (name) => api.post('/types', { name });
export const deleteBatteryType = (id) => api.delete(`/types/${id}`);

// Battery Product Lines
export const getBatteryProductLines = () => api.get('/product-lines');
export const createBatteryProductLine = (name) => api.post('/product-lines', { name });
export const deleteBatteryProductLine = (id) => api.delete(`/product-lines/${id}`);

// Battery Presets
export const getBatteryPresets = () => api.get('/presets');
export const upsertBatteryPreset = (preset) => api.put('/presets', preset);
export const deleteBatteryPreset = (batteryType, productLine) =>
  api.delete(`/presets/${encodeURIComponent(batteryType)}/${encodeURIComponent(productLine)}`);

// Battery Order History
export const getOrderHistory = () => api.get('/order-history');
export const saveOrderHistorySnapshot = (snapshot) => api.post('/order-history', snapshot);
export const deleteOrderHistorySnapshot = (id) => api.delete(`/order-history/${id}`);
export const clearOrderHistory = () => api.delete('/order-history');
