import api from './index';

function withCsv(params = {}) {
  const next = { ...params };
  Object.keys(next).forEach((key) => {
    if (Array.isArray(next[key])) next[key] = next[key].join(',');
    if (next[key] === '' || next[key] === null || next[key] === undefined) delete next[key];
  });
  return next;
}

export const listDictionary = (dictType, params = {}) => api.get(`/qc/dictionaries/${dictType}`, { params });
export const createDictionary = (dictType, payload) => api.post(`/qc/dictionaries/${dictType}`, payload);
export const updateDictionary = (dictType, id, payload) => api.put(`/qc/dictionaries/${dictType}/${id}`, payload);
export const deleteDictionary = (dictType, id) => api.delete(`/qc/dictionaries/${dictType}/${id}`);
export const importDictionaryFile = (dictType, file) => {
  const formData = new FormData();
  formData.append('file', file);
  return api.post(`/qc/dictionaries/${dictType}/import`, formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
};
export const exportDictionaryFile = (dictType) =>
  api.get(`/qc/dictionaries/${dictType}/export`, {
    responseType: 'blob',
  });

export const parseCodes = (payload) => api.post('/qc/parse', payload);
export const createQualityRecord = (payload) => api.post('/qc/quality-records', payload);
export const createQualityRecordWithPhoto = (formData) =>
  api.post('/qc/quality-records/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
export const listQualityRecords = (params = {}) => api.get('/qc/quality-records', { params: withCsv(params) });
export const deleteQualityRecord = (id) => api.delete(`/qc/quality-records/${id}`);
export const getQualityRecordFilterOptions = () => api.get('/qc/quality-records/filter-options');

export const getMonthlySummary = (year, filters = {}) =>
  api.get('/qc/dashboard/monthly-summary', { params: withCsv({ year, ...filters }) });
export const getYearlySummary = (startYear, endYear, filters = {}) =>
  api.get('/qc/dashboard/yearly-summary', {
    params: withCsv({ start_year: startYear, end_year: endYear, ...filters }),
  });
export const getMonthlyPpm = (year, filters = {}) =>
  api.get('/qc/dashboard/monthly-ppm', { params: withCsv({ year, ...filters }) });
export const getRangeSummary = (params = {}) => api.get('/qc/dashboard/range-summary', { params: withCsv(params) });
export const getRangePpm = (params = {}) => api.get('/qc/dashboard/range-ppm', { params: withCsv(params) });

export const listProductionOutputs = (params = {}) =>
  api.get('/qc/production-outputs', { params: withCsv(params) });
export const createProductionOutput = (payload) => api.post('/qc/production-outputs', payload);
export const updateProductionOutput = (id, payload) => api.put(`/qc/production-outputs/${id}`, payload);
export const deleteProductionOutput = (id) => api.delete(`/qc/production-outputs/${id}`);
