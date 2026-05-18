import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
});

// Set auth header from storage on startup
const token = localStorage.getItem('accessToken');
if (token) {
  api.defaults.headers.common['Authorization'] = `Bearer ${token}`;
}

// Response interceptor for token refresh
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;

    if (
      error.response?.status === 401 &&
      !originalRequest._retry &&
      !originalRequest.url.includes('/auth/login')
    ) {
      originalRequest._retry = true;

      const refreshToken = localStorage.getItem('refreshToken');
      if (!refreshToken) {
        localStorage.removeItem('accessToken');
        localStorage.removeItem('refreshToken');
        window.location.href = '/login';
        return Promise.reject(error);
      }

      try {
        const res = await axios.post('/api/auth/refresh', { refreshToken });
        const { accessToken, refreshToken: newRefresh } = res.data;
        localStorage.setItem('accessToken', accessToken);
        localStorage.setItem('refreshToken', newRefresh);
        api.defaults.headers.common['Authorization'] = `Bearer ${accessToken}`;
        originalRequest.headers['Authorization'] = `Bearer ${accessToken}`;
        return api(originalRequest);
      } catch {
        localStorage.removeItem('accessToken');
        localStorage.removeItem('refreshToken');
        window.location.href = '/login';
        return Promise.reject(error);
      }
    }

    return Promise.reject(error);
  }
);

export default api;

export const getFolders = () => api.get('/folders');
export const createFolder = (data) => api.post('/folders', data);
export const updateFolder = (id, data) => api.put(`/folders/${id}`, data);
export const deleteFolder = (id) => api.delete(`/folders/${id}`);

// File lock
export const lockFile = (id, data) => api.post(`/files/${id}/lock`, data);
export const unlockFile = (id) => api.post(`/files/${id}/unlock`);

// Admin backup
export const triggerBackup = () => api.post('/admin/backup');
export const listBackups = () => api.get('/admin/backups');
export const deleteBackup = (name) => api.delete(`/admin/backups/${encodeURIComponent(name)}`);
export const getBackupFiles = (name) => api.get(`/admin/backups/${encodeURIComponent(name)}/files`);
export const restoreBackupFile = (name, data) => api.post(`/admin/backups/${encodeURIComponent(name)}/restore-file`, data);
export const downloadBackupFile = (name, fileId, versionId) =>
  api.post(
    `/admin/backups/${encodeURIComponent(name)}/download-file`,
    { fileId, versionId },
    { responseType: 'blob', timeout: 0 }
  );