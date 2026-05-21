import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import api from '../api';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const token = localStorage.getItem('accessToken');
    if (token) {
      api.get('/auth/me')
        .then(res => setUser(res.data))
        .catch(() => {
          localStorage.removeItem('accessToken');
          localStorage.removeItem('refreshToken');
        })
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  const login = useCallback(async (username, password) => {
    const res = await api.post('/auth/login', { username, password });
    const { accessToken, refreshToken, user: userData } = res.data;
    localStorage.setItem('accessToken', accessToken);
    localStorage.setItem('refreshToken', refreshToken);
    api.defaults.headers.common['Authorization'] = `Bearer ${accessToken}`;
    setUser(userData);
    return userData;
  }, []);

  const logout = useCallback(async () => {
    const refreshToken = localStorage.getItem('refreshToken');
    try {
      await api.post('/auth/logout', { refreshToken });
    } catch {}
    localStorage.removeItem('accessToken');
    localStorage.removeItem('refreshToken');
    delete api.defaults.headers.common['Authorization'];
    setUser(null);
  }, []);

  const refreshUser = useCallback(async () => {
    try {
      const res = await api.get('/auth/me');
      setUser(res.data);
    } catch {}
  }, []);

  const isAdmin = user?.role === 'admin';
  const isViewer = user?.role === 'viewer';
  const canEdit = user?.role === 'admin' || user?.role === 'engineer';
  const isEngineer = user?.role === 'engineer';
  const isQC = user?.role === 'qc';
  const isQCManager = user?.role === 'qc_manager';
  const isLab = user?.role === 'lab';
  const isLabManager = user?.role === 'lab_manager';
  const canManageQCDictionaries = isAdmin || isQCManager;
  const canImportQCDictionaries = canManageQCDictionaries;
  const canExportQCDictionaries = canManageQCDictionaries || isQC;
  const canMutateQCDictionaries = canManageQCDictionaries;
  const canDeleteQCDashboardData = !isQC;
  const canAccessFiles = isAdmin || isEngineer;
  const canAccessBarcode = isAdmin || isEngineer;
  const canAccessBattery = isAdmin || isQC || isQCManager;
  const canAccessDM = isAdmin || isLab || isLabManager;
  const canAccessCounter = isAdmin || isEngineer;
  const canAccessQC = isAdmin || isQC || isQCManager;

  return (
    <AuthContext.Provider value={{
      user,
      loading,
      login,
      logout,
      refreshUser,
      isAdmin,
      isViewer,
      canEdit,
      isEngineer,
      isQC,
      isQCManager,
      isLab,
      isLabManager,
      canManageQCDictionaries,
      canImportQCDictionaries,
      canExportQCDictionaries,
      canMutateQCDictionaries,
      canDeleteQCDashboardData,
      canAccessFiles,
      canAccessBarcode,
      canAccessBattery,
      canAccessDM,
      canAccessCounter,
      canAccessQC,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
