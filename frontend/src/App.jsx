import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, Spin } from 'antd';
import viVN from 'antd/locale/vi_VN';
import enUS from 'antd/locale/en_US';
import zhCN from 'antd/locale/zh_CN';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import 'dayjs/locale/vi';
import 'dayjs/locale/zh-cn';

dayjs.extend(relativeTime);

import { AuthProvider, useAuth } from './contexts/AuthContext';
import { LangProvider, useLang } from './contexts/LangContext';
import { NotificationProvider } from './contexts/NotificationContext';

import AppLayout from './components/Layout/AppLayout';
import LoginPage from './pages/Login/LoginPage';
import DashboardPage from './pages/Dashboard/DashboardPage';
import FilesPage from './pages/Files/FilesPage';
import FileDetailPage from './pages/FileDetail/FileDetailPage';
import UsersPage from './pages/Users/UsersPage';
import ProfilePage from './pages/Profile/ProfilePage';
import BackupViewerPage from './pages/BackupViewer/BackupViewerPage';
import BarcodePage from './pages/Barcode/BarcodePage';
import BatteryPage from './pages/Battery/BatteryPage';
import BatteryDMPPage from './pages/BatteryDMP/BatteryDMPPage';
import CountBatteriesPage from './pages/CountBatteries/CountBatteriesPage';
import QCDashboard from './pages/QCDashboard';
import QCEntry from './pages/QCEntry';
import QCDictionaries from './pages/QCDictionaries';

function ProtectedRoute({ children }) {
  const { user, loading } = useAuth();
  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '100vh' }}>
        <Spin size="large" />
      </div>
    );
  }
  if (!user) return <Navigate to="/login" replace />;
  return children;
}

function AdminRoute({ children }) {
  const { user, loading } = useAuth();
  if (loading) return null;
  if (!user || user.role !== 'admin') return <Navigate to="/" replace />;
  return children;
}

function RoleRoute({ children, allowedRoles }) {
  const { user, loading } = useAuth();
  if (loading) return null;
  if (!user || !allowedRoles.includes(user.role)) return <Navigate to="/" replace />;
  return children;
}

function QCRedirect() {
  const { isQC, isQCManager, isLab, isLabManager } = useAuth();
  if (isQC || isQCManager) return <Navigate to="/battery" replace />;
  if (isLab || isLabManager) return <Navigate to="/battery-dmp" replace />;
  return <DashboardPage />;
}

function AppRoutes() {
  const { lang } = useLang();

  // Set dayjs locale
  dayjs.locale(lang === 'vi' ? 'vi' : lang === 'zh' ? 'zh-cn' : 'en');

  const antdLocale = lang === 'vi' ? viVN : lang === 'zh' ? zhCN : enUS;

  return (
    <ConfigProvider
      locale={antdLocale}
      theme={{
        token: {
          colorPrimary: '#1677ff',
          borderRadius: 8,
          fontFamily: '"Inter", "Noto Sans", "Noto Sans SC", sans-serif',
        },
      }}
    >
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <AppLayout />
              </ProtectedRoute>
            }
          >
            <Route index element={<QCRedirect />} />
            <Route
              path="files"
              element={(
                <RoleRoute allowedRoles={['admin', 'engineer']}>
                  <FilesPage />
                </RoleRoute>
              )}
            />
            <Route
              path="files/folders"
              element={(
                <RoleRoute allowedRoles={['admin', 'engineer']}>
                  <FilesPage />
                </RoleRoute>
              )}
            />
            <Route
              path="files/:id"
              element={(
                <RoleRoute allowedRoles={['admin', 'engineer']}>
                  <FileDetailPage />
                </RoleRoute>
              )}
            />
            <Route
              path="users"
              element={
                <AdminRoute>
                  <UsersPage />
                </AdminRoute>
              }
            />
            <Route path="profile" element={<ProfilePage />} />
            <Route
              path="barcode"
              element={(
                <RoleRoute allowedRoles={['admin', 'engineer']}>
                  <BarcodePage />
                </RoleRoute>
              )}
            />
            <Route
              path="battery"
              element={(
                <RoleRoute allowedRoles={['admin', 'qc', 'qc_manager']}>
                  <BatteryPage />
                </RoleRoute>
              )}
            />
            <Route path="battery-dmp" element={<Navigate to="/battery-dmp/dmp" replace />} />
            <Route
              path="battery-dmp/:section"
              element={(
                <RoleRoute allowedRoles={['admin', 'lab', 'lab_manager']}>
                  <BatteryDMPPage />
                </RoleRoute>
              )}
            />
            <Route
              path="count-batteries"
              element={(
                <RoleRoute allowedRoles={['admin', 'engineer']}>
                  <CountBatteriesPage />
                </RoleRoute>
              )}
            />
            <Route
              path="qc/dashboard"
              element={(
                <RoleRoute allowedRoles={['admin', 'qc', 'qc_manager']}>
                  <QCDashboard />
                </RoleRoute>
              )}
            />
            <Route
              path="qc/entry"
              element={(
                <RoleRoute allowedRoles={['admin', 'qc', 'qc_manager']}>
                  <QCEntry />
                </RoleRoute>
              )}
            />
            <Route
              path="qc/dictionaries"
              element={
                <RoleRoute allowedRoles={['admin', 'qc', 'qc_manager']}>
                  <QCDictionaries />
                </RoleRoute>
              }
            />
            <Route path="dm2000" element={<Navigate to="/battery-dmp/dm2000" replace />} />
            <Route path="dm3000" element={<Navigate to="/battery-dmp/dm3000" replace />} />
          </Route>
          <Route
            path="backups/:name"
            element={
              <ProtectedRoute>
                <AdminRoute>
                  <BackupViewerPage />
                </AdminRoute>
              </ProtectedRoute>
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </ConfigProvider>
  );
}

export default function App() {
  return (
    <LangProvider>
      <AuthProvider>
        <NotificationProvider>
          <AppRoutes />
        </NotificationProvider>
      </AuthProvider>
    </LangProvider>
  );
}
