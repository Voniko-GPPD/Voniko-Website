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

function QCRedirect() {
  const { isQC } = useAuth();
  if (isQC) return <Navigate to="/barcode" replace />;
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
            <Route path="files" element={<FilesPage />} />
            <Route path="files/:id" element={<FileDetailPage />} />
            <Route
              path="users"
              element={
                <AdminRoute>
                  <UsersPage />
                </AdminRoute>
              }
            />
            <Route path="profile" element={<ProfilePage />} />
            <Route path="barcode" element={<BarcodePage />} />
            <Route path="battery" element={<BatteryPage />} />
            <Route path="battery-dmp" element={<BatteryDMPPage />} />
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
