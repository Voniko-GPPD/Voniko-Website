import React, { useState, useEffect, useMemo } from 'react';
import { Layout, Menu, Avatar, Dropdown, Button, Space, Badge, Popover, List, Empty, Tag, Drawer, Grid } from 'antd';
import {
  DashboardOutlined,
  FileOutlined,
  TeamOutlined,
  UserOutlined,
  LogoutOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  SettingOutlined,
  GlobalOutlined,
  FolderOutlined,
  BellOutlined,
  ClearOutlined,
  QrcodeOutlined,
  ThunderboltOutlined,
  ExperimentOutlined,
  DatabaseOutlined,
  FileExcelOutlined,
  CameraOutlined,
  CheckCircleOutlined,
  FormOutlined,
  BookOutlined,
  MenuOutlined,
} from '@ant-design/icons';
import { useNavigate, useLocation, Outlet } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useLang } from '../../contexts/LangContext';
import { useNotifications } from '../../contexts/NotificationContext';
import dayjs from 'dayjs';
import PendingUploadsModal from '../PendingUploadsModal/PendingUploadsModal';
import { getPendingUploads, removePendingUpload } from '../../utils/pendingUploads';

const { Header, Sider, Content } = Layout;
const SIDEBAR_WIDTH = 256;
const COLLAPSED_SIDEBAR_WIDTH = 80;

function matchesPath(itemKey, pathname) {
  if (!itemKey || typeof itemKey !== 'string' || !itemKey.startsWith('/')) return false;
  if (itemKey === '/') return pathname === '/';
  return pathname === itemKey || pathname.startsWith(`${itemKey}/`);
}

function findSelectedKey(items, pathname) {
  for (const item of items) {
    if (item.children) {
      const childMatch = findSelectedKey(item.children, pathname);
      if (childMatch) return childMatch;
    }
    if (matchesPath(item.key, pathname)) return item.key;
  }
  return null;
}

function findParentKeys(items, pathname, parents = []) {
  for (const item of items) {
    if (item.children) {
      const match = findParentKeys(item.children, pathname, [...parents, item.key]);
      if (match) return match;
    }
    if (matchesPath(item.key, pathname)) return parents;
  }
  return [];
}

export default function AppLayout() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const [collapsed, setCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [menuOpenKeys, setMenuOpenKeys] = useState([]);
  const {
    user,
    logout,
    isAdmin,
    isQC,
    isQCManager,
    isEngineer,
    isLab,
    isLabManager,
    canAccessFiles,
    canAccessBarcode,
    canAccessBattery,
    canAccessDM,
    canAccessCounter,
    canAccessQC,
  } = useAuth();
  const { t, lang, switchLang } = useLang();
  const { notifications, unreadCount, markAllRead, clearNotifications, dbNotifications, dbUnreadCount } = useNotifications();
  const totalUnread = unreadCount + dbUnreadCount;
  const navigate = useNavigate();
  const location = useLocation();

  const [pendingUploads, setPendingUploads] = useState([]);
  const [pendingModalOpen, setPendingModalOpen] = useState(false);

  useEffect(() => {
    const pending = getPendingUploads();
    if (pending.length > 0) {
      setPendingUploads(pending);
      setPendingModalOpen(true);
    }
  }, []);

  const menuLabel = (text) => (
    <span style={{ whiteSpace: 'normal', lineHeight: '20px', display: 'inline-block' }}>
      {text}
    </span>
  );

  const canSeeDashboard = isAdmin || isEngineer;

  const menuItems = useMemo(() => [
    ...(canSeeDashboard ? [
      { key: '/', icon: <DashboardOutlined />, label: menuLabel(t('dashboard')) },
    ] : []),
    ...(canAccessFiles ? [
      {
        key: 'files',
        icon: <FileOutlined />,
        label: menuLabel(t('files')),
        children: [
          {
            key: '/files/folders',
            icon: <FolderOutlined />,
            label: menuLabel(t('folderManagement')),
          },
          {
            key: '/files',
            icon: <FileOutlined />,
            label: menuLabel(t('fileList')),
          },
        ],
      },
    ] : []),
    ...(canAccessBarcode ? [
      { key: '/barcode', icon: <QrcodeOutlined />, label: menuLabel(t('barcode')) },
    ] : []),
    ...(canAccessBattery ? [
      { key: '/battery', icon: <ThunderboltOutlined />, label: menuLabel(t('batteryTest')) },
    ] : []),
    ...(canAccessDM ? [
      {
        key: 'dm',
        icon: <ExperimentOutlined />,
        label: menuLabel(t('dmManagement')),
        children: [
          {
            key: '/battery-dmp/dmp',
            icon: <ExperimentOutlined />,
            label: menuLabel(t('dmpBridgeTitle')),
          },
          {
            key: '/battery-dmp/dm2000',
            icon: <DatabaseOutlined />,
            label: menuLabel(t('dm2000Title')),
          },
          {
            key: '/battery-dmp/dm3000',
            icon: <DatabaseOutlined />,
            label: menuLabel(t('dm3000Title')),
          },
          {
            key: '/battery-dmp/perf-report',
            icon: <FileExcelOutlined />,
            label: menuLabel(t('dmpPerfReportTab')),
          },
        ],
      },
    ] : []),
    ...(canAccessCounter ? [
      { key: '/count-batteries', icon: <CameraOutlined />, label: menuLabel(t('countBatteries')) },
    ] : []),
    ...(canAccessQC ? [{
      key: 'qc',
      icon: <CheckCircleOutlined />,
      label: menuLabel(t('qc.module_name')),
      children: [
        {
          key: '/qc/dashboard',
          icon: <DashboardOutlined />,
          label: menuLabel(t('qc.nav_dashboard')),
        },
        {
          key: '/qc/entry',
          icon: <FormOutlined />,
          label: menuLabel(t('qc.nav_entry')),
        },
        ...((isAdmin || isQC || isQCManager) ? [{
          key: '/qc/dictionaries',
          icon: <BookOutlined />,
          label: menuLabel(t('qc.nav_dictionaries')),
        }] : []),
      ],
    }] : []),
    ...(isAdmin ? [
      { key: '/users', icon: <TeamOutlined />, label: menuLabel(t('users')) },
    ] : []),
  ], [
    canAccessBarcode,
    canAccessBattery,
    canAccessCounter,
    canAccessDM,
    canAccessFiles,
    canAccessQC,
    canSeeDashboard,
    isAdmin,
    isQC,
    isQCManager,
    t,
  ]);

  const langMenuItems = [
    {
      key: 'lang-vi',
      label: (
        <Space size={6}>
          <img src="https://flagcdn.com/w20/vn.png" width="18" height="12" alt="VN" style={{ borderRadius: 2 }} />
          <span>Tiếng Việt</span>
          {lang === 'vi' && <Tag color="blue" style={{ fontSize: 10, padding: '0 4px', lineHeight: '16px' }}>✓</Tag>}
        </Space>
      ),
      onClick: () => switchLang('vi'),
    },
    {
      key: 'lang-en',
      label: (
        <Space size={6}>
          <img src="https://flagcdn.com/w20/gb.png" width="18" height="12" alt="GB" style={{ borderRadius: 2 }} />
          <span>English</span>
          {lang === 'en' && <Tag color="blue" style={{ fontSize: 10, padding: '0 4px', lineHeight: '16px' }}>✓</Tag>}
        </Space>
      ),
      onClick: () => switchLang('en'),
    },
    {
      key: 'lang-zh',
      label: (
        <Space size={6}>
          <img src="https://flagcdn.com/w20/cn.png" width="18" height="12" alt="CN" style={{ borderRadius: 2 }} />
          <span>中文</span>
          {lang === 'zh' && <Tag color="blue" style={{ fontSize: 10, padding: '0 4px', lineHeight: '16px' }}>✓</Tag>}
        </Space>
      ),
      onClick: () => switchLang('zh'),
    },
  ];

  const userMenuItems = [
    {
      key: 'profile',
      icon: <UserOutlined />,
      label: t('profile'),
      onClick: () => navigate('/profile'),
    },
    { type: 'divider' },
    {
      key: 'language',
      icon: <GlobalOutlined />,
      label: t('language'),
      children: langMenuItems,
    },
    { type: 'divider' },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: t('logout'),
      danger: true,
      onClick: async () => {
        await logout();
        navigate('/login');
      },
    },
  ];

  const selectedKey = findSelectedKey(menuItems, location.pathname) || '/';
  const derivedOpenKeys = useMemo(
    () => findParentKeys(menuItems, location.pathname),
    [location.pathname, menuItems],
  );

  useEffect(() => {
    if (isMobile) {
      setCollapsed(false);
    }
  }, [isMobile]);

  useEffect(() => {
    if (isMobile) {
      setMobileMenuOpen(false);
    }
  }, [isMobile, location.pathname]);

  useEffect(() => {
    if (collapsed) {
      setMenuOpenKeys((prev) => (prev.length ? [] : prev));
      return;
    }
    setMenuOpenKeys((prev) => {
      const next = Array.from(new Set([...(prev || []), ...derivedOpenKeys]));
      if (prev.length === next.length && prev.every((value, index) => value === next[index])) {
        return prev;
      }
      return next;
    });
  }, [collapsed, derivedOpenKeys]);

  const handleMenuClick = ({ key }) => {
    if (typeof key === 'string' && key.startsWith('/')) {
      navigate(key);
      if (isMobile) {
        setMobileMenuOpen(false);
      }
    }
  };

  const sideMenu = (
    <>
      <div style={{
        height: 64,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '0 16px',
        borderBottom: '1px solid rgba(255,255,255,0.1)',
      }}>
        {(collapsed && !isMobile) ? (
          <img src="/logo.avif" alt="Voniko" style={{ height: 32, objectFit: 'contain' }} />
        ) : (
          <img src="/logo.avif" alt="Voniko" style={{ height: 44, maxWidth: '100%', objectFit: 'contain' }} />
        )}
      </div>
      <Menu
        className="app-side-menu"
        theme="dark"
        mode="inline"
        selectedKeys={[selectedKey]}
        openKeys={menuOpenKeys}
        onOpenChange={setMenuOpenKeys}
        items={menuItems}
        onClick={handleMenuClick}
        style={{ marginTop: 8, borderInlineEnd: 'none' }}
      />
    </>
  );

  return (
    <>
    <style>
      {`
        .app-side-menu.ant-menu-inline,
        .app-side-menu .ant-menu-sub.ant-menu-inline {
          border-inline-end: none !important;
        }
        .app-side-menu .ant-menu-item,
        .app-side-menu .ant-menu-submenu-title {
          height: auto !important;
          min-height: 40px;
          line-height: 20px !important;
          display: flex !important;
          align-items: center !important;
          padding-top: 10px;
          padding-bottom: 10px;
        }
        .app-side-menu .ant-menu-submenu-title {
          padding-inline-end: 40px !important;
        }
        .app-side-menu .ant-menu-title-content {
          white-space: normal !important;
          overflow: visible !important;
          text-overflow: unset !important;
          line-height: 20px !important;
        }
        .app-side-menu .ant-menu-submenu-title .ant-menu-title-content {
          padding-inline-end: 18px !important;
        }
        .app-side-menu .ant-menu-submenu-arrow {
          inset-inline-end: 16px !important;
        }
        .app-mobile-drawer .ant-drawer-body {
          padding: 0 !important;
          background: #001529;
        }
        @media (max-width: 767px) {
          .app-page-content {
            margin: 12px !important;
            min-height: calc(100vh - 88px) !important;
          }
        }
      `}
    </style>
    <Layout style={{ minHeight: '100vh' }}>
      {!isMobile ? (
        <Sider
          collapsible
          collapsed={collapsed}
          trigger={null}
          width={SIDEBAR_WIDTH}
          collapsedWidth={COLLAPSED_SIDEBAR_WIDTH}
          style={{
            background: '#001529',
            overflow: 'auto',
            height: '100vh',
            position: 'fixed',
            left: 0,
            top: 0,
            bottom: 0,
            zIndex: 100,
          }}
        >
          {sideMenu}
        </Sider>
      ) : null}

      <Layout style={{ marginLeft: isMobile ? 0 : (collapsed ? COLLAPSED_SIDEBAR_WIDTH : SIDEBAR_WIDTH), transition: 'margin 0.2s' }}>
        <Header style={{
          padding: isMobile ? '0 12px' : '0 24px',
          background: '#fff',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
          position: 'sticky',
          top: 0,
          zIndex: 99,
        }}>
          <Button
            type="text"
            icon={isMobile ? <MenuOutlined /> : (collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />)}
            onClick={() => (isMobile ? setMobileMenuOpen(true) : setCollapsed(!collapsed))}
            style={{ fontSize: 16 }}
          />

          <Space size={isMobile ? 8 : 16}>
            <Dropdown menu={{ items: langMenuItems }} trigger={['click']}>
              <Button type="text" icon={<GlobalOutlined />} style={{ padding: '0 8px' }}>
                <img
                  src={lang === 'vi' ? 'https://flagcdn.com/w20/vn.png' : lang === 'en' ? 'https://flagcdn.com/w20/gb.png' : 'https://flagcdn.com/w20/cn.png'}
                  width="18" height="12"
                  alt={lang === 'vi' ? 'VN' : lang === 'en' ? 'EN' : 'CN'}
                  style={{ borderRadius: 2, marginRight: 4 }}
                />
                {lang === 'vi' ? 'VI' : lang === 'en' ? 'EN' : '中文'}
              </Button>
            </Dropdown>

            <Popover
              trigger="click"
              placement="bottomRight"
              onOpenChange={(open) => { if (open) markAllRead(); }}
              content={
                <div style={{ width: 320 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                    <span style={{ fontWeight: 600 }}>{t('notifications')}</span>
                    {notifications.length > 0 && (
                      <Button
                        size="small"
                        type="text"
                        icon={<ClearOutlined />}
                        onClick={clearNotifications}
                      >
                        {t('clearNotifications')}
                      </Button>
                    )}
                  </div>
                  {dbNotifications.length === 0 && notifications.length === 0 ? (
                    <Empty description={t('noNotifications')} image={Empty.PRESENTED_IMAGE_SIMPLE} />
                  ) : (
                    <List
                      size="small"
                      dataSource={[
                        ...dbNotifications.map(n => ({ id: n.id, message: n.message, timestamp: n.createdAt, isRead: n.isRead, source: 'db' })),
                        ...notifications.filter(n => !n.read).map(n => ({ id: n.id, message: n.message, timestamp: n.timestamp, isRead: false, source: 'sse' })),
                      ].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)).slice(0, 20)}
                      style={{ maxHeight: 360, overflow: 'auto' }}
                      renderItem={(item) => (
                        <List.Item style={{ padding: '6px 0' }}>
                          <div style={{ width: '100%' }}>
                            <div style={{ fontSize: 13, fontWeight: item.isRead ? 400 : 600 }}>{item.message}</div>
                            <div style={{ fontSize: 11, color: '#8c8c8c' }}>
                              {dayjs(item.timestamp).format('HH:mm DD/MM')}
                            </div>
                          </div>
                        </List.Item>
                      )}
                    />
                  )}
                </div>
              }
            >
              <Badge count={totalUnread} size="small" offset={[-2, 2]}>
                <Button
                  type="text"
                  icon={<BellOutlined style={{ fontSize: 18 }} />}
                  style={{ padding: '0 8px' }}
                />
              </Badge>
            </Popover>

            <Dropdown menu={{ items: userMenuItems }} trigger={['click']}>
              <Space style={{ cursor: 'pointer', padding: '0 8px' }}>
                <Avatar
                  src={user?.avatarUrl}
                  icon={!user?.avatarUrl && <UserOutlined />}
                  style={{ background: '#1677ff' }}
                />
                {!isMobile ? <span style={{ fontWeight: 500 }}>{user?.displayName}</span> : null}
              </Space>
            </Dropdown>
          </Space>
        </Header>

        <Content style={{
          margin: isMobile ? '12px' : '24px',
          minHeight: 'calc(100vh - 112px)',
        }} className="app-page-content">
          <Outlet />
        </Content>
      </Layout>
    </Layout>

    <Drawer
      className="app-mobile-drawer"
      title={null}
      placement="left"
      open={mobileMenuOpen}
      onClose={() => setMobileMenuOpen(false)}
      closable={false}
      width={288}
      styles={{ header: { display: 'none' }, body: { padding: 0, background: '#001529' } }}
    >
      {sideMenu}
    </Drawer>

    <PendingUploadsModal
      open={pendingModalOpen}
      pendingUploads={pendingUploads}
      onDismissOne={(fileId) => {
        const updated = pendingUploads.filter(p => p.fileId !== fileId);
        setPendingUploads(updated);
        if (updated.length === 0) setPendingModalOpen(false);
      }}
      onUpload={(item) => {
        setPendingModalOpen(false);
        navigate(`/files/${item.fileId}`);
      }}
      onClose={() => setPendingModalOpen(false)}
    />
    </>
  );
}
