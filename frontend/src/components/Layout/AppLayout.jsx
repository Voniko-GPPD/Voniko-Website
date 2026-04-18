import React, { useState, useEffect } from 'react';
import { Layout, Menu, Avatar, Dropdown, Button, Space, Badge, Popover, List, Empty, Tag } from 'antd';
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
} from '@ant-design/icons';
import { useNavigate, useLocation, Outlet } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useLang } from '../../contexts/LangContext';
import { useNotifications } from '../../contexts/NotificationContext';
import dayjs from 'dayjs';
import PendingUploadsModal from '../PendingUploadsModal/PendingUploadsModal';
import { getPendingUploads, removePendingUpload } from '../../utils/pendingUploads';

const { Header, Sider, Content } = Layout;

export default function AppLayout() {
  const [collapsed, setCollapsed] = useState(false);
  const { user, logout, isAdmin, isQC } = useAuth();
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

  const menuItems = [
    ...(!isQC ? [
      { key: '/', icon: <DashboardOutlined />, label: t('dashboard') },
      { key: '/files', icon: <FileOutlined />, label: t('files') },
    ] : []),
    { key: '/barcode', icon: <QrcodeOutlined />, label: t('barcode') },
    { key: '/battery', icon: <ThunderboltOutlined />, label: t('batteryTest') },
    { key: '/battery-dmp', icon: <ExperimentOutlined />, label: 'DMP Bridge' },
    ...(isAdmin ? [
      { key: '/users', icon: <TeamOutlined />, label: t('users') },
    ] : []),
  ];

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

  const selectedKey = menuItems.find(item => {
    if (item.key === '/') return location.pathname === '/';
    return location.pathname === item.key || location.pathname.startsWith(item.key + '/');
  })?.key || '/';

  return (
    <>
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        trigger={null}
        width={220}
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
        <div style={{
          height: 64,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '0 16px',
          borderBottom: '1px solid rgba(255,255,255,0.1)',
        }}>
          {collapsed ? (
            <img src="/logo.avif" alt="Voniko" style={{ height: 32, objectFit: 'contain' }} />
          ) : (
            <img src="/logo.avif" alt="Voniko" style={{ height: 44, maxWidth: '100%', objectFit: 'contain' }} />
          )}
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{ marginTop: 8 }}
        />
      </Sider>

      <Layout style={{ marginLeft: collapsed ? 80 : 220, transition: 'margin 0.2s' }}>
        <Header style={{
          padding: '0 24px',
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
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed(!collapsed)}
            style={{ fontSize: 16 }}
          />

          <Space>
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
                <span style={{ fontWeight: 500 }}>{user?.displayName}</span>
              </Space>
            </Dropdown>
          </Space>
        </Header>

        <Content style={{
          margin: '24px',
          minHeight: 'calc(100vh - 112px)',
        }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>

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
