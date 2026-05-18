import React, { useEffect, useState, useCallback } from 'react';
import { Row, Col, Card, Statistic, Table, Tag, Typography, Spin, Space, Avatar, Button, Popconfirm, message, DatePicker, Select } from 'antd';
import {
  FileOutlined, HistoryOutlined, TeamOutlined,
  DatabaseOutlined, UserOutlined, CloudUploadOutlined, DeleteOutlined, EyeOutlined, DownloadOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import dayjs from 'dayjs';
import api from '../../api';
import { triggerBackup, listBackups, deleteBackup } from '../../api';
import { useLang } from '../../contexts/LangContext';
import { useAuth } from '../../contexts/AuthContext';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;

const ACTION_COLORS = {
  add_file: 'success',
  update_file: 'processing',
  delete_file: 'error',
  restore_version: 'warning',
  create_user: 'success',
  update_user: 'processing',
  delete_user: 'error',
  create_folder: 'success',
  update_folder: 'processing',
  delete_folder: 'error',
  lock_file: 'orange',
  unlock_file: 'cyan',
  login: 'default',
  logout: 'default',
  subscribe_file: 'geekblue',
  unsubscribe_file: 'geekblue',
  add_file_tags: 'blue',
  create_tag: 'success',
  delete_tag: 'error',
  add_comment: 'purple',
};

function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

export default function DashboardPage() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [backups, setBackups] = useState([]);
  const [backupLoading, setBackupLoading] = useState(false);
  const [backupsLoading, setBackupsLoading] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [exportDateRange, setExportDateRange] = useState(null);
  const { t } = useLang();
  const { isAdmin } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    api.get('/files/stats')
      .then(res => setData(res.data))
      .finally(() => setLoading(false));
  }, []);

  const fetchBackups = useCallback(async () => {
    if (!isAdmin) return;
    setBackupsLoading(true);
    try {
      const res = await listBackups();
      setBackups(res.data.data || []);
    } catch {
      // silently ignore
    } finally {
      setBackupsLoading(false);
    }
  }, [isAdmin]);

  useEffect(() => { fetchBackups(); }, [fetchBackups]);

  const handleBackupNow = async () => {
    setBackupLoading(true);
    try {
      await triggerBackup();
      message.success(t('backupSuccess'));
      fetchBackups();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setBackupLoading(false);
    }
  };

  const handleDeleteBackup = async (name) => {
    try {
      await deleteBackup(name);
      message.success(t('backupDeleted'));
      fetchBackups();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const handleExportCSV = async () => {
    setExportLoading(true);
    try {
      const params = {};
      if (exportDateRange && exportDateRange[0]) params.from = exportDateRange[0].toISOString();
      if (exportDateRange && exportDateRange[1]) params.to = exportDateRange[1].toISOString();
      const res = await api.get('/files/activity/export', { params, responseType: 'blob' });
      const dateStr = dayjs().format('YYYY-MM-DD');
      const url = window.URL.createObjectURL(new Blob([res.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `activity_log_${dateStr}.csv`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setExportLoading(false);
    }
  };

  const actionLabel = (action) => {
    const map = {
      add_file: t('actionAddFile'),
      update_file: t('actionUpdateFile'),
      delete_file: t('actionDeleteFile'),
      restore_version: t('actionRestoreVersion'),
      create_user: t('actionCreateUser'),
      update_user: t('actionUpdateUser'),
      delete_user: t('actionDeleteUser'),
      create_folder: t('actionCreateFolder'),
      update_folder: t('actionUpdateFolder'),
      delete_folder: t('actionDeleteFolder'),
      lock_file: t('actionLockFile'),
      unlock_file: t('actionUnlockFile'),
      login: t('actionLogin'),
      logout: t('actionLogout'),
      subscribe_file: t('actionSubscribeFile'),
      unsubscribe_file: t('actionUnsubscribeFile'),
      add_file_tags: t('actionAddTags'),
      create_tag: t('actionCreateTag'),
      delete_tag: t('actionDeleteTag'),
      add_comment: t('actionAddComment'),
    };
    return map[action] || action;
  };
    if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', paddingTop: 100 }}>
        <Spin size="large" />
      </div>
    );
  }

  const statCards = [
    {
      title: t('totalFiles'),
      value: data?.stats?.totalFiles || 0,
      icon: <FileOutlined />,
      color: '#1677ff',
      onClick: () => navigate('/files'),
    },
    {
      title: t('totalVersions'),
      value: data?.stats?.totalVersions || 0,
      icon: <HistoryOutlined />,
      color: '#52c41a',
    },
    {
      title: t('totalUsers'),
      value: data?.stats?.totalUsers || 0,
      icon: <TeamOutlined />,
      color: '#fa8c16',
      onClick: () => navigate('/users'),
    },
    {
      title: t('totalStorage'),
      value: formatBytes(data?.stats?.totalSize),
      icon: <DatabaseOutlined />,
      color: '#722ed1',
      isString: true,
    },
  ];

  return (
    <div>
      <Title level={3} style={{ marginBottom: 24 }}>{t('dashboard')}</Title>

      {/* Stat cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        {statCards.map((card) => (
          <Col key={card.title} xs={24} sm={12} xl={6}>
            <Card
              hoverable={!!card.onClick}
              onClick={card.onClick}
              style={{ cursor: card.onClick ? 'pointer' : 'default' }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                <div style={{
                  width: 48,
                  height: 48,
                  borderRadius: 12,
                  background: `${card.color}1a`,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: 22,
                  color: card.color,
                }}>
                  {card.icon}
                </div>
                <div>
                  <Text type="secondary" style={{ fontSize: 13 }}>{card.title}</Text>
                  <div style={{ fontSize: 24, fontWeight: 700, color: card.color, lineHeight: 1.2 }}>
                    {card.isString ? card.value : card.value.toLocaleString()}
                  </div>
                </div>
              </div>
            </Card>
          </Col>
        ))}
      </Row>

      {/* Recent Activity */}
      <Card
        title={t('recentActivity')}
        style={{ marginBottom: isAdmin ? 24 : 0 }}
        extra={isAdmin && (
          <Space>
            <RangePicker
              size="small"
              onChange={setExportDateRange}
              allowEmpty={[true, true]}
            />
            <Button
              size="small"
              icon={<DownloadOutlined />}
              loading={exportLoading}
              onClick={handleExportCSV}
            >
              {t('exportCSV')}
            </Button>
          </Space>
        )}
      >
        <Table
          dataSource={data?.recentActivity || []}
          rowKey="id"
          pagination={false}
          size="small"
          columns={[
            {
              title: t('user'),
              dataIndex: 'userName',
              render: (name, record) => (
                <Space>
                  <Avatar size={24} src={record.avatarUrl} icon={<UserOutlined />} style={{ background: '#1677ff' }} />
                  <Text>{name}</Text>
                </Space>
              ),
            },
            {
              title: t('action'),
              dataIndex: 'action',
              render: (action) => (
                <Tag color={ACTION_COLORS[action] || 'default'}>
                  {actionLabel(action)}
                </Tag>
              ),
            },
            {
              title: t('entity'),
              dataIndex: 'entityName',
            },
            {
              title: t('timestamp'),
              dataIndex: 'createdAt',
              render: (v) => dayjs(v).format('YYYY-MM-DD HH:mm'),
            },
          ]}
        />
      </Card>

      {/* Backup section - admin only */}
      {isAdmin && (
        <Card
          title={t('backup')}
          extra={
            <Button
              type="primary"
              icon={<CloudUploadOutlined />}
              loading={backupLoading}
              onClick={handleBackupNow}
            >
              {t('backupNow')}
            </Button>
          }
        >
          <Table
            dataSource={backups}
            rowKey="name"
            loading={backupsLoading}
            pagination={false}
            size="small"
            locale={{ emptyText: t('noBackups') }}
            columns={[
              {
                title: t('backupName'),
                dataIndex: 'name',
              },
              {
                title: t('backupDate'),
                dataIndex: 'createdAt',
                render: (v) => dayjs(v).format('YYYY-MM-DD HH:mm:ss'),
              },
              {
                title: t('backupSize'),
                dataIndex: 'size',
                render: (v) => formatBytes(v),
              },
              {
                title: t('actions'),
                key: 'actions',
                render: (_, record) => (
                  <Space>
                    <Button
                      size="small"
                      icon={<EyeOutlined />}
                      onClick={() => navigate(`/backups/${record.name}`)}
                    >
                      {t('backupViewer')}
                    </Button>
                    <Popconfirm
                      title={t('backupDeleteConfirm')}
                      onConfirm={() => handleDeleteBackup(record.name)}
                      okText={t('yes')}
                      cancelText={t('no')}
                    >
                      <Button size="small" danger icon={<DeleteOutlined />}>
                        {t('delete')}
                      </Button>
                    </Popconfirm>
                  </Space>
                ),
              },
            ]}
          />
        </Card>
      )}
    </div>
  );
}
