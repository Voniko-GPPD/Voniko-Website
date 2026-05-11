import React, { useEffect, useState, useCallback } from 'react';
import { Card, Table, Tag, Typography, Input, Space, Select, Button, Avatar } from 'antd';
import { SearchOutlined, ReloadOutlined, UserOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import api from '../../api';
import { useLang } from '../../contexts/LangContext';

const { Title, Text } = Typography;

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

export default function HistoryPage() {
  const [logs, setLogs] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);

  const { t } = useLang();

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.get('/files/activity', { params: { page, limit: 50 } });
      setLogs(res.data.data);
      setTotal(res.data.total);
    } finally {
      setLoading(false);
    }
  }, [page]);

  useEffect(() => { fetchLogs(); }, [fetchLogs]);

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

  const columns = [
    {
      title: '#',
      width: 60,
      render: (_, __, i) => (page - 1) * 50 + i + 1,
    },
    {
      title: t('user'),
      dataIndex: 'userName',
      width: 150,
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
      width: 160,
      render: (action) => (
        <Tag color={ACTION_COLORS[action] || 'default'}>{actionLabel(action)}</Tag>
      ),
    },
    {
      title: t('entity'),
      dataIndex: 'entityName',
      render: (name, record) => (
        <Space direction="vertical" size={0}>
          <Text>{name}</Text>
          {record.details?.commitMessage && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              "{record.details.commitMessage}"
            </Text>
          )}
          {record.details?.versionNumber && (
            <Tag color="blue" style={{ fontSize: 11 }}>v{record.details.versionNumber}</Tag>
          )}
        </Space>
      ),
    },
    {
      title: t('timestamp'),
      dataIndex: 'createdAt',
      width: 170,
      render: (v) => dayjs(v).format('YYYY-MM-DD HH:mm:ss'),
    },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>{t('activityLog')}</Title>
        <Button icon={<ReloadOutlined />} onClick={fetchLogs}>{t('refresh')}</Button>
      </div>

      <Card>
        <Table
          columns={columns}
          dataSource={logs}
          rowKey="id"
          loading={loading}
          pagination={{
            current: page,
            total,
            pageSize: 50,
            onChange: (p) => setPage(p),
            showTotal: (t) => `${t} entries`,
          }}
          size="middle"
        />
      </Card>
    </div>
  );
}
