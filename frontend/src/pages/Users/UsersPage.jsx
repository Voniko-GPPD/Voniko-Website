import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Button, Space, Typography, Tag, Modal, Form,
  Input, Select, message, Popconfirm, Avatar, Badge, Tooltip,
} from 'antd';
import { PlusOutlined, EditOutlined, StopOutlined, DeleteOutlined, UserOutlined, CheckOutlined, KeyOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import api from '../../api';
import { useLang } from '../../contexts/LangContext';
import { useAuth } from '../../contexts/AuthContext';

const { Title, Text } = Typography;

export default function UsersPage() {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editUser, setEditUser] = useState(null);
  const [submitLoading, setSubmitLoading] = useState(false);
  const [form] = Form.useForm();
  const [resetPwdModal, setResetPwdModal] = useState(false);
  const [resetPwdUser, setResetPwdUser] = useState(null);
  const [resetPwdForm] = Form.useForm();
  const [resetPwdLoading, setResetPwdLoading] = useState(false);

  const { t, lang } = useLang();
  const { user: currentUser } = useAuth();

  const fetchUsers = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.get('/users');
      setUsers(res.data);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchUsers();
    const interval = setInterval(fetchUsers, 30000);
    return () => clearInterval(interval);
  }, [fetchUsers]);

  const openCreate = () => {
    setEditUser(null);
    form.resetFields();
    setModalOpen(true);
  };

  const openEdit = (user) => {
    setEditUser(user);
    form.setFieldsValue({
      displayName: user.displayName,
      role: user.role,
    });
    setModalOpen(true);
  };

  const handleSubmit = async (values) => {
    setSubmitLoading(true);
    try {
      if (editUser) {
        await api.put(`/users/${editUser.id}`, {
          displayName: values.displayName,
          role: values.role,
        });
        message.success(t('userUpdated'));
      } else {
        await api.post('/users', {
          username: values.username,
          password: values.password,
          displayName: values.displayName,
          role: values.role,
        });
        message.success(t('userCreated'));
      }
      setModalOpen(false);
      fetchUsers();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setSubmitLoading(false);
    }
  };

  const handleDeactivate = async (id) => {
    try {
      await api.delete(`/users/${id}`);
      message.success(t('userDeleted'));
      fetchUsers();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const handlePermanentDelete = async (id) => {
    try {
      await api.delete(`/users/${id}/permanent`);
      message.success(t('userPermanentlyDeleted'));
      fetchUsers();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const handleReactivate = async (id) => {
    try {
      await api.put(`/users/${id}`, { isActive: true });
      message.success(t('userReactivated'));
      fetchUsers();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const openResetPwd = (user) => {
    setResetPwdUser(user);
    resetPwdForm.resetFields();
    setResetPwdModal(true);
  };

  const handleResetPassword = async (values) => {
    setResetPwdLoading(true);
    try {
      await api.put(`/users/${resetPwdUser.id}/reset-password`, { password: values.password });
      message.success(t('resetPasswordSuccess'));
      setResetPwdModal(false);
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setResetPwdLoading(false);
    }
  };

  const roleLabel = (role) => {
    const map = { admin: t('admin'), engineer: t('engineerRole'), qc: t('qcRole') };
    return map[role] || role;
  };

  const roleColor = (role) => {
    const map = { admin: 'red', engineer: 'green', qc: 'orange' };
    return map[role] || 'default';
  };

  const columns = [
    {
      title: t('displayName'),
      render: (_, record) => (
        <Space>
          <Avatar
            src={record.avatarUrl}
            icon={<UserOutlined />}
            style={{ background: record.role === 'admin' ? '#ff4d4f' : '#1677ff' }}
          />
          <div>
            <div style={{ fontWeight: 500 }}>{record.displayName}</div>
            <Text type="secondary" style={{ fontSize: 12 }}>@{record.username}</Text>
          </div>
        </Space>
      ),
    },
    {
      title: t('role'),
      dataIndex: 'role',
      render: (role) => <Tag color={roleColor(role)}>{roleLabel(role)}</Tag>,
    },
    {
      title: t('status'),
      dataIndex: 'isActive',
      render: (_, record) => {
        if (!record.isActive) {
          return <Badge status="error" text={t('lockedAccount')} />;
        }
        if (record.isOnline) {
          return <Badge status="success" text={t('onlineNow')} />;
        }
        const timeRef = record.lastSeen || record.lastLogin;
        if (timeRef) {
          return (
            <Tooltip title={dayjs(timeRef).format('YYYY-MM-DD HH:mm')}>  
              <Badge status="default" text={`${t('active')} ${dayjs(timeRef).locale(lang).fromNow()}`} />
            </Tooltip>
          );
        }
        return <Badge status="success" text={t('active')} />;
      },
    },
    {
      title: t('createdAt'),
      dataIndex: 'createdAt',
      render: (v) => dayjs(v).format('YYYY-MM-DD'),
    },
    {
      title: t('actions'),
      render: (_, record) => (
        <Space>
          <Button
            size="small"
            icon={<EditOutlined />}
            onClick={() => openEdit(record)}
          >
            {t('edit')}
          </Button>
          {record.id !== currentUser?.id && (
            <Button
              size="small"
              icon={<KeyOutlined />}
              onClick={() => openResetPwd(record)}
            >
              {t('resetPassword')}
            </Button>
          )}
          {record.id !== currentUser?.id && (
            record.isActive ? (
              <Popconfirm
                title={t('confirmDeleteUser')}
                onConfirm={() => handleDeactivate(record.id)}
                okText={t('yes')}
                cancelText={t('no')}
              >
                <Button size="small" danger icon={<StopOutlined />}>  
                  {t('inactive')}
                </Button>
              </Popconfirm>
            ) : (
              <>
                <Popconfirm
                  title={t('confirmReactivateUser')}
                  onConfirm={() => handleReactivate(record.id)}
                  okText={t('yes')}
                  cancelText={t('no')}
                >
                  <Button size="small" type="primary" icon={<CheckOutlined />}>  
                    {t('reactivateUser')}
                  </Button>
                </Popconfirm>
                <Popconfirm
                  title={t('confirmPermanentDeleteUser')}
                  onConfirm={() => handlePermanentDelete(record.id)}
                  okText={t('yes')}
                  cancelText={t('no')}
                  okButtonProps={{ danger: true }}
                >
                  <Button size="small" danger icon={<DeleteOutlined />}>
                    {t('permanentDeleteUser')}
                  </Button>
                </Popconfirm>
              </>
            )
          )}
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>{t('userList')}</Title>
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
          {t('createUser')}
        </Button>
      </div>

      <Card>
        <Table
          columns={columns}
          dataSource={users}
          rowKey="id"
          loading={loading}
          pagination={false}
          size="middle"
        />
      </Card>

      <Modal
        title={editUser ? t('edit') : t('createUser')}
        open={modalOpen}
        onCancel={() => setModalOpen(false)}
        footer={null}
        width={480}
      >
        <Form form={form} layout="vertical" onFinish={handleSubmit}>
          {!editUser && (
            <>  
              <Form.Item
                name="username"
                label={t('username')}
                rules={[
                  { required: true },
                  { min: 3, message: t('username') + ': min 3 chars' },
                  { pattern: /^[a-z0-9_-]+$/, message: t('username') + ': a-z, 0-9, _, - only' },
                ]}
              >
                <Input placeholder="username" />
              </Form.Item>
              <Form.Item
                name="password"
                label={t('password')}
                rules={[{ required: true }, { min: 6, message: t('passwordTooShort') }]}  
              >
                <Input.Password placeholder="••••••" />
              </Form.Item>
            </>
          )}
          <Form.Item
            name="displayName"
            label={t('displayName')}
            rules={[{ required: true }]}
          >
            <Input placeholder="Nguyễn Văn A" />
          </Form.Item>
          <Form.Item
            name="role"
            label={t('role')}
            rules={[{ required: true }]}
          >
            <Select
              disabled={!!editUser && editUser.id === currentUser?.id}
              options={[
                { value: 'admin', label: t('admin') },
                { value: 'engineer', label: t('engineerRole') },
                { value: 'qc', label: t('qcRole') },
              ]}
            />
          </Form.Item>
          {editUser && editUser.id === currentUser?.id && (
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: -16, marginBottom: 16 }}>
              {t('cannotChangeOwnRole')}
            </Text>
          )}  

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setModalOpen(false)}>{t('cancel')}</Button>
            <Button type="primary" htmlType="submit" loading={submitLoading}>  
              {t('save')}
            </Button>
          </div>
        </Form>
      </Modal>

      <Modal
        title={t('resetPassword') + (resetPwdUser ? ` — ${resetPwdUser.displayName}` : '')}
        open={resetPwdModal}
        onCancel={() => setResetPwdModal(false)}
        footer={null}
        width={400}
      >
        <Form form={resetPwdForm} layout="vertical" onFinish={handleResetPassword}>  
          <Form.Item
            name="password"
            label={t('newPassword')}
            rules={[{ required: true }, { min: 6, message: t('passwordTooShort') }]}
          >
            <Input.Password placeholder="••••••" />
          </Form.Item>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setResetPwdModal(false)}>{t('cancel')}</Button>
            <Button type="primary" htmlType="submit" loading={resetPwdLoading}>
              {t('resetPassword')}
            </Button>
          </div>
        </Form>
      </Modal>
    </div>
  );
}