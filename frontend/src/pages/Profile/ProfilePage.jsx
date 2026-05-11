import React, { useState, useEffect, useRef } from 'react';
import {
  Card, Form, Input, Button, Avatar, Typography, Space,
  message, Divider, Row, Col, Tag, Upload,
} from 'antd';
import { UserOutlined, LockOutlined, SaveOutlined, UploadOutlined } from '@ant-design/icons';
import api from '../../api';
import { useAuth } from '../../contexts/AuthContext';
import { useLang } from '../../contexts/LangContext';

const { Title, Text } = Typography;

export default function ProfilePage() {
  const { user, refreshUser } = useAuth();
  const { t } = useLang();
  const [profileLoading, setProfileLoading] = useState(false);
  const [passwordLoading, setPasswordLoading] = useState(false);
  const [uploadingAvatar, setUploadingAvatar] = useState(false);
  const [profileForm] = Form.useForm();
  const [passwordForm] = Form.useForm();
  const userIdRef = useRef(null);

  useEffect(() => {
    if (user) {
      // Only reset form when user changes (login/logout), not when avatar/profile updates
      if (userIdRef.current !== user.id) {
        userIdRef.current = user.id;
        profileForm.setFieldsValue({
          displayName: user.displayName,
          avatarUrl: user.avatarUrl,
        });
      }
    }
  }, [user]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleProfileUpdate = async (values) => {
    setProfileLoading(true);
    try {
      await api.put('/users/me/profile', {
        displayName: values.displayName,
        avatarUrl: values.avatarUrl || null,
      });
      message.success(t('profileUpdated'));
      await refreshUser();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setProfileLoading(false);
    }
  };

  const handleAvatarUpload = async ({ file, onSuccess, onError }) => {
    const formData = new FormData();
    formData.append('avatar', file);
    setUploadingAvatar(true);
    try {
      const res = await api.post('/users/me/avatar', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      // Save current displayName before refreshUser to prevent race condition
      const currentDisplayName = profileForm.getFieldValue('displayName') || user?.displayName;
      await refreshUser();
      // After refreshUser, restore displayName and set new avatarUrl explicitly
      profileForm.setFieldsValue({
        displayName: currentDisplayName,
        avatarUrl: res.data.avatarUrl,
      });
      message.success(t('avatarUploaded'));
      onSuccess(res.data);
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
      onError(err);
    } finally {
      setUploadingAvatar(false);
    }
  };

  const handlePasswordChange = async (values) => {
    if (values.newPassword !== values.confirmPassword) {
      message.error(t('passwordMismatch'));
      return;
    }
    setPasswordLoading(true);
    try {
      await api.put('/users/me/password', {
        currentPassword: values.currentPassword,
        newPassword: values.newPassword,
      });
      message.success(t('passwordChanged'));
      passwordForm.resetFields();
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    } finally {
      setPasswordLoading(false);
    }
  };

  const roleLabel = (role) => {
    const map = { admin: t('admin'), user: t('userRole'), viewer: t('viewer') };
    return map[role] || role;
  };

  const roleColor = (role) => {
    const map = { admin: 'red', user: 'blue', viewer: 'default' };
    return map[role] || 'default';
  };

  return (
    <div>
      <Title level={3} style={{ marginBottom: 24 }}>{t('myProfile')}</Title>

      <Row gutter={[16, 16]}>
        {/* User info card */}
        <Col xs={24} md={8}>
          <Card>
            <div style={{ textAlign: 'center', padding: '16px 0' }}>
              <Avatar
                size={80}
                src={user?.avatarUrl}
                icon={<UserOutlined />}
                style={{ background: '#1677ff', marginBottom: 16 }}
              />
              <div>
                <Title level={4} style={{ margin: 0 }}>{user?.displayName}</Title>
                <Text type="secondary">@{user?.username}</Text>
              </div>
              <div style={{ marginTop: 8 }}>
                <Tag color={roleColor(user?.role)}>{roleLabel(user?.role)}</Tag>
              </div>
            </div>
          </Card>
        </Col>

        <Col xs={24} md={16}>
          {/* Update profile */}
          <Card title={t('myProfile')} style={{ marginBottom: 16 }}>
            <Form
              form={profileForm}
              layout="vertical"
              initialValues={{
                displayName: user?.displayName,
                avatarUrl: user?.avatarUrl,
              }}
              onFinish={handleProfileUpdate}
            >
              <Form.Item
                name="displayName"
                label={t('displayName')}
                rules={[{ required: true }]}
              >
                <Input prefix={<UserOutlined />} placeholder="Nguyễn Văn A" />
              </Form.Item>
              <Form.Item name="avatarUrl" label={`${t('avatarUrl')} ${t('optional')}`}>
                <Input
                  placeholder="https://..."
                  addonAfter={
                    <Upload
                      accept="image/*"
                      showUploadList={false}
                      customRequest={handleAvatarUpload}
                      beforeUpload={(file) => {
                        if (file.size > 5 * 1024 * 1024) {
                          message.error(t('fileTooLarge'));
                          return Upload.LIST_IGNORE;
                        }
                        return true;
                      }}
                    >
                      <Button
                        size="small"
                        icon={<UploadOutlined />}
                        loading={uploadingAvatar}
                        type="link"
                        style={{ padding: '0 4px' }}
                      >
                        {t('upload')}
                      </Button>
                    </Upload>
                  }
                />
              </Form.Item>
              <Button type="primary" htmlType="submit" loading={profileLoading} icon={<SaveOutlined />}>
                {t('save')}
              </Button>
            </Form>
          </Card>

          {/* Change password */}
          <Card title={t('changePassword')}>
            <Form form={passwordForm} layout="vertical" onFinish={handlePasswordChange}>
              <Form.Item
                name="currentPassword"
                label={t('currentPassword')}
                rules={[{ required: true }]}
              >
                <Input.Password prefix={<LockOutlined />} />
              </Form.Item>
              <Form.Item
                name="newPassword"
                label={t('newPassword')}
                rules={[
                  { required: true },
                  { min: 6, message: t('passwordTooShort') },
                ]}
              >
                <Input.Password prefix={<LockOutlined />} />
              </Form.Item>
              <Form.Item
                name="confirmPassword"
                label={t('confirmPassword')}
                rules={[{ required: true }]}
              >
                <Input.Password prefix={<LockOutlined />} />
              </Form.Item>
              <Button type="primary" htmlType="submit" loading={passwordLoading} icon={<SaveOutlined />}>
                {t('save')}
              </Button>
            </Form>
          </Card>
        </Col>
      </Row>
    </div>
  );
}
