import React, { useState } from 'react';
import { Card, Form, Input, Button, Typography, Space, Alert, message } from 'antd';
import { UserOutlined, LockOutlined, GlobalOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useLang } from '../../contexts/LangContext';

const { Title, Text } = Typography;

export default function LoginPage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const { login } = useAuth();
  const { t, lang, switchLang } = useLang();
  const navigate = useNavigate();

  const onFinish = async ({ username, password }) => {
    setLoading(true);
    setError('');
    try {
      await login(username, password);
      message.success(t('loginSuccess'));
      navigate('/');
    } catch (err) {
      setError(err.response?.data?.message || t('loginFailed'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: 24,
    }}>
      <div style={{ width: '100%', maxWidth: 420 }}>
        {/* Logo/Title */}
        <div style={{ textAlign: 'center', marginBottom: 32 }}>
          <img
            src="/logo.avif"
            alt="Voniko"
            style={{ height: 64, maxWidth: 200, objectFit: 'contain', marginBottom: 8 }}
          />
          <br />
          <Text style={{ color: 'rgba(255,255,255,0.8)', fontSize: 14 }}>
            {t('appTagline')}
          </Text>
        </div>

        <Card
          style={{ borderRadius: 16, boxShadow: '0 8px 40px rgba(0,0,0,0.15)' }}
          bodyStyle={{ padding: 32 }}
        >
          <Title level={3} style={{ textAlign: 'center', marginBottom: 24 }}>
            {t('loginTitle')}
          </Title>

          {error && (
            <Alert
              message={error}
              type="error"
              showIcon
              style={{ marginBottom: 16 }}
              closable
              onClose={() => setError('')}
            />
          )}

          <Form layout="vertical" onFinish={onFinish} size="large">
            <Form.Item
              name="username"
              rules={[{ required: true, message: `${t('username')} required` }]}
            >
              <Input
                prefix={<UserOutlined style={{ color: '#bfbfbf' }} />}
                placeholder={t('username')}
              />
            </Form.Item>

            <Form.Item
              name="password"
              rules={[{ required: true, message: `${t('password')} required` }]}
            >
              <Input.Password
                prefix={<LockOutlined style={{ color: '#bfbfbf' }} />}
                placeholder={t('password')}
              />
            </Form.Item>

            <Button type="primary" htmlType="submit" loading={loading} block style={{ height: 44 }}>
              {t('login')}
            </Button>
          </Form>

          <div style={{ textAlign: 'center', marginTop: 16 }}>
            <Space>
              <GlobalOutlined style={{ color: '#999' }} />
              <Button
                type={lang === 'vi' ? 'link' : 'text'}
                size="small"
                onClick={() => switchLang('vi')}
                style={{ padding: '0 4px', fontWeight: lang === 'vi' ? 600 : 400 }}
              >
                Tiếng Việt
              </Button>
              <span style={{ color: '#d9d9d9' }}>|</span>
              <Button
                type={lang === 'en' ? 'link' : 'text'}
                size="small"
                onClick={() => switchLang('en')}
                style={{ padding: '0 4px', fontWeight: lang === 'en' ? 600 : 400 }}
              >
                English
              </Button>
              <span style={{ color: '#d9d9d9' }}>|</span>
              <Button
                type={lang === 'zh' ? 'link' : 'text'}
                size="small"
                onClick={() => switchLang('zh')}
                style={{ padding: '0 4px', fontWeight: lang === 'zh' ? 600 : 400 }}
              >
                中文
              </Button>
            </Space>
          </div>
        </Card>
      </div>
    </div>
  );
}
