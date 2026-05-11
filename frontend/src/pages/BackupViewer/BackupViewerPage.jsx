import React, { useEffect, useState, useCallback } from 'react';
import {
  Button, Card, Table, Tag, Typography, Spin, Space, message,
  Modal, Form, Input, Tooltip, Alert,
} from 'antd';
import {
  WarningOutlined, ArrowLeftOutlined, DownloadOutlined, RollbackOutlined,
} from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import dayjs from 'dayjs';
import { getBackupFiles, restoreBackupFile, downloadBackupFile } from '../../api';
import { useLang } from '../../contexts/LangContext';

const { Title, Text } = Typography;

function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

function ExistsTag({ status, t }) {
  if (status === 'yes') return <Tag color="success">{t('existsYes')}</Tag>;
  if (status === 'deleted') return <Tag color="warning">{t('existsDeleted')}</Tag>;
  return <Tag color="default">{t('existsNo')}</Tag>;
}

export default function BackupViewerPage() {
  const { name } = useParams();
  const navigate = useNavigate();
  const { t } = useLang();

  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedRows, setExpandedRows] = useState([]);
  const [incompatibleBackup, setIncompatibleBackup] = useState(false);

  // Restore modal state
  const [restoreModal, setRestoreModal] = useState(false);
  const [restoreTarget, setRestoreTarget] = useState(null); // { fileId, versionId, fileName, versionNumber }
  const [restoreLoading, setRestoreLoading] = useState(false);
  const [restoreError, setRestoreError] = useState('');
  const [form] = Form.useForm();

  const fetchFiles = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getBackupFiles(name);
      setFiles(res.data.files || []);
    } catch (err) {
      if (err.response?.status === 422) {
        setIncompatibleBackup(true);
      } else {
        message.error(err.response?.data?.message || t('error'));
      }
    } finally {
      setLoading(false);
    }
  }, [name, t]);

  useEffect(() => { fetchFiles(); }, [fetchFiles]);

  const openRestoreModal = (fileId, versionId, fileName, versionNumber) => {
    setRestoreTarget({ fileId, versionId, fileName, versionNumber });
    setRestoreError('');
    form.resetFields();
    setRestoreModal(true);
  };

  const handleRestore = async () => {
    let values;
    try {
      values = await form.validateFields();
    } catch {
      return;
    }
    setRestoreLoading(true);
    setRestoreError('');
    try {
      await restoreBackupFile(name, {
        fileId: restoreTarget.fileId,
        versionId: restoreTarget.versionId,
        adminPassword: values.adminPassword,
      });
      message.success(t('restoreFromBackupSuccess'));
      setRestoreModal(false);
      fetchFiles();
    } catch (err) {
      if (err.response?.status === 401) {
        setRestoreError(t('invalidAdminPassword'));
      } else {
        setRestoreError(err.response?.data?.message || t('error'));
      }
    } finally {
      setRestoreLoading(false);
    }
  };

  const handleDownload = async (fileId, versionId, fileName) => {
    try {
      const res = await downloadBackupFile(name, fileId, versionId);
      const disposition = res.headers['content-disposition'];
      let downloadName = fileName;
      if (disposition) {
        const match = disposition.match(/filename\*?=(?:UTF-8'')?["']?([^"';\n]+)/i);
        if (match) downloadName = decodeURIComponent(match[1]);
      }
      const url = window.URL.createObjectURL(new Blob([res.data]));
      const a = document.createElement('a');
      a.href = url;
      a.download = downloadName;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      message.error(err.response?.data?.message || t('error'));
    }
  };

  const expandedRowRender = (record) => {
    const versionColumns = [
      {
        title: t('versionNumber'),
        dataIndex: 'versionNumber',
        render: (v) => `v${v}`,
        width: 80,
      },
      {
        title: t('fileSize'),
        dataIndex: 'size',
        render: (v) => formatBytes(v),
        width: 100,
      },
      {
        title: t('commitMessage'),
        dataIndex: 'commitMessage',
        render: (v) => v || '-',
      },
      {
        title: t('createdAt'),
        dataIndex: 'createdAt',
        render: (v) => dayjs(v).format('YYYY-MM-DD HH:mm'),
        width: 150,
      },
      {
        title: t('uploadedBy'),
        dataIndex: 'uploadedBy',
        render: (v) => v || '-',
        width: 120,
      },
      {
        title: t('actions'),
        key: 'actions',
        width: 200,
        render: (_, ver) => (
          <Space>
            <Tooltip title={t('downloadFromBackup')}>
              <Button
                size="small"
                icon={<DownloadOutlined />}
                onClick={() => handleDownload(record.id, ver.id, record.name)}
              >
                {t('download')}
              </Button>
            </Tooltip>
            <Tooltip title={t('restoreFromBackup')}>
              <Button
                size="small"
                type="primary"
                danger
                icon={<RollbackOutlined />}
                onClick={() => openRestoreModal(record.id, ver.id, record.name, ver.versionNumber)}
              >
                {t('restore')}
              </Button>
            </Tooltip>
          </Space>
        ),
      },
    ];

    return (
      <Table
        columns={versionColumns}
        dataSource={record.versions || []}
        rowKey="id"
        pagination={false}
        size="small"
        style={{ marginLeft: 48 }}
      />
    );
  };

  const columns = [
    {
      title: t('fileName'),
      dataIndex: 'name',
      render: (v) => <Text strong>{v}</Text>,
    },
    {
      title: t('filePath'),
      dataIndex: 'path',
    },
    {
      title: t('versions'),
      dataIndex: 'versionCount',
      width: 90,
      render: (v) => <Tag>{v}</Tag>,
    },
    {
      title: t('fileSize'),
      dataIndex: 'totalSize',
      width: 110,
      render: (v) => formatBytes(v),
    },
    {
      title: t('existsInCurrent'),
      dataIndex: 'existsInCurrent',
      width: 150,
      render: (v) => <ExistsTag status={v} t={t} />,
    },
    {
      title: t('actions'),
      key: 'actions',
      width: 220,
      render: (_, record) => {
        const latestVer = record.versions && record.versions[record.versions.length - 1];
        return (
          <Space>
            <Tooltip title={t('downloadFromBackup')}>
              <Button
                size="small"
                icon={<DownloadOutlined />}
                disabled={!latestVer}
                onClick={() => latestVer && handleDownload(record.id, latestVer.id, record.name, latestVer.versionNumber)}
              >
                {t('download')}
              </Button>
            </Tooltip>
            <Tooltip title={t('restoreFromBackup')}>
              <Button
                size="small"
                type="primary"
                danger
                icon={<RollbackOutlined />}
                disabled={!latestVer}
                onClick={() => latestVer && openRestoreModal(record.id, latestVer.id, record.name, latestVer.versionNumber)}
              >
                {t('restore')}
              </Button>
            </Tooltip>
          </Space>
        );
      },
    },
  ];

  const restoreWarningText = restoreTarget
    ? t('restoreWarning')
        .replace('{fileName}', restoreTarget.fileName)
        .replace('{backupName}', name)
    : '';

  return (
    <div style={{ minHeight: '100vh', background: '#fff8f0', padding: '0 0 40px 0' }}>
      {/* Warning banner */}
      <div style={{
        background: '#fa8c16',
        color: '#fff',
        padding: '12px 24px',
        fontWeight: 700,
        fontSize: 15,
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        letterSpacing: 0.5,
      }}>
        <WarningOutlined style={{ fontSize: 20 }} />
        <span>⚠️ {t('backupViewerWarning')}</span>
        <Tag color="red" style={{ marginLeft: 12, fontWeight: 700, fontSize: 13 }}>BACKUP</Tag>
      </div>

      <div style={{ maxWidth: 1200, margin: '0 auto', padding: '24px 24px 0' }}>
        {/* Back button */}
        <Button
          icon={<ArrowLeftOutlined />}
          onClick={() => navigate('/')}
          style={{ marginBottom: 16 }}
        >
          {t('backToBackups')}
        </Button>

        {/* Backup info card */}
        <Card
          title={<span style={{ color: '#fa8c16' }}>{t('backupInfo')}</span>}
          style={{ marginBottom: 20, borderColor: '#fa8c16', borderWidth: 1.5 }}
          styles={{ header: { background: '#fff3e0' } }}
        >
          <Space size="large" wrap>
            <div>
              <Text type="secondary">{t('backupName')}:</Text>{' '}
              <Text strong>{name}</Text>
            </div>
            <div>
              <Text type="secondary">{t('backupFileCount')}:</Text>{' '}
              <Tag color="blue">{files.length}</Tag>
            </div>
          </Space>
        </Card>

        {/* Files table */}
        <Card
          title={t('backupFiles')}
          style={{ borderColor: '#fa8c16', borderWidth: 1.5 }}
          styles={{ header: { background: '#fff3e0' } }}
        >
          {loading ? (
            <div style={{ textAlign: 'center', padding: 60 }}>
              <Spin size="large" />
            </div>
          ) : incompatibleBackup ? (
            <Alert
              type="warning"
              showIcon
              message={t('incompatibleBackupTitle')}
              description={t('incompatibleBackupDesc')}
              style={{ margin: '16px 0' }}
            />
          ) : (
            <Table
              dataSource={files}
              rowKey="id"
              columns={columns}
              expandable={{
                expandedRowRender,
                expandedRowKeys: expandedRows,
                onExpandedRowsChange: setExpandedRows,
              }}
              pagination={{ pageSize: 20, showSizeChanger: false }}
              size="small"
              locale={{ emptyText: t('noFiles') }}
            />
          )}
        </Card>
      </div>

      {/* Restore confirmation modal */}
      <Modal
        open={restoreModal}
        title={
          <Space>
            <WarningOutlined style={{ color: '#ff4d4f', fontSize: 20 }} />
            <span style={{ color: '#ff4d4f' }}>{t('restoreFromBackup')}</span>
          </Space>
        }
        onCancel={() => setRestoreModal(false)}
        footer={[
          <Button key="cancel" onClick={() => setRestoreModal(false)}>
            {t('cancel')}
          </Button>,
          <Button
            key="confirm"
            type="primary"
            danger
            loading={restoreLoading}
            onClick={handleRestore}
          >
            {t('confirm')}
          </Button>,
        ]}
        destroyOnClose
      >
        <div style={{ marginBottom: 16 }}>
          <Text>{restoreWarningText}</Text>
        </div>
        <div style={{ marginBottom: 16, padding: '8px 12px', background: '#fff1f0', borderRadius: 6, border: '1px solid #ffccc7' }}>
          <Text type="danger" strong>{t('restoreCannotUndo')}</Text>
        </div>
        <Form form={form} layout="vertical">
          <Form.Item
            name="adminPassword"
            label={t('enterAdminPassword')}
            rules={[{ required: true, message: t('adminPasswordRequired') }]}
            validateStatus={restoreError ? 'error' : undefined}
            help={restoreError || undefined}
          >
            <Input.Password autoFocus />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
