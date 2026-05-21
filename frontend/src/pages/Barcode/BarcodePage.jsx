import React, { useState } from 'react';
import {
  Card, Button, Typography, Upload, Space, Alert, Descriptions, Tag, Spin,
} from 'antd';
import { InboxOutlined, BarcodeOutlined, FilePdfOutlined } from '@ant-design/icons';
import { useLang } from '../../contexts/LangContext';
import { generateBarcodePdf } from '../../api/barcode';

const { Title, Text } = Typography;
const { Dragger } = Upload;

const ACCEPTED_EXTENSIONS = ['.csv', '.xlsx', '.xls'];
const ACCEPTED_MIME = [
  'text/csv',
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
];

const STATUS = {
  GENERATING: 'generating',
  SUCCESS: 'success',
  ERROR: 'error',
};

function isValidFile(file) {
  const ext = file.name?.split('.').pop()?.toLowerCase();
  return ACCEPTED_EXTENSIONS.includes(`.${ext}`) || ACCEPTED_MIME.includes(file.type);
}

export default function BarcodePage() {
  const { t } = useLang();

  const [selectedFile, setSelectedFile] = useState(null);
  const [status, setStatus]             = useState(null); // 'generating' | 'success' | 'error'
  const [errorMsg, setErrorMsg]         = useState('');

  const handleGenerate = async () => {
    if (!selectedFile) {
      setStatus(STATUS.ERROR);
      setErrorMsg(t('barcodeNoFile'));
      return;
    }

    setStatus(STATUS.GENERATING);
    setErrorMsg('');

    try {
      const blob = await generateBarcodePdf(selectedFile);

      // Trigger browser download
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'barcodes.pdf';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);

      setStatus(STATUS.SUCCESS);
    } catch (err) {
      let msg = t('barcodeError');
      if (err?.response?.data) {
        const data = err.response.data;
        if (data instanceof Blob) {
          try {
            const text = await data.text();
            const parsed = JSON.parse(text);
            msg = parsed.error || text;
          } catch {
            // keep default message
          }
        } else if (typeof data === 'object' && data.error) {
          msg = data.error;
        }
      } else if (err.message) {
        msg = err.message;
      }
      setErrorMsg(msg);
      setStatus(STATUS.ERROR);
    }
  };

  const uploadProps = {
    name: 'file',
    multiple: false,
    accept: '.csv,.xlsx,.xls',
    beforeUpload: (file) => {
      if (!isValidFile(file)) {
        setStatus(STATUS.ERROR);
        setErrorMsg(t('barcodeFileInvalid'));
        return Upload.LIST_IGNORE;
      }
      setSelectedFile(file);
      setStatus(null);
      setErrorMsg('');
      return false; // Prevent auto-upload
    },
    onRemove: () => {
      setSelectedFile(null);
      setStatus(null);
      setErrorMsg('');
    },
    fileList: selectedFile ? [{ uid: '1', name: selectedFile.name, status: 'done' }] : [],
  };

  return (
    <div style={{ maxWidth: 720, margin: '0 auto' }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* Header */}
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>
            <BarcodeOutlined style={{ marginRight: 8 }} />
            {t('barcodeTitle')}
          </Title>
          <Text type="secondary">{t('barcodeSubtitle')}</Text>
        </div>

        {/* Upload area */}
        <Card>
          <Dragger {...uploadProps}>
            <p className="ant-upload-drag-icon">
              <InboxOutlined style={{ fontSize: 40, color: '#1677ff' }} />
            </p>
            <p className="ant-upload-text">{t('barcodeUploadHint')}</p>
            <p className="ant-upload-hint">CSV / XLSX / XLS</p>
          </Dragger>
        </Card>

        {/* Selected file info */}
        {selectedFile && (
          <Card size="small" title={t('barcodePreviewInfo')}>
            <Descriptions size="small" column={1}>
              <Descriptions.Item label={t('fileName')}>{selectedFile.name}</Descriptions.Item>
              <Descriptions.Item label={t('fileSize')}>
                {(selectedFile.size / 1024).toFixed(1)} KB
              </Descriptions.Item>
              <Descriptions.Item label={t('status')}>
                <Tag color="blue">
                  {selectedFile.name.split('.').pop().toUpperCase()}
                </Tag>
              </Descriptions.Item>
            </Descriptions>
          </Card>
        )}

        {/* Status alerts */}
        {status === STATUS.SUCCESS && (
          <Alert
            type="success"
            message={t('barcodeSuccess')}
            icon={<FilePdfOutlined />}
            showIcon
            closable
            onClose={() => setStatus(null)}
          />
        )}
        {status === STATUS.ERROR && errorMsg && (
          <Alert
            type="error"
            message={t('barcodeError')}
            description={errorMsg}
            showIcon
            closable
            onClose={() => setStatus(null)}
          />
        )}

        {/* Generate button */}
        <Button
          type="primary"
          size="large"
          icon={status === STATUS.GENERATING ? <Spin size="small" /> : <FilePdfOutlined />}
          onClick={handleGenerate}
          disabled={!selectedFile || status === STATUS.GENERATING}
          style={{ width: '100%' }}
        >
          {status === STATUS.GENERATING ? t('barcodeGenerating') : t('barcodeGenerate')}
        </Button>
      </Space>
    </div>
  );
}
