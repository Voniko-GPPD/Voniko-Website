import React from 'react';
import { Modal, List, Button, Space, Typography } from 'antd';
import { WarningOutlined, UploadOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { useLang } from '../../contexts/LangContext';
import { removePendingUpload } from '../../utils/pendingUploads';

const { Text } = Typography;

export default function PendingUploadsModal({ open, pendingUploads, onDismissOne, onUpload, onClose }) {
  const { t } = useLang();

  return (
    <Modal
      open={open}
      title={
        <Space>
          <WarningOutlined style={{ color: '#faad14', fontSize: 18 }} />
          <span>{t('pendingUploadsTitle')}</span>
        </Space>
      }
      footer={null}
      closable={false}
      maskClosable={false}
      keyboard={false}
      width={520}
    >
      <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
        {t('pendingUploadsDesc')}
      </Text>

      <List
        bordered
        dataSource={pendingUploads}
        style={{ maxHeight: 320, overflow: 'auto' }}
        renderItem={(item) => (
          <List.Item
            actions={[
              <Button
                key="dismiss"
                size="small"
                onClick={() => {
                  removePendingUpload(item.fileId);
                  onDismissOne(item.fileId);
                }}
              >
                {t('pendingUploadsNoNeed')}
              </Button>,
              <Button
                key="upload"
                size="small"
                type="primary"
                icon={<UploadOutlined />}
                onClick={() => onUpload(item)}
              >
                {t('pendingUploadsUpload')}
              </Button>,
            ]}
          >
            <List.Item.Meta
              title={item.fileName}
              description={
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {t('pendingDownloadedAt')}: {dayjs(item.downloadedAt).format('HH:mm DD/MM/YYYY')}
                </Text>
              }
            />
          </List.Item>
        )}
      />
    </Modal>
  );
}