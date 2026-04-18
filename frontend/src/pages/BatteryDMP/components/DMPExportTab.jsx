import React, { useEffect, useState } from 'react';
import { Alert, Breadcrumb, Button, Card, Empty, Radio, Space, Spin, notification, Typography } from 'antd';
import { downloadReport, fetchTemplates } from '../../../api/dmpApi';

export default function DMPExportTab({ selection }) {
  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState('');
  const [templates, setTemplates] = useState([]);
  const [templateName, setTemplateName] = useState('');

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError('');

    fetchTemplates()
      .then((rows) => {
        if (!mounted) return;
        setTemplates(rows);
        if (rows.length > 0) setTemplateName((prev) => prev || rows[0]);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err.message || 'Failed to load templates');
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, []);

  const handleDownload = async () => {
    if (!selection || !templateName) return;

    setDownloading(true);
    try {
      await downloadReport({
        batchId: selection.batchId,
        cdmc: selection.cdmc,
        channel: selection.channel,
        templateName,
      });
      notification.success({ message: 'Report downloaded successfully' });
    } catch (err) {
      notification.error({ message: 'Failed to download report', description: err.message });
    } finally {
      setDownloading(false);
    }
  };

  if (loading) return <Spin />;

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      {error && <Alert type="error" message={error} showIcon />}

      <Breadcrumb
        items={[
          { title: `Model: ${selection?.model || '-'}` },
          { title: `Date: ${selection?.date || '-'}` },
          { title: `Batch: ${selection?.batchId || '-'}` },
          { title: `Channel: ${selection?.channel ?? '-'}` },
        ]}
      />

      <Alert
        type="info"
        showIcon
        message="Templates can be customized with {{tags}} as long as the required tags are preserved."
      />

      {templates.length === 0 ? (
        <Empty description="No templates found" />
      ) : (
        <Radio.Group value={templateName} onChange={(event) => setTemplateName(event.target.value)} style={{ width: '100%' }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {templates.map((name) => (
              <Card key={name} size="small">
                <Radio value={name}>{name}</Radio>
              </Card>
            ))}
          </Space>
        </Radio.Group>
      )}

      <Button
        type="primary"
        onClick={handleDownload}
        loading={downloading}
        disabled={!selection || !templateName}
      >
        Download Report
      </Button>

      <Typography.Text type="secondary">
        Selected template: {templateName || '-'}
      </Typography.Text>
    </Space>
  );
}
