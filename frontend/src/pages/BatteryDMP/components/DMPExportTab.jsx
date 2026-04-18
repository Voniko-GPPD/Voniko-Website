import React, { useEffect, useState } from 'react';
import { Alert, Breadcrumb, Button, Card, Empty, Radio, Space, Spin, notification, Typography } from 'antd';
import { downloadReport, fetchTemplates } from '../../../api/dmpApi';

export default function DMPExportTab({ stationId, selection }) {
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState('');
  const [templates, setTemplates] = useState([]);
  const [templateName, setTemplateName] = useState('');

  useEffect(() => {
    setTemplates([]);
    setTemplateName('');

    if (!stationId) {
      setLoading(false);
      return () => {};
    }

    let mounted = true;
    setLoading(true);
    setError('');

    fetchTemplates(stationId)
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
  }, [stationId]);

  const handleDownload = async () => {
    if (!stationId || !selection || !templateName) return;

    setDownloading(true);
    try {
      await downloadReport({
        stationId,
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

  if (!stationId) {
    return <Empty description="Select a station to export reports" />;
  }

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
        disabled={!stationId || !selection || !templateName}
      >
        Download Report
      </Button>

      <Typography.Text type="secondary">
        Selected template: {templateName || '-'}
      </Typography.Text>
    </Space>
  );
}
