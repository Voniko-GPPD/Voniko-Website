import React, { useEffect, useState } from 'react';
import { Alert, Breadcrumb, Button, Card, Empty, Radio, Space, Spin, notification, Typography } from 'antd';
import { downloadReport, fetchTemplates } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

export default function DMPExportTab({ stationId, selection }) {
  const { t } = useLang();
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
      notification.success({ message: t('dmpReportDownloaded') });
    } catch (err) {
      notification.error({ message: t('dmpReportDownloadFailed'), description: err.message });
    } finally {
      setDownloading(false);
    }
  };

  if (!stationId) {
    return <Empty description={t('dmpSelectStationToExport')} />;
  }

  if (loading) return <Spin />;

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      {error && <Alert type="error" message={error} showIcon />}

      <Breadcrumb
        items={[
          { title: `${t('dmpModel')}: ${selection?.model || '-'}` },
          { title: `${t('dmpDate')}: ${selection?.date || '-'}` },
          { title: `${t('dmpBatch')}: ${selection?.batchId || '-'}` },
          { title: `${t('dmpChannel')}: ${selection?.channel ?? '-'}` },
        ]}
      />

      <Alert
        type="info"
        showIcon
        message={t('dmpTemplateInfo')}
      />

      {templates.length === 0 ? (
        <Empty description={t('dmpNoTemplates')} />
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
        {t('dmpDownloadReport')}
      </Button>

      <Typography.Text type="secondary">
        {t('dmpSelectedTemplate', { name: templateName || '-' })}
      </Typography.Text>
    </Space>
  );
}
