import React, { useEffect, useState } from 'react';
import { Alert, Breadcrumb, Button, Card, Empty, Radio, Select, Space, Spin, Typography, notification } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { downloadReport, downloadSimpleReport, fetchChannels, fetchTemplates } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

export default function DMPExportTab({ stationId, selection }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [channelLoading, setChannelLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [downloadingTemplate, setDownloadingTemplate] = useState(false);
  const [error, setError] = useState('');
  const [templates, setTemplates] = useState([]);
  const [templateName, setTemplateName] = useState('');
  const [channels, setChannels] = useState([]);
  const [selectedBaty, setSelectedBaty] = useState(null);

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

  // Load channels when selection changes
  useEffect(() => {
    setChannels([]);
    setSelectedBaty(null);
    if (!stationId || !selection?.id) {
      setChannelLoading(false);
      return () => {};
    }
    let active = true;
    setChannelLoading(true);
    fetchChannels(stationId, selection.id)
      .then((result) => {
        if (!active) return;
        const sorted = (result || [])
          .filter((ch) => ch.baty != null && Number.isFinite(Number(ch.baty)) && Number(ch.baty) > 0)
          .sort((a, b) => Number(a.baty) - Number(b.baty));
        setChannels(sorted);
        if (sorted.length > 0) setSelectedBaty(Number(sorted[0].baty));
      })
      .catch((err) => {
        if (!active) return;
        setError(err.message || 'Failed to load channels');
      })
      .finally(() => {
        if (!active) return;
        setChannelLoading(false);
      });
    return () => { active = false; };
  }, [stationId, selection?.id]);

  const selectedChannel = channels.find((ch) => Number(ch.baty) === selectedBaty);

  const handleDownloadSimple = async () => {
    if (!stationId || !selection || !selectedChannel) return;

    setDownloading(true);
    try {
      await downloadSimpleReport({
        stationId,
        batchId: selection.id,
        cdmc: selectedChannel.cdmc,
        channel: selectedChannel.baty,
      });
      notification.success({ message: t('dmpReportDownloaded') });
    } catch (err) {
      notification.error({ message: t('dmpReportDownloadFailed'), description: err.message });
    } finally {
      setDownloading(false);
    }
  };

  const handleDownloadTemplate = async () => {
    if (!stationId || !selection || !templateName || !selectedChannel) return;

    setDownloadingTemplate(true);
    try {
      await downloadReport({
        stationId,
        batchId: selection.id,
        cdmc: selectedChannel.cdmc,
        channel: selectedChannel.baty,
        templateName,
      });
      notification.success({ message: t('dmpReportDownloaded') });
    } catch (err) {
      notification.error({ message: t('dmpReportDownloadFailed'), description: err.message });
    } finally {
      setDownloadingTemplate(false);
    }
  };

  if (!stationId) {
    return <Empty description={t('dmpSelectStationToExport')} />;
  }

  if (loading) return <Spin />;

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      {error && <Alert type="error" message={error} showIcon />}

      {!selection ? (
        <Empty description={t('dmpSelectBatchToExport')} />
      ) : (
        <>
          <Breadcrumb
            items={[
              { title: `${t('dm2000Type')}: ${selection.dcxh || '-'}` },
              { title: `${t('dm2000StartDate')}: ${selection.fdrq || '-'}` },
              { title: `${t('dmpBatchId')}: ${selection.id || '-'}` },
            ]}
          />

          <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <Typography.Text strong>{t('dmpChannel')}:</Typography.Text>
            <Select
              style={{ width: 200 }}
              value={selectedBaty}
              onChange={setSelectedBaty}
              loading={channelLoading}
              placeholder={t('dmpSelectChannel')}
              options={channels.map((ch) => ({ value: Number(ch.baty), label: `CH ${ch.baty}` }))}
            />
          </div>

          <Button
            type="primary"
            icon={<DownloadOutlined />}
            onClick={handleDownloadSimple}
            loading={downloading}
            disabled={!selectedChannel}
          >
            {t('dmpDownloadReport')}
          </Button>

          {templates.length > 0 && (
            <Card size="small" title={t('dmpTemplateInfo')}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Radio.Group value={templateName} onChange={(event) => setTemplateName(event.target.value)} style={{ width: '100%' }}>
                  <Space direction="vertical" style={{ width: '100%' }}>
                    {templates.map((name) => (
                      <Card key={name} size="small">
                        <Radio value={name}>{name}</Radio>
                      </Card>
                    ))}
                  </Space>
                </Radio.Group>
                <Button
                  onClick={handleDownloadTemplate}
                  loading={downloadingTemplate}
                  disabled={!templateName || !selectedChannel}
                >
                  {t('dmpDownloadReport')} ({templateName || '-'})
                </Button>
              </Space>
            </Card>
          )}
        </>
      )}
    </Space>
  );
}
