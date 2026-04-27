import React, { useState } from 'react';
import {
  Alert, Button, Empty, Input, Select, Space, Table, Typography, notification,
} from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import { downloadDM2000PerfReport } from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const BATTERY_TYPE_OPTIONS = [
  { value: 'HP', label: 'HP' },
  { value: 'UD', label: 'UD' },
  { value: 'UD+', label: 'UD+' },
];

function detectBatteryType(dcxh) {
  if (!dcxh) return '';
  const upper = String(dcxh).toUpperCase();
  if (upper.includes('UD+') || upper.includes('UD PLUS')) return 'UD+';
  if (upper.includes('UD')) return 'UD';
  if (upper.includes('HP')) return 'HP';
  return '';
}

function autoSheetName(dcxh, serialno) {
  const parts = [dcxh, serialno].filter(Boolean);
  return parts.join(' ').trim();
}

export default function DM2000PerfReportTab({ stationId, selection }) {
  const { t } = useLang();
  const [entries, setEntries] = useState([]);
  const [downloading, setDownloading] = useState(false);

  const handleAddFromSelection = () => {
    if (!selection?.archname) return;
    setEntries((prev) => [
      ...prev,
      {
        id: Date.now(),
        archname: selection.archname,
        battery_type: detectBatteryType(selection.dcxh),
        sheet_name: '',
        _autoSheet: autoSheetName(selection.dcxh, selection.serialno),
      },
    ]);
  };

  const handleAddManual = () => {
    setEntries((prev) => [
      ...prev,
      { id: Date.now(), archname: '', battery_type: '', sheet_name: '', _autoSheet: '' },
    ]);
  };

  const handleChange = (id, field, value) => {
    setEntries((prev) =>
      prev.map((e) => (e.id === id ? { ...e, [field]: value } : e)),
    );
  };

  const handleRemove = (id) => {
    setEntries((prev) => prev.filter((e) => e.id !== id));
  };

  const handleGenerate = async () => {
    if (!stationId) {
      notification.warning({ message: t('dmpSelectStation') });
      return;
    }
    const valid = entries.filter((e) => e.archname.trim() && e.battery_type);
    if (valid.length === 0) {
      notification.warning({ message: t('dm2000PerfReportNoValidEntries') });
      return;
    }
    setDownloading(true);
    try {
      await downloadDM2000PerfReport({
        stationId,
        entries: valid.map((e) => ({
          archname: e.archname.trim(),
          battery_type: e.battery_type,
          sheet_name: e.sheet_name.trim(),
          batys: [],
        })),
      });
      notification.success({ message: t('dmpReportDownloaded') });
    } catch (err) {
      notification.error({ message: t('dmpReportDownloadFailed'), description: err.message });
    } finally {
      setDownloading(false);
    }
  };

  const columns = [
    {
      title: t('dm2000PerfReportArchname'),
      dataIndex: 'archname',
      width: 220,
      render: (val, record) => (
        <Input
          size="small"
          value={val}
          onChange={(e) => handleChange(record.id, 'archname', e.target.value)}
          placeholder="e.g. QC2026/4/18"
        />
      ),
    },
    {
      title: t('dm2000PerfReportBatteryGrade'),
      dataIndex: 'battery_type',
      width: 120,
      render: (val, record) => (
        <Select
          size="small"
          style={{ width: '100%' }}
          value={val || undefined}
          placeholder="HP / UD / UD+"
          options={BATTERY_TYPE_OPTIONS}
          onChange={(v) => handleChange(record.id, 'battery_type', v)}
        />
      ),
    },
    {
      title: t('dm2000PerfReportSheetName'),
      dataIndex: 'sheet_name',
      render: (val, record) => (
        <Input
          size="small"
          value={val}
          onChange={(e) => handleChange(record.id, 'sheet_name', e.target.value)}
          placeholder={record._autoSheet || t('dm2000PerfReportSheetNamePlaceholder')}
        />
      ),
    },
    {
      title: '',
      width: 60,
      render: (_, record) => (
        <Button
          size="small"
          danger
          icon={<DeleteOutlined />}
          onClick={() => handleRemove(record.id)}
        />
      ),
    },
  ];

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Alert
        type="info"
        showIcon
        message={t('dm2000PerfReportHint')}
      />

      <Space wrap>
        <Button
          icon={<PlusOutlined />}
          onClick={handleAddFromSelection}
          disabled={!selection?.archname}
          title={!selection?.archname ? t('dm2000PerfReportNoSelection') : undefined}
        >
          {t('dm2000PerfReportAddFromSelection')}
        </Button>
        <Button icon={<PlusOutlined />} onClick={handleAddManual}>
          {t('dm2000PerfReportAddManual')}
        </Button>
      </Space>

      {entries.length === 0 ? (
        <Empty description={t('dm2000PerfReportNoEntries')} />
      ) : (
        <Table
          size="small"
          dataSource={entries}
          columns={columns}
          rowKey="id"
          pagination={false}
          bordered
        />
      )}

      {entries.length > 0 && (
        <Space direction="vertical" size={4}>
          <Typography.Text type="secondary">
            {t('dm2000PerfReportInfo', { count: entries.filter((e) => e.archname.trim() && e.battery_type).length })}
          </Typography.Text>
          <Button
            type="primary"
            onClick={handleGenerate}
            loading={downloading}
            disabled={!stationId || entries.every((e) => !e.archname.trim() || !e.battery_type)}
          >
            {t('dm2000PerfReportGenerate')}
          </Button>
        </Space>
      )}
    </Space>
  );
}
