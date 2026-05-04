import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  DatePicker,
  Empty,
  Form,
  Input,
  Popconfirm,
  Select,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Typography,
  notification,
} from 'antd';
import {
  DeleteOutlined,
  DownloadOutlined,
  EditOutlined,
  PlusOutlined,
  SaveOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import {
  createPerfEntry,
  deletePerfEntry,
  downloadDmpPerfReport,
  fetchDmpPerfTemplates,
  fetchPerfEntries,
  fetchStations,
  updatePerfEntry,
  uploadDmpPerfTemplate,
} from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const SPECIAL_TYPES = ['normal', '6020', '3thang', '6thang'];
const LOAI_OPTIONS = ['UD', 'UD+', 'HP'].map((v) => ({ value: v, label: v }));

// Derives default tray assignment from group count (fixed convention)
function autoTrays(groupCount, groupIndex) {
  if (groupCount === 1) return [1, 2, 3, 4, 5, 6, 7, 8, 9];
  if (groupCount === 2) return groupIndex === 0 ? [1, 2, 3, 4] : [6, 7, 8, 9];
  if (groupCount === 3) {
    if (groupIndex === 0) return [1, 2, 3];
    if (groupIndex === 1) return [4, 5, 6];
    return [7, 8, 9];
  }
  return [];
}

// ─── Group editor ─────────────────────────────────────────────────────────────

function GroupEditor({ groups, onChange }) {
  const { t } = useLang();

  const handleGroupChange = (idx, field, value) => {
    const next = groups.map((g, i) => (i === idx ? { ...g, [field]: value } : g));
    onChange(next);
  };

  const handleAddGroup = () => {
    onChange([...groups, { loai: 'UD', chuyen: '', trays: [] }]);
  };

  const handleRemoveGroup = (idx) => {
    onChange(groups.filter((_, i) => i !== idx));
  };

  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      {groups.map((grp, idx) => (
        <Card
          key={idx}
          size="small"
          style={{ background: '#fafafa' }}
          bodyStyle={{ padding: '6px 10px' }}
        >
          <Space wrap size={6} align="center">
            <Select
              size="small"
              style={{ width: 90 }}
              value={grp.loai}
              options={LOAI_OPTIONS}
              placeholder={t('dmpPerfGroupLoai')}
              onChange={(v) => handleGroupChange(idx, 'loai', v)}
            />
            <Input
              size="small"
              style={{ width: 90 }}
              value={grp.chuyen}
              placeholder={t('dmpPerfGroupChuyen')}
              onChange={(e) => handleGroupChange(idx, 'chuyen', e.target.value)}
            />
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {t('dmpPerfGroupTrays')}: {(grp.trays && grp.trays.length ? grp.trays : autoTrays(groups.length, idx)).join(',')}
            </Typography.Text>
            <Button
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleRemoveGroup(idx)}
            />
          </Space>
        </Card>
      ))}
      <Button size="small" icon={<PlusOutlined />} onClick={handleAddGroup}>
        {t('dmpPerfAddGroup')}
      </Button>
    </Space>
  );
}

// ─── Entry form (inline) ──────────────────────────────────────────────────────

function EntryForm({ initial, stationId, onSave, onCancel }) {
  const { t } = useLang();
  const [form] = Form.useForm();
  const [groups, setGroups] = useState(initial?.groups || [{ loai: 'UD', chuyen: '', trays: [] }]);
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);
      const payload = {
        station_id: stationId,
        batch_id: values.batch_id,
        report_date: values.report_date ? values.report_date.format('YYYY-MM-DD') : '',
        model: values.model,
        groups,
        special_type: values.special_type || 'normal',
        raw_remark: values.raw_remark || null,
        notes: values.notes || null,
      };
      if (initial?.id) {
        await updatePerfEntry(initial.id, payload);
      } else {
        await createPerfEntry(payload);
      }
      notification.success({ message: t('dmpPerfEntrySaved') });
      onSave();
    } catch (err) {
      if (err?.errorFields) return; // validation error
      notification.error({ message: t('dmpPerfEntrySaveFailed'), description: err.message });
    } finally {
      setSaving(false);
    }
  };

  return (
    <Form
      form={form}
      size="small"
      layout="vertical"
      initialValues={{
        batch_id: initial?.batch_id || '',
        report_date: initial?.report_date ? dayjs(initial.report_date) : null,
        model: initial?.model || 'LR6',
        special_type: initial?.special_type || 'normal',
        raw_remark: initial?.raw_remark || '',
        notes: initial?.notes || '',
      }}
    >
      <Form.Item name="batch_id" label={t('dmpPerfEntryBatchId')} rules={[{ required: true }]}>
        <Input placeholder="e.g. 2026041814130402" />
      </Form.Item>
      <Form.Item name="report_date" label={t('dmpPerfEntryDate')} rules={[{ required: true }]}>
        <DatePicker style={{ width: '100%' }} />
      </Form.Item>
      <Form.Item name="model" label={t('dmpPerfEntryModel')} rules={[{ required: true }]}>
        <Select options={['LR6', 'LR03', 'LR61', '9V'].map((v) => ({ value: v, label: v }))} />
      </Form.Item>
      <Form.Item name="special_type" label={t('dmpPerfEntrySpecialType')}>
        <Select
          options={SPECIAL_TYPES.map((v) => ({
            value: v,
            label: t(`dmpPerfSpecial${v.charAt(0).toUpperCase() + v.slice(1)}`),
          }))}
        />
      </Form.Item>
      <Form.Item label={t('dmpPerfEntryGroups')}>
        <GroupEditor groups={groups} onChange={setGroups} />
      </Form.Item>
      <Form.Item name="raw_remark" label={t('dmpPerfEntryRemark')}>
        <Input placeholder="e.g. 150326 LR6 UD501 HP504" />
      </Form.Item>
      <Form.Item name="notes" label={t('dmpPerfEntryNotes')}>
        <Input.TextArea rows={2} />
      </Form.Item>
      <Space>
        <Button type="primary" icon={<SaveOutlined />} loading={saving} onClick={handleSave}>
          {t('dmpPerfSaveEntry')}
        </Button>
        <Button onClick={onCancel}>{t('dmpPerfCancelEntry')}</Button>
      </Space>
    </Form>
  );
}

// ─── Remark Registry Tab ──────────────────────────────────────────────────────

function RemarkRegistryTab({ stationId, selection }) {
  const { t } = useLang();
  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [editingId, setEditingId] = useState(null); // null = none, 'new' = new form
  const [dateRange, setDateRange] = useState([null, null]);

  const load = useCallback(async () => {
    if (!stationId) return;
    setLoading(true);
    try {
      const [from, to] = dateRange;
      const data = await fetchPerfEntries(stationId, {
        dateFrom: from ? from.format('YYYY-MM-DD') : undefined,
        dateTo: to ? to.format('YYYY-MM-DD') : undefined,
      });
      setEntries(data);
    } catch (err) {
      notification.error({ message: err.message });
    } finally {
      setLoading(false);
    }
  }, [stationId, dateRange]);

  useEffect(() => { load(); }, [load]);

  const handleDelete = async (id) => {
    try {
      await deletePerfEntry(id);
      notification.success({ message: t('dmpPerfEntryDeleted') });
      load();
    } catch (err) {
      notification.error({ message: t('dmpPerfEntryDeleteFailed'), description: err.message });
    }
  };

  const handleAddFromBatch = () => {
    if (!selection?.id) return;
    setEditingId('new');
  };

  const specialTag = (type) => {
    const colors = { '6020': 'gold', '3thang': 'blue', '6thang': 'purple', normal: 'default' };
    const labels = { '6020': '6020', '3thang': '3 THÁNG', '6thang': '6 THÁNG', normal: '-' };
    return <Tag color={colors[type] || 'default'}>{labels[type] || type}</Tag>;
  };

  const columns = [
    { title: '#', key: 'idx', width: 44, render: (_, __, i) => i + 1 },
    { title: t('dmpPerfEntryDate'), dataIndex: 'report_date', key: 'report_date', width: 110 },
    { title: t('dmpPerfEntryModel'), dataIndex: 'model', key: 'model', width: 80 },
    {
      title: t('dmpPerfEntryGroups'),
      key: 'groups',
      render: (_, row) => (row.groups || []).map((g, i) => (
        <Tag key={i}>{g.loai} {g.chuyen}</Tag>
      )),
    },
    {
      title: t('dmpPerfEntrySpecialType'),
      dataIndex: 'special_type',
      key: 'special_type',
      width: 120,
      render: (v) => specialTag(v),
    },
    {
      title: t('dmpPerfEntryBatchId'),
      dataIndex: 'batch_id',
      key: 'batch_id',
      ellipsis: true,
      width: 180,
    },
    {
      title: t('dmpPerfEntryRemark'),
      dataIndex: 'raw_remark',
      key: 'raw_remark',
      ellipsis: true,
    },
    {
      title: '',
      key: 'actions',
      width: 90,
      render: (_, row) => (
        <Space>
          <Button
            size="small"
            icon={<EditOutlined />}
            onClick={() => setEditingId(row.id)}
          />
          <Popconfirm
            title={t('dmpPerfDeleteConfirm')}
            onConfirm={() => handleDelete(row.id)}
            okText="OK"
            cancelText={t('dmpPerfCancelEntry')}
          >
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  if (editingId !== null) {
    const editing = editingId === 'new' ? null : entries.find((e) => e.id === editingId);
    const prefill = editingId === 'new' && selection
      ? { batch_id: selection.id || '', report_date: selection.fdrq || '' }
      : null;
    const initial = editing || prefill;
    return (
      <Card size="small" title={editingId === 'new' ? t('dmpPerfAddEntry') : t('dmpPerfEditEntry')}>
        <EntryForm
          initial={initial}
          stationId={stationId}
          onSave={() => { setEditingId(null); load(); }}
          onCancel={() => setEditingId(null)}
        />
      </Card>
    );
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Alert type="info" showIcon message={t('dmpPerfHint')} />
      <Space wrap>
        <DatePicker.RangePicker
          value={dateRange}
          onChange={setDateRange}
          allowClear
        />
        <Button onClick={load} loading={loading}>{t('dm2000Search')}</Button>
        <Button icon={<PlusOutlined />} onClick={() => setEditingId('new')}>
          {t('dmpPerfAddEntry')}
        </Button>
        {selection?.id && (
          <Button onClick={handleAddFromBatch}>
            {t('dmpPerfAddFromBatch')}
          </Button>
        )}
      </Space>

      {loading ? <Spin /> : entries.length === 0 ? (
        <Empty description={t('dmpPerfNoEntries')} />
      ) : (
        <Table
          size="small"
          rowKey="id"
          columns={columns}
          dataSource={entries}
          pagination={{ pageSize: 50, showSizeChanger: true }}
          scroll={{ x: 900 }}
        />
      )}
    </Space>
  );
}

// ─── Template Management Tab ──────────────────────────────────────────────────

function TemplateTab({ stationId }) {
  const { t } = useLang();
  const [templates, setTemplates] = useState([]);
  const [loading, setLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);

  const loadTemplates = useCallback(async () => {
    if (!stationId) return;
    setLoading(true);
    try {
      setTemplates(await fetchDmpPerfTemplates(stationId));
    } catch (err) {
      notification.error({ message: err.message });
    } finally {
      setLoading(false);
    }
  }, [stationId]);

  useEffect(() => { loadTemplates(); }, [loadTemplates]);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!stationId) { notification.warning({ message: t('dmpPerfSelectStation') }); return; }
    setUploading(true);
    try {
      await uploadDmpPerfTemplate(stationId, file);
      notification.success({ message: t('dmpPerfTemplateUploaded') });
      loadTemplates();
    } catch (err) {
      notification.error({ message: t('dmpPerfTemplateUploadFailed'), description: err.message });
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Space wrap>
        <Button
          icon={<UploadOutlined />}
          loading={uploading}
          onClick={() => fileInputRef.current?.click()}
        >
          {t('dmpPerfUploadTemplate')}
        </Button>
        <input
          ref={fileInputRef}
          type="file"
          accept=".xlsx"
          style={{ display: 'none' }}
          onChange={handleUpload}
        />
        <Button onClick={loadTemplates} loading={loading}>{t('dm2000Search')}</Button>
      </Space>

      {loading ? <Spin /> : templates.length === 0 ? (
        <Empty description={t('dmpPerfNoTemplates')} />
      ) : (
        <Table
          size="small"
          rowKey={(r) => r}
          dataSource={templates}
          pagination={false}
          columns={[
            { title: '#', key: 'idx', width: 44, render: (_, __, i) => i + 1 },
            { title: 'Template', dataIndex: null, render: (v) => v },
          ]}
        />
      )}
    </Space>
  );
}

// ─── Export Tab ───────────────────────────────────────────────────────────────

function ExportTab({ stationId }) {
  const { t } = useLang();
  const [templates, setTemplates] = useState([]);
  const [selectedTemplate, setSelectedTemplate] = useState(null);
  const [entries, setEntries] = useState([]);
  const [loadingTemplates, setLoadingTemplates] = useState(false);
  const [loadingEntries, setLoadingEntries] = useState(false);
  const [generating, setGenerating] = useState(false);

  useEffect(() => {
    if (!stationId) return;
    setLoadingTemplates(true);
    fetchDmpPerfTemplates(stationId)
      .then(setTemplates)
      .catch((err) => notification.error({ message: err.message }))
      .finally(() => setLoadingTemplates(false));

    setLoadingEntries(true);
    fetchPerfEntries(stationId)
      .then(setEntries)
      .catch((err) => notification.error({ message: err.message }))
      .finally(() => setLoadingEntries(false));
  }, [stationId]);

  const handleGenerate = async () => {
    if (!stationId) { notification.warning({ message: t('dmpPerfSelectStation') }); return; }
    if (!selectedTemplate) { notification.warning({ message: t('dmpPerfNoTemplate') }); return; }
    if (entries.length === 0) { notification.warning({ message: t('dmpPerfNoEntriesToExport') }); return; }

    setGenerating(true);
    try {
      await downloadDmpPerfReport({
        stationId,
        entries: entries.map((e) => ({
          batch_id: e.batch_id,
          model: e.model,
          groups: e.groups,
          special_type: e.special_type || 'normal',
        })),
        templateName: selectedTemplate,
      });
      notification.success({ message: t('dmpPerfReportDownloaded') });
    } catch (err) {
      notification.error({ message: t('dmpPerfReportFailed'), description: err.message });
    } finally {
      setGenerating(false);
    }
  };

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Card size="small">
        <Space direction="vertical" size={8} style={{ width: '100%' }}>
          <Typography.Text strong>{t('dmpPerfSelectTemplate')}:</Typography.Text>
          <Select
            style={{ width: 320 }}
            loading={loadingTemplates}
            placeholder={t('dmpPerfSelectTemplate')}
            value={selectedTemplate}
            onChange={setSelectedTemplate}
            options={(templates || []).map((n) => ({ value: n, label: n }))}
          />
        </Space>
      </Card>

      <Card size="small">
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Typography.Text>
            {loadingEntries ? <Spin size="small" /> : `${entries.length} entries registered`}
          </Typography.Text>
          <Button
            type="primary"
            icon={<DownloadOutlined />}
            loading={generating}
            disabled={!selectedTemplate || entries.length === 0}
            onClick={handleGenerate}
          >
            {generating ? t('dmpPerfGenerating') : t('dmpPerfGenerate')}
          </Button>
        </Space>
      </Card>
    </Space>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function DMPPerfReportTab() {
  const { t } = useLang();
  const [stations, setStations] = useState([]);
  const [stationId, setStationId] = useState(undefined);

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      try {
        const result = await fetchStations();
        if (!mounted) return;
        setStations(result || []);
        const online = (result || []).filter((s) => s.online);
        setStationId((prev) => prev ?? online[0]?.id);
      } catch (_) { /* ignore */ }
    };
    load();
    const id = setInterval(load, 30000);
    return () => { mounted = false; clearInterval(id); };
  }, []);

  const onlineStations = useMemo(() => stations.filter((s) => s.online), [stations]);
  const selectedStation = useMemo(
    () => stations.find((s) => s.id === stationId) || null,
    [stations, stationId],
  );

  return (
    <div style={{ background: '#fff', minHeight: 'calc(100vh - 112px)', padding: '0 16px' }}>
      <Typography.Title level={4}>{t('dmpPerfReportTab')}</Typography.Title>

      <Card size="small" style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <Typography.Text strong>{t('dmpStation')}:</Typography.Text>
          <Select
            style={{ minWidth: 280 }}
            placeholder={t('dmpSelectStation')}
            value={stationId}
            onChange={setStationId}
            options={onlineStations.map((s) => ({ value: s.id, label: s.name }))}
          />
          {selectedStation && (
            <Tag color={selectedStation.online ? 'green' : 'red'}>
              {selectedStation.name} • {selectedStation.online ? t('dmpOnline') : t('dmpOffline')}
            </Tag>
          )}
        </div>
        {onlineStations.length === 0 && (
          <Alert style={{ marginTop: 12 }} type="warning" showIcon message={t('dmpNoStations')} />
        )}
      </Card>

      <Tabs
        destroyInactiveTabPane
        items={[
          {
            key: 'remark',
            label: t('dmpPerfRemarkTab'),
            children: <RemarkRegistryTab stationId={stationId} selection={null} />,
          },
          {
            key: 'template',
            label: t('dmpPerfTemplateTab'),
            children: <TemplateTab stationId={stationId} />,
          },
          {
            key: 'export',
            label: t('dmpPerfExportTab'),
            children: <ExportTab stationId={stationId} />,
          },
        ]}
      />
    </div>
  );
}
