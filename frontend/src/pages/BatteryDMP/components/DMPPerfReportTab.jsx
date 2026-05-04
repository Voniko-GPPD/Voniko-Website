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
  fetchBatches,
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

/** Matches exactly 6 digits — used to identify the DDMMYY date token in a remark. */
const SIX_DIGIT_RE = /^\d{6}$/;

/**
 * Parses a remark string such as "160226 LR6 UD501 UDP504" into:
 *   { date: Date|null, model: string|null, groups: [{loai, chuyen, trays}] }
 *
 * Rules:
 *   - First token that is exactly 6 digits → DDMMYY date (optional)
 *   - Token matching battery family (LR6, LR03, 9V, …) → model
 *   - "UDP<n>" → { loai: 'UD+', chuyen: '<n>', trays: [] }
 *   - "HP<n>"  → { loai: 'HP',  chuyen: '<n>', trays: [] }
 *   - "UD<n>"  → { loai: 'UD',  chuyen: '<n>', trays: [] }
 *
 * Trays are always left empty so the backend assigns them positionally:
 *   1 group → trays 1-9 | 2 groups → 1-4 / 6-9 | 3 groups → 1-3 / 4-6 / 7-9
 */
function parseRemark(raw) {
  if (!raw || !raw.trim()) return { date: null, model: null, groups: [] };

  const tokens = raw.trim().toUpperCase().split(/\s+/);
  let date = null;
  let model = null;
  const groups = [];
  let start = 0;

  // Optional 6-digit date prefix (DDMMYY)
  if (SIX_DIGIT_RE.test(tokens[0])) {
    const s = tokens[0];
    const day = parseInt(s.substring(0, 2), 10);
    const month = parseInt(s.substring(2, 4), 10);
    const year = 2000 + parseInt(s.substring(4, 6), 10);
    // Validate by round-tripping through Date to catch impossible dates (e.g. Feb 30)
    const candidate = new Date(year, month - 1, day);
    if (
      candidate.getFullYear() === year
      && candidate.getMonth() === month - 1
      && candidate.getDate() === day
    ) {
      date = candidate;
    }
    start = 1;
  }

  // LR\d{1,2} already covers LR6, LR03, LR61, etc.
  const batteryRe = /^(LR\d{1,2}|9V)$/;
  for (let i = start; i < tokens.length; i++) {
    const tok = tokens[i];
    if (batteryRe.test(tok)) {
      model = tok;
    } else if (/^UDP\d+$/.test(tok)) {
      // Trays are assigned positionally (not by type) — leave empty for auto-assignment
      groups.push({ loai: 'UD+', chuyen: tok.substring(3), trays: [] });
    } else if (/^HP\d+$/.test(tok)) {
      groups.push({ loai: 'HP', chuyen: tok.substring(2), trays: [] });
    } else if (/^UD\d+$/.test(tok)) {
      // Trays are assigned positionally (not by type) — leave empty for auto-assignment
      groups.push({ loai: 'UD', chuyen: tok.substring(2), trays: [] });
    }
  }

  return { date, model, groups };
}

// ─── Group editor ─────────────────────────────────────────────────────────────

function GroupEditor({ groups, onChange, model }) {
  const { t } = useLang();

  const handleGroupChange = (idx, field, value) => {
    const next = groups.map((g, i) => {
      if (i !== idx) return g;
      return { ...g, [field]: value };
    });
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
              {t('dmpPerfGroupTrays')}: {(grp.trays?.length ? grp.trays : autoTrays(groups.length, idx)).join(',')}
            </Typography.Text>
            {model && grp.chuyen && (
              <Tag color="blue">{t('dmpPerfSheetLabel')}: {model} {grp.chuyen}</Tag>
            )}
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
  const [groups, setGroups] = useState(initial?.groups || []);
  const [saving, setSaving] = useState(false);
  const [currentModel, setCurrentModel] = useState(initial?.model || 'LR6');

  // Batch selector state — fetches real para_pub records from the DMP station
  const [batches, setBatches] = useState([]);
  const [loadingBatches, setLoadingBatches] = useState(false);
  // explicitBatchId: the actual para_pub.id chosen from the dropdown (takes
  // priority over the DDMMYY prefix derived from raw_remark)
  const [explicitBatchId, setExplicitBatchId] = useState(
    initial?.batch_id != null ? String(initial.batch_id) : null,
  );

  useEffect(() => {
    if (!stationId) return;
    setLoadingBatches(true);
    fetchBatches(stationId)
      .then(setBatches)
      .catch((err) => notification.warning({ message: t('dmpPerfNoBatches'), description: err.message }))
      .finally(() => setLoadingBatches(false));
  }, [stationId]);

  const handleBatchSelect = (value) => {
    setExplicitBatchId(value != null ? String(value) : null);
    if (value == null) return;
    const batch = batches.find((b) => String(b.id) === String(value));
    if (!batch) return;
    // Auto-fill report_date from the batch's discharge date
    if (batch.fdrq) {
      form.setFieldValue('report_date_display', batch.fdrq);
    }
  };

  // Auto-parse remark on change and fill model + groups
  const handleRemarkChange = (e) => {
    const raw = e.target.value;
    const parsed = parseRemark(raw);
    if (parsed.model) {
      form.setFieldValue('model', parsed.model);
      setCurrentModel(parsed.model);
    }
    if (parsed.groups.length > 0) {
      setGroups(parsed.groups);
    }
  };

  const handleModelChange = (val) => {
    setCurrentModel(val);
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);

      // Prefer the explicitly selected batch ID (real para_pub.id).
      // Fall back to deriving batch_id from the DDMMYY prefix in raw_remark
      // for backward-compatibility with manually entered remarks.
      let batchId = explicitBatchId;
      let reportDate;

      if (batchId) {
        // Use the selected batch's discharge date as the report date if available
        const selectedBatch = batches.find((b) => String(b.id) === batchId);
        reportDate = selectedBatch?.fdrq || dayjs().format('YYYY-MM-DD');
      } else {
        // Legacy: derive from the DDMMYY remark prefix
        const parsed = parseRemark(values.raw_remark);
        reportDate = parsed.date
          ? dayjs(parsed.date).format('YYYY-MM-DD')
          : dayjs().format('YYYY-MM-DD');
        const remarkTokens = (values.raw_remark || '').trim().split(/\s+/);
        batchId = SIX_DIGIT_RE.test(remarkTokens[0])
          ? remarkTokens[0]
          : dayjs().format('DDMMYY');
      }

      const payload = {
        station_id: stationId,
        batch_id: batchId,
        report_date: reportDate,
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
        model: initial?.model || 'LR6',
        special_type: initial?.special_type || 'normal',
        raw_remark: initial?.raw_remark || '',
        notes: initial?.notes || '',
      }}
    >
      <Form.Item label={t('dmpPerfEntryBatchId')} required>
        <Select
          showSearch
          allowClear
          loading={loadingBatches}
          placeholder={t('dmpPerfSelectBatch')}
          value={explicitBatchId}
          onChange={handleBatchSelect}
          filterOption={(input, opt) =>
            (opt?.label ?? '').toLowerCase().includes(input.toLowerCase())
          }
          options={(batches || []).map((b) => {
            const parts = [String(b.id), b.fdrq, b.dcxh || b.remarks].filter(Boolean);
            return { value: String(b.id), label: parts.join(' – ') };
          })}
          style={{ width: '100%' }}
          notFoundContent={loadingBatches ? null : t('dmpPerfNoBatches')}
        />
      </Form.Item>
      <Form.Item name="raw_remark" label={t('dmpPerfEntryRemark')}>
        <Input
          placeholder="e.g. LR6 UD501 UDP504"
          onChange={handleRemarkChange}
        />
      </Form.Item>
      <Form.Item name="model" label={t('dmpPerfEntryModel')} rules={[{ required: true }]}>
        <Select
          options={['LR6', 'LR03', 'LR61', '9V'].map((v) => ({ value: v, label: v }))}
          onChange={handleModelChange}
        />
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
        <GroupEditor groups={groups} onChange={setGroups} model={currentModel} />
      </Form.Item>
      <Form.Item name="notes" label={t('dmpPerfEntryNotes')}>
        <Input.TextArea rows={2} />
      </Form.Item>
      <Space>
        <Button
          type="primary"
          icon={<SaveOutlined />}
          loading={saving}
          disabled={!explicitBatchId}
          onClick={handleSave}
        >
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
  const [newFromBatchId, setNewFromBatchId] = useState(null); // pre-fill batch for new form
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
    if (!selection?.batchId) return;
    setNewFromBatchId(String(selection.batchId));
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
    // For a new entry triggered from a selected batch, pre-populate batch_id
    const initial = editing || (newFromBatchId ? { batch_id: newFromBatchId } : null);
    return (
      <Card size="small" title={editingId === 'new' ? t('dmpPerfAddEntry') : t('dmpPerfEditEntry')}>
        <EntryForm
          initial={initial}
          stationId={stationId}
          onSave={() => { setEditingId(null); setNewFromBatchId(null); load(); }}
          onCancel={() => { setEditingId(null); setNewFromBatchId(null); }}
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
        {selection?.batchId && (
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
            {loadingEntries ? <Spin size="small" /> : t('dmpPerfEntriesCount', { count: entries.length })}
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
