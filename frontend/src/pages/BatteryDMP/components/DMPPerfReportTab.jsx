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
  Tooltip,
  Typography,
  notification,
} from 'antd';
import {
  DeleteOutlined,
  DownloadOutlined,
  EditOutlined,
  EyeOutlined,
  PlusOutlined,
  SaveOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import {
  createPerfEntry,
  deletePerfEntry,
  downloadDmpPerfReport,
  fetchDmpPerfData,
  fetchDmpPerfTemplates,
  fetchPerfEntries,
  fetchStations,
  updatePerfEntry,
  uploadDmpPerfTemplate,
} from '../../../api/dmpApi';
import { getBatteryTypes } from '../../../api/battery';
import { useLang } from '../../../contexts/LangContext';

const SPECIAL_TYPES = ['normal', '6020', '3thang', '6thang'];
const LOAI_OPTIONS = ['UD', 'UD+', 'HP'].map((v) => ({ value: v, label: v }));

/** Derive model/loai/chuyen filter option lists and a filtered subset from entries. */
function useEntryFilters(entries, filterModel, filterLoai, filterChuyen) {
  const modelOptions = useMemo(() => {
    const models = [...new Set((entries || []).map((e) => e.model).filter(Boolean))].sort();
    return models.map((m) => ({ value: m, label: m }));
  }, [entries]);

  const loaiOptions = useMemo(() => {
    const loais = [...new Set(
      (entries || []).flatMap((e) => (e.groups || []).map((g) => g.loai)).filter(Boolean),
    )].sort();
    return loais.map((l) => ({ value: l, label: l }));
  }, [entries]);

  const chuyenOptions = useMemo(() => {
    const chuyens = [...new Set(
      (entries || []).flatMap((e) => (e.groups || []).map((g) => g.chuyen)).filter(Boolean),
    )].sort();
    return chuyens.map((c) => ({ value: c, label: c }));
  }, [entries]);

  const filteredEntries = useMemo(() => {
    let result = entries;
    if (filterModel) result = result.filter((e) => e.model === filterModel);
    if (filterLoai || filterChuyen) {
      result = result
        .map((e) => ({
          ...e,
          groups: (e.groups || []).filter((g) => {
            if (filterLoai && g.loai !== filterLoai) return false;
            if (filterChuyen && g.chuyen !== filterChuyen) return false;
            return true;
          }),
        }))
        .filter((e) => e.groups.length > 0);
    }
    return result;
  }, [entries, filterModel, filterLoai, filterChuyen]);

  return { modelOptions, loaiOptions, chuyenOptions, filteredEntries };
}

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
 *   - Token matching a battery family name (from batteryTypes list, or LR\d{1,2}/9V as fallback) → model
 *   - "UDP<n>" → { loai: 'UD+', chuyen: '<n>', trays: [] }
 *   - "HP<n>"  → { loai: 'HP',  chuyen: '<n>', trays: [] }
 *   - "UD<n>"  → { loai: 'UD',  chuyen: '<n>', trays: [] }
 *
 * Trays are always left empty so the backend assigns them positionally:
 *   1 group → trays 1-9 | 2 groups → 1-4 / 6-9 | 3 groups → 1-3 / 4-6 / 7-9
 *
 * @param {string} raw - raw remark string to parse
 * @param {string[]} batteryTypes - list of battery family names from admin settings
 *   (e.g. ['LR6', 'LR03', 'LR61', '9V']). Falls back to /^(LR\d{1,2}|9V)$/ when empty.
 */
function parseRemark(raw, batteryTypes) {
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

  // Build battery family matcher from the admin-configured list.
  // Falls back to the built-in pattern when no types are loaded yet.
  const batterySet = batteryTypes && batteryTypes.length > 0
    ? new Set(batteryTypes.map((t) => t.trim().toUpperCase()))
    : null;
  const isBatteryModel = batterySet
    ? (tok) => batterySet.has(tok)
    : (tok) => /^(LR\d{1,2}|9V)$/.test(tok);

  for (let i = start; i < tokens.length; i++) {
    const tok = tokens[i];
    if (isBatteryModel(tok)) {
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
  const [batteryTypes, setBatteryTypes] = useState([]);

  // Load admin-configured battery family names once.
  // GET /api/battery/types returns { types: [{id, name, created_at}, ...] }
  useEffect(() => {
    getBatteryTypes()
      .then((res) => {
        const names = (res.data?.types || []).map((bt) => bt.name).filter(Boolean);
        if (names.length > 0) setBatteryTypes(names);
      })
      .catch((err) => {
        // Non-critical: fall back to built-in defaults so the form remains usable
        console.warn('DMPPerfReportTab: failed to load battery types', err?.message);
      });
  }, []);

  const modelOptions = useMemo(
    () => (batteryTypes.length > 0 ? batteryTypes : ['LR6', 'LR03', 'LR61', '9V'])
      .map((v) => ({ value: v, label: v })),
    [batteryTypes],
  );

  // Auto-parse remark to fill model + groups
  const handleRemarkChange = (e) => {
    const raw = e.target.value;
    const parsed = parseRemark(raw, batteryTypes);
    if (parsed.model) {
      form.setFieldValue('model', parsed.model);
      setCurrentModel(parsed.model);
    }
    if (parsed.groups.length > 0) {
      setGroups(parsed.groups);
    }
  };

  const handleModelChange = (val) => { setCurrentModel(val); };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);

      const rawRemark = values.raw_remark || null;
      const parsed = parseRemark(values.raw_remark, batteryTypes);
      const reportDate = parsed.date
        ? dayjs(parsed.date).format('YYYY-MM-DD')
        : dayjs().format('YYYY-MM-DD');
      const remarkTokens = (values.raw_remark || '').trim().split(/\s+/);
      const batchId = SIX_DIGIT_RE.test(remarkTokens[0])
        ? remarkTokens[0]
        : dayjs().format('DDMMYY');

      const payload = {
        station_id: stationId,
        batch_id: batchId,
        report_date: reportDate,
        model: values.model,
        groups,
        special_type: values.special_type || 'normal',
        raw_remark: rawRemark,
        notes: values.notes || null,
        dm2000_archname: null,
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
        // Show raw_remark, falling back to dm2000_archname for legacy DM2000 entries
        raw_remark: initial?.raw_remark || initial?.dm2000_archname || '',
        notes: initial?.notes || '',
      }}
    >
      {/* ── Remark ── */}
      <Form.Item name="raw_remark" label={t('dmpPerfEntryRemark')} rules={[{ required: true }]}>
        <Input
          placeholder="e.g. 160226 LR6 UD501 UDP504 or LR6 UDP501 HP503"
          onChange={handleRemarkChange}
        />
      </Form.Item>

      {/* ── Common fields ── */}
      <Form.Item name="model" label={t('dmpPerfEntryModel')} rules={[{ required: true }]}>
        <Select
          options={modelOptions}
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
      title: t('dmpPerfEntryRemark'),
      key: 'remark',
      ellipsis: true,
      render: (_, row) => row.raw_remark || row.dm2000_archname || '-',
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
    const initial = editing || null;
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

// ─── Performance View Tab ─────────────────────────────────────────────────────

/** Special row labels that should be highlighted (non-date rows). */
const SPECIAL_ROW_LABELS = new Set(['6020', '3 THÁNG', '6 THÁNG']);

/** Return a coloured tag for special row labels or plain text for dates. */
function RowLabelCell({ label }) {
  if (label === '6020') return <Tag color="gold" style={{ fontWeight: 600 }}>{label}</Tag>;
  if (label === '3 THÁNG') return <Tag color="blue" style={{ fontWeight: 600 }}>{label}</Tag>;
  if (label === '6 THÁNG') return <Tag color="purple" style={{ fontWeight: 600 }}>{label}</Tag>;
  return <span>{label}</span>;
}

/** Format a numeric performance result for display. */
function fmtResult(cond, unit) {
  if (!cond) return '-';
  if (unit === 'times') {
    return cond.avg_count != null ? String(cond.avg_count) : '-';
  }
  if (unit === 'minute') {
    return cond.avg_minutes != null ? `${Number(cond.avg_minutes).toFixed(1)}` : '-';
  }
  return cond.avg_hours != null ? `${Number(cond.avg_hours).toFixed(2)}` : '-';
}

function PerfViewTab({ stationId }) {
  const { t } = useLang();
  const [entries, setEntries] = useState([]);
  const [loadingEntries, setLoadingEntries] = useState(false);
  const [loadingData, setLoadingData] = useState(false);
  const [dateRange, setDateRange] = useState([null, null]);
  const [filterModel, setFilterModel] = useState(null);
  const [filterLoai, setFilterLoai] = useState(null);
  const [filterChuyen, setFilterChuyen] = useState(null);
  const [sheetsData, setSheetsData] = useState(null); // { [sheetKey]: {rows, conditions} }
  const [activeSheet, setActiveSheet] = useState(null);

  // Load entries from SQLite (used on mount and when date range changes)
  const loadEntries = useCallback(async () => {
    if (!stationId) return;
    setLoadingEntries(true);
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
      setLoadingEntries(false);
    }
  }, [stationId, dateRange]);

  useEffect(() => { loadEntries(); }, [loadEntries]);

  const { modelOptions, loaiOptions, chuyenOptions, filteredEntries } = useEntryFilters(
    entries, filterModel, filterLoai, filterChuyen,
  );

  /** Apply the same client-side filters as useEntryFilters to a raw list. */
  const applyFilters = useCallback((data) => {
    let result = data;
    if (filterModel) result = result.filter((e) => e.model === filterModel);
    if (filterLoai || filterChuyen) {
      result = result
        .map((e) => ({
          ...e,
          groups: (e.groups || []).filter((g) => {
            if (filterLoai && g.loai !== filterLoai) return false;
            if (filterChuyen && g.chuyen !== filterChuyen) return false;
            return true;
          }),
        }))
        .filter((e) => e.groups.length > 0);
    }
    return result;
  }, [filterLoai, filterChuyen, filterModel]);

  /**
   * "Xem trước": fetch entries from the server with the current date range,
   * apply client-side filters, then immediately query DMP performance data
   * so the table updates in one click.
   */
  const handleSearch = useCallback(async () => {
    if (!stationId) { notification.warning({ message: t('dmpPerfSelectStation') }); return; }
    setLoadingEntries(true);
    setSheetsData(null);
    let freshEntries = [];
    try {
      const [from, to] = dateRange;
      freshEntries = await fetchPerfEntries(stationId, {
        dateFrom: from ? from.format('YYYY-MM-DD') : undefined,
        dateTo: to ? to.format('YYYY-MM-DD') : undefined,
      });
      setEntries(freshEntries);
    } catch (err) {
      notification.error({ message: err.message });
      setLoadingEntries(false);
      return;
    }
    setLoadingEntries(false);

    const toPreview = applyFilters(freshEntries);
    if (toPreview.length === 0) { notification.warning({ message: t('dmpPerfNoEntries') }); return; }
    setLoadingData(true);
    try {
      const payload = toPreview.map((e) => ({
        batch_id: e.batch_id,
        report_date: e.report_date || null,
        model: e.model,
        groups: e.groups,
        special_type: e.special_type || 'normal',
        raw_remark: e.raw_remark || null,
        dm2000_archname: e.dm2000_archname || null,
      }));
      const data = await fetchDmpPerfData({ stationId, entries: payload });
      setSheetsData(data.sheets || {});
      const sheetKeys = Object.keys(data.sheets || {});
      setActiveSheet(sheetKeys[0] || null);
    } catch (err) {
      notification.error({ message: t('dmpPerfViewTab'), description: err.message });
    } finally {
      setLoadingData(false);
    }
  }, [stationId, dateRange, applyFilters, t]);

  // Build Ant Design table columns for a given sheet
  const buildColumns = useCallback((sheetKey) => {
    if (!sheetsData || !sheetsData[sheetKey]) return [];
    const { conditions, units: sheetUnits = {} } = sheetsData[sheetKey];
    const cols = [
      {
        title: t('dmpPerfViewDate'),
        dataIndex: 'date',
        key: 'date',
        width: 120,
        fixed: 'left',
        render: (v) => <RowLabelCell label={v} />,
        onCell: (row) => ({
          style: SPECIAL_ROW_LABELS.has(row.date)
            ? { background: '#fffbe6', fontWeight: 600 }
            : {},
        }),
      },
      {
        title: t('dmpPerfViewLoaiCol'),
        dataIndex: 'loai',
        key: 'loai',
        width: 80,
        fixed: 'left',
        render: (v) => <Tag>{v}</Tag>,
      },
    ];

    (conditions || []).forEach((cond) => {
      // Prefer unit from backend hfsj field; fall back to text inference from label.
      const backendUnit = sheetUnits[cond]; // "hour" | "minute" | "times" | undefined
      let unit;
      if (backendUnit === 'times') {
        unit = 'times';
      } else if (backendUnit === 'minute') {
        unit = 'minute';
      } else if (backendUnit === 'hour') {
        unit = 'hour';
      } else {
        // Fallback: infer from label text
        const lc = cond.toLowerCase();
        if (lc.endsWith('(t)') || /\d+t\/h/.test(lc)) unit = 'times';
        else if (lc.endsWith('(m)')) unit = 'minute';
        else unit = 'hour';
      }
      const unitLabel = unit === 'times' ? t('dmpPerfViewCount')
        : unit === 'minute' ? t('dmpPerfViewMinutes')
        : t('dmpPerfViewHours');

      cols.push({
        title: (
          <Tooltip title={cond}>
            <span style={{ fontSize: 11 }}>{cond}</span>
          </Tooltip>
        ),
        key: cond,
        children: [
          {
            title: `${t('dmpPerfViewResult')} (${unitLabel})`,
            key: `${cond}_result`,
            width: 110,
            align: 'right',
            render: (_, row) => {
              const val = fmtResult(row.conditions?.[cond], unit);
              return <Typography.Text strong={val !== '-'}>{val}</Typography.Text>;
            },
          },
          {
            title: t('dmpPerfViewRate'),
            key: `${cond}_rate`,
            width: 90,
            align: 'right',
            render: (_, row) => {
              const ur = row.conditions?.[cond]?.uniform_rate;
              if (ur == null) return '-';
              const color = ur >= 95 ? '#52c41a' : ur >= 85 ? '#faad14' : '#ff4d4f';
              return <span style={{ color, fontWeight: 500 }}>{`${Number(ur).toFixed(1)}%`}</span>;
            },
          },
        ],
      });
    });

    return cols;
  }, [sheetsData, t]);

  const sheetKeys = Object.keys(sheetsData || {});

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Card
        size="small"
        style={{ borderRadius: 8 }}
        bodyStyle={{ padding: '12px 16px' }}
      >
        <Space wrap size={8}>
          <DatePicker.RangePicker
            value={dateRange}
            onChange={setDateRange}
            allowClear
          />
          <Select
            allowClear
            style={{ minWidth: 120 }}
            placeholder={t('dmpPerfViewFilterModel')}
            value={filterModel}
            onChange={setFilterModel}
            options={modelOptions}
          />
          <Select
            allowClear
            style={{ minWidth: 130 }}
            placeholder={t('dmpPerfViewFilterLoai')}
            value={filterLoai}
            onChange={setFilterLoai}
            options={loaiOptions}
          />
          <Select
            allowClear
            style={{ minWidth: 120 }}
            placeholder={t('dmpPerfViewFilterChuyen')}
            value={filterChuyen}
            onChange={setFilterChuyen}
            options={chuyenOptions}
          />
          <Button
            type="primary"
            icon={<EyeOutlined />}
            loading={loadingEntries || loadingData}
            onClick={handleSearch}
          >
            {t('dmpPerfViewPreview')}
          </Button>
        </Space>
      </Card>

      {(loadingEntries || loadingData) && (
        <Card size="small" style={{ borderRadius: 8, textAlign: 'center' }}>
          <Space><Spin /><Typography.Text type="secondary">{t('dmpPerfViewLoading')}</Typography.Text></Space>
        </Card>
      )}

      {!loadingEntries && !loadingData && sheetsData !== null && sheetKeys.length === 0 && (
        <Empty description={t('dmpPerfViewNoData')} />
      )}

      {!loadingEntries && !loadingData && sheetKeys.length > 0 && (
        <Card
          size="small"
          style={{ borderRadius: 8 }}
          bodyStyle={{ padding: 0 }}
          title={(
            <Tabs
              size="small"
              activeKey={activeSheet}
              onChange={setActiveSheet}
              items={sheetKeys.map((k) => ({ key: k, label: <Typography.Text strong>{k}</Typography.Text> }))}
              style={{ marginBottom: 0 }}
            />
          )}
        >
          {activeSheet && sheetsData[activeSheet] && (
            <Table
              size="small"
              rowKey={(row, i) => `${row.date}-${row.loai}-${i}`}
              columns={buildColumns(activeSheet)}
              dataSource={sheetsData[activeSheet].rows || []}
              pagination={false}
              scroll={{ x: 'max-content' }}
              bordered
              rowClassName={(row) => SPECIAL_ROW_LABELS.has(row.date) ? 'perf-special-row' : ''}
              style={{ borderRadius: 0 }}
            />
          )}
        </Card>
      )}

      {!loadingEntries && !loadingData && sheetsData === null && (
        <Card size="small" style={{ borderRadius: 8, textAlign: 'center' }}>
          <Empty description={t('dmpPerfViewNoData')} />
        </Card>
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
  const [dateRange, setDateRange] = useState([null, null]);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [filterModel, setFilterModel] = useState(null);
  const [filterLoai, setFilterLoai] = useState(null);
  const [filterChuyen, setFilterChuyen] = useState(null);

  const loadEntries = useCallback(async () => {
    if (!stationId) return;
    setLoadingEntries(true);
    try {
      const [from, to] = dateRange;
      const data = await fetchPerfEntries(stationId, {
        dateFrom: from ? from.format('YYYY-MM-DD') : undefined,
        dateTo: to ? to.format('YYYY-MM-DD') : undefined,
      });
      setEntries(data);
      setSelectedRowKeys(data.map((e) => e.id));
    } catch (err) {
      notification.error({ message: err.message });
    } finally {
      setLoadingEntries(false);
    }
  }, [stationId, dateRange]);

  useEffect(() => {
    if (!stationId) return;
    setLoadingTemplates(true);
    fetchDmpPerfTemplates(stationId)
      .then(setTemplates)
      .catch((err) => notification.error({ message: err.message }))
      .finally(() => setLoadingTemplates(false));
  }, [stationId]);

  useEffect(() => { loadEntries(); }, [loadEntries]);

  const { modelOptions, loaiOptions, chuyenOptions, filteredEntries } = useEntryFilters(
    entries, filterModel, filterLoai, filterChuyen,
  );

  const specialTag = (type) => {
    const colors = { '6020': 'gold', '3thang': 'blue', '6thang': 'purple', normal: 'default' };
    const labelKeys = { '6020': 'dmpPerfSpecial6020', '3thang': 'dmpPerfSpecial3thang', '6thang': 'dmpPerfSpecial6thang', normal: 'dmpPerfSpecialNormal' };
    const label = labelKeys[type] ? t(labelKeys[type]) : type;
    return <Tag color={colors[type] || 'default'}>{label}</Tag>;
  };

  const handleGenerate = async () => {
    if (!stationId) { notification.warning({ message: t('dmpPerfSelectStation') }); return; }
    if (!selectedTemplate) { notification.warning({ message: t('dmpPerfNoTemplate') }); return; }
    const selected = filteredEntries.filter((e) => selectedRowKeys.includes(e.id));
    const toExport = selected.length > 0 ? selected : filteredEntries;
    if (toExport.length === 0) { notification.warning({ message: t('dmpPerfNoEntriesToExport') }); return; }

    setGenerating(true);
    try {
      await downloadDmpPerfReport({
        stationId,
        entries: toExport.map((e) => ({
          batch_id: e.batch_id,
          report_date: e.report_date || null,
          model: e.model,
          groups: e.groups,
          special_type: e.special_type || 'normal',
          raw_remark: e.raw_remark || null,
          dm2000_archname: e.dm2000_archname || null,
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

  const exportCount = selectedRowKeys.length > 0
    ? filteredEntries.filter((e) => selectedRowKeys.includes(e.id)).length
    : filteredEntries.length;

  const columns = [
    { title: '#', key: 'idx', width: 44, render: (_, __, i) => i + 1 },
    { title: t('dmpPerfEntryDate'), dataIndex: 'report_date', key: 'report_date', width: 110 },
    { title: t('dmpPerfEntryModel'), dataIndex: 'model', key: 'model', width: 80 },
    {
      title: t('dmpPerfEntryGroups'),
      key: 'groups',
      render: (_, row) => (row.groups || []).map((g, i) => (
        <Tag key={`${g.loai}-${g.chuyen}-${i}`}>{g.loai} {g.chuyen}</Tag>
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
  ];

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
        <Space direction="vertical" size={8} style={{ width: '100%' }}>
          <Space wrap>
            <DatePicker.RangePicker
              value={dateRange}
              onChange={setDateRange}
              allowClear
            />
            <Select
              allowClear
              style={{ minWidth: 110 }}
              placeholder={t('dmpPerfViewFilterModel')}
              value={filterModel}
              onChange={setFilterModel}
              options={modelOptions}
            />
            <Select
              allowClear
              style={{ minWidth: 110 }}
              placeholder={t('dmpPerfViewFilterLoai')}
              value={filterLoai}
              onChange={setFilterLoai}
              options={loaiOptions}
            />
            <Select
              allowClear
              style={{ minWidth: 120 }}
              placeholder={t('dmpPerfViewFilterChuyen')}
              value={filterChuyen}
              onChange={setFilterChuyen}
              options={chuyenOptions}
            />
            <Button onClick={loadEntries} loading={loadingEntries}>{t('dm2000Search')}</Button>
            <Button
              size="small"
              onClick={() => setSelectedRowKeys(filteredEntries.map((e) => e.id))}
              disabled={filteredEntries.length === 0}
            >
              {t('dmpPerfExportSelectAll')}
            </Button>
            <Button
              size="small"
              onClick={() => setSelectedRowKeys([])}
              disabled={selectedRowKeys.length === 0}
            >
              {t('dmpPerfExportDeselectAll')}
            </Button>
          </Space>

          {loadingEntries ? <Spin /> : filteredEntries.length === 0 ? (
            <Empty description={t('dmpPerfNoEntries')} />
          ) : (
            <Table
              size="small"
              rowKey="id"
              columns={columns}
              dataSource={filteredEntries}
              pagination={{ pageSize: 50, showSizeChanger: true }}
              scroll={{ x: 700 }}
              rowSelection={{
                selectedRowKeys,
                onChange: setSelectedRowKeys,
              }}
            />
          )}
        </Space>
      </Card>

      <Card size="small">
        <Space size={12} align="center">
          <Button
            type="primary"
            icon={<DownloadOutlined />}
            loading={generating}
            disabled={!selectedTemplate || filteredEntries.length === 0}
            onClick={handleGenerate}
          >
            {generating ? t('dmpPerfGenerating') : t('dmpPerfGenerate')}
          </Button>
          {!loadingEntries && filteredEntries.length > 0 && (
            <Typography.Text type="secondary">
              {t('dmpPerfExportSelectedCount', { selected: exportCount, total: filteredEntries.length })}
            </Typography.Text>
          )}
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
            key: 'view',
            label: t('dmpPerfViewTab'),
            children: <PerfViewTab stationId={stationId} />,
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
