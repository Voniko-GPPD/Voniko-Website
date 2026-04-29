import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert, AutoComplete, Button, Drawer, Empty, Form, Input, Modal, Popconfirm, Select,
  Space, Spin, Switch, Table, Tabs, Tag, Tooltip, Typography, message,
} from 'antd';
import {
  CopyOutlined, DeleteOutlined, EditOutlined, PlusOutlined,
  QuestionCircleOutlined, ReloadOutlined, ThunderboltOutlined,
} from '@ant-design/icons';
import {
  FALLBACK_DISCHARGE_PRESETS,
  FALLBACK_FAMILY_KEYWORDS,
  SUFFIX_INFO,
  defaultFamilyLabel,
  formatPresetEntry,
} from '../constants/dischargeConditions';
import {
  createDischargePreset, updateDischargePreset, deleteDischargePreset,
  createFamilyKeyword, updateFamilyKeyword, deleteFamilyKeyword,
} from '../api/dischargeConditionsApi';
import useDischargeConditions from '../hooks/useDischargeConditions';
import { useLang } from '../contexts/LangContext';
import { useAuth } from '../contexts/AuthContext';

const SUFFIX_OPTIONS = [
  { label: '(none)', value: '' },
  { label: '(h) hours', value: 'h' },
  { label: '(m) minutes', value: 'm' },
  { label: '(t) times/pulses', value: 't' },
];

/**
 * Help drawer that lists discharge conditions per battery family
 * (LR6, LR03, LR61, 9V, ...) and lets the operator copy or apply a
 * specific condition to the current archive. Admins also see an "Edit"
 * mode that allows adding, modifying and deleting presets and family
 * keywords directly against the backend (so the customer can update the
 * standard list without a code change).
 */
export default function DischargeConditionHelp({
  batteryType,
  onApply,
  buttonProps = {},
}) {
  const { t } = useLang();
  const { isAdmin } = useAuth();
  const [open, setOpen] = useState(false);
  const [editMode, setEditMode] = useState(false);

  const {
    presetsGrouped, keywords, loading, error, fromServer, reload, detectFamily,
  } = useDischargeConditions();

  // Determine the active tab based on the detected family for the current
  // battery type, falling back to the first available group.
  const detectedFamily = useMemo(() => detectFamily(batteryType), [detectFamily, batteryType]);
  const groups = presetsGrouped.length > 0 ? presetsGrouped : FALLBACK_DISCHARGE_PRESETS;
  const defaultKey = useMemo(
    () => detectedFamily || groups[0]?.family || '',
    [detectedFamily, groups],
  );
  const [activeKey, setActiveKey] = useState(defaultKey);

  // Reset the active tab whenever the drawer is reopened or the underlying
  // data changes shape (e.g. admin added a new family).
  useEffect(() => {
    if (!open) setActiveKey(defaultKey);
  }, [defaultKey, open]);

  // Make sure activeKey stays valid when the groups list changes.
  useEffect(() => {
    if (groups.length === 0) return;
    if (!groups.some((g) => g.family === activeKey)) {
      setActiveKey(groups[0].family);
    }
  }, [groups, activeKey]);

  const handleCopy = async (text) => {
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      message.success(t('remarkCopied'));
    } catch (err) {
      message.error(t('remarkCopyFailed'));
    }
  };

  // ── Preset edit dialog state ───────────────────────────────────────────
  const [presetDialog, setPresetDialog] = useState(null); // {mode: 'create'|'edit', preset?}
  const [presetForm] = Form.useForm();

  const openPresetDialog = (mode, preset = null, family = activeKey) => {
    presetForm.resetFields();
    presetForm.setFieldsValue({
      family: preset?.family ?? family ?? '',
      condition_text: preset?.text ?? preset?.condition_text ?? '',
      suffix: preset?.suffix ?? '',
    });
    setPresetDialog({ mode, preset });
  };

  const submitPreset = async () => {
    try {
      const values = await presetForm.validateFields();
      if (presetDialog?.mode === 'create') {
        await createDischargePreset({
          family: values.family,
          conditionText: values.condition_text,
          suffix: values.suffix || '',
        });
        message.success(t('remarkPresetCreated'));
      } else {
        await updateDischargePreset(presetDialog.preset.id, {
          family: values.family,
          conditionText: values.condition_text,
          suffix: values.suffix || '',
        });
        message.success(t('remarkPresetUpdated'));
      }
      setPresetDialog(null);
      reload();
    } catch (err) {
      if (err?.errorFields) return; // validation
      message.error(err?.message || t('remarkSaveFailed'));
    }
  };

  const removePreset = async (preset) => {
    try {
      await deleteDischargePreset(preset.id);
      message.success(t('remarkPresetDeleted'));
      reload();
    } catch (err) {
      message.error(err?.message || t('remarkSaveFailed'));
    }
  };

  // ── Keyword edit dialog state ──────────────────────────────────────────
  const [keywordDialog, setKeywordDialog] = useState(null); // {mode, keyword?}
  const [keywordForm] = Form.useForm();

  const openKeywordDialog = (mode, kw = null) => {
    keywordForm.resetFields();
    keywordForm.setFieldsValue({
      keyword: kw?.keyword ?? '',
      family: kw?.family ?? '',
      sort_order: kw?.sort_order ?? kw?.sortOrder ?? 0,
    });
    setKeywordDialog({ mode, keyword: kw });
  };

  const submitKeyword = async () => {
    try {
      const values = await keywordForm.validateFields();
      const payload = {
        keyword: values.keyword,
        family: values.family,
        sortOrder: Number.isFinite(Number(values.sort_order)) ? Number(values.sort_order) : 0,
      };
      if (keywordDialog?.mode === 'create') {
        await createFamilyKeyword(payload);
        message.success(t('remarkKeywordCreated'));
      } else {
        await updateFamilyKeyword(keywordDialog.keyword.id, payload);
        message.success(t('remarkKeywordUpdated'));
      }
      setKeywordDialog(null);
      reload();
    } catch (err) {
      if (err?.errorFields) return;
      message.error(err?.message || t('remarkSaveFailed'));
    }
  };

  const removeKeyword = async (kw) => {
    try {
      await deleteFamilyKeyword(kw.id);
      message.success(t('remarkKeywordDeleted'));
      reload();
    } catch (err) {
      message.error(err?.message || t('remarkSaveFailed'));
    }
  };

  // ── Render helpers ─────────────────────────────────────────────────────
  const renderPresetTable = (preset) => {
    const data = preset.conditions.map((c, i) => ({
      key: c.id ?? `${preset.family}-${i}`,
      ...c,
      family: preset.family,
      formatted: formatPresetEntry(c),
    }));

    const columns = [
      { title: '#', key: 'idx', width: 48, render: (_v, _r, idx) => idx + 1 },
      {
        title: t('remarkConditionText'),
        dataIndex: 'formatted',
        key: 'formatted',
        render: (text) => (
          <Typography.Text style={{ fontFamily: 'monospace', fontSize: 12 }}>{text}</Typography.Text>
        ),
      },
      {
        title: t('remarkSuffixColumn'),
        dataIndex: 'suffix',
        key: 'suffix',
        width: 120,
        render: (sfx) => {
          if (!sfx) return <Tag>{t('remarkSuffixNone')}</Tag>;
          const info = SUFFIX_INFO[sfx];
          return (
            <Tooltip title={info ? t(info.unitKey) : ''}>
              <Tag color={sfx === 'h' ? 'blue' : sfx === 'm' ? 'green' : 'orange'}>
                ({sfx}) {info ? t(info.unitKey) : ''}
              </Tag>
            </Tooltip>
          );
        },
      },
      {
        title: t('remarkActions'),
        key: 'actions',
        width: editMode && isAdmin ? 220 : 180,
        render: (_v, record) => (
          <Space size={4}>
            <Tooltip title={t('remarkCopy')}>
              <Button size="small" icon={<CopyOutlined />} onClick={() => handleCopy(record.formatted)} />
            </Tooltip>
            {onApply && (
              <Tooltip title={t('remarkApply')}>
                <Button
                  size="small" type="primary" ghost icon={<ThunderboltOutlined />}
                  onClick={() => { onApply(record.formatted, record); message.success(t('remarkApplied')); }}
                >
                  {t('remarkApply')}
                </Button>
              </Tooltip>
            )}
            {editMode && isAdmin && record.id && (
              <>
                <Tooltip title={t('remarkEdit')}>
                  <Button size="small" icon={<EditOutlined />} onClick={() => openPresetDialog('edit', record)} />
                </Tooltip>
                <Popconfirm
                  title={t('remarkConfirmDelete')}
                  okText={t('remarkConfirmDeleteOk')}
                  cancelText={t('remarkConfirmDeleteCancel')}
                  onConfirm={() => removePreset(record)}
                >
                  <Button size="small" danger icon={<DeleteOutlined />} />
                </Popconfirm>
              </>
            )}
          </Space>
        ),
      },
    ];

    return (
      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        {editMode && isAdmin && (
          <Button
            size="small" type="dashed" icon={<PlusOutlined />}
            onClick={() => openPresetDialog('create', null, preset.family)}
          >
            {t('remarkAddPreset')}
          </Button>
        )}
        <Table
          size="small" rowKey="key" columns={columns} dataSource={data}
          pagination={false} scroll={{ y: 380 }}
        />
      </Space>
    );
  };

  const tabItems = groups.map((preset) => ({
    key: preset.family,
    label: preset.label || defaultFamilyLabel(preset.family),
    children: renderPresetTable(preset),
  }));

  // ── Keyword admin table ────────────────────────────────────────────────
  const keywordColumns = [
    { title: t('remarkKeyword'), dataIndex: 'keyword', key: 'keyword', width: 160 },
    { title: t('remarkFamily'), dataIndex: 'family', key: 'family', width: 120 },
    { title: t('remarkSortOrder'), dataIndex: 'sort_order', key: 'sort_order', width: 90, render: (v) => v ?? 0 },
    {
      title: t('remarkActions'),
      key: 'actions',
      width: 160,
      render: (_v, record) => (
        <Space size={4}>
          {record.id && (
            <>
              <Button size="small" icon={<EditOutlined />} onClick={() => openKeywordDialog('edit', record)} />
              <Popconfirm
                title={t('remarkConfirmDelete')}
                okText={t('remarkConfirmDeleteOk')}
                cancelText={t('remarkConfirmDeleteCancel')}
                onConfirm={() => removeKeyword(record)}
              >
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Popconfirm>
            </>
          )}
        </Space>
      ),
    },
  ];

  // Detect available family options for keyword form (existing families + manual entry).
  const familyOptions = useMemo(() => {
    const set = new Set(groups.map((g) => g.family).filter(Boolean));
    return [...set].map((f) => ({ value: f, label: defaultFamilyLabel(f) }));
  }, [groups]);

  return (
    <>
      <Tooltip title={t('remarkHelpButton')}>
        <Button
          size="small" icon={<QuestionCircleOutlined />}
          onClick={() => setOpen(true)} {...buttonProps}
        >
          {t('remarkHelpButton')}
        </Button>
      </Tooltip>
      <Drawer
        title={t('remarkHelpTitle')}
        placement="right" width={780} open={open} onClose={() => setOpen(false)}
        extra={(
          <Space>
            <Button size="small" icon={<ReloadOutlined />} onClick={reload} loading={loading}>
              {t('remarkReload')}
            </Button>
            {isAdmin && (
              <Space size={6}>
                <Typography.Text type="secondary">{t('remarkEditMode')}</Typography.Text>
                <Switch size="small" checked={editMode} onChange={setEditMode} />
              </Space>
            )}
          </Space>
        )}
      >
        <Spin spinning={loading && !fromServer}>
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
              {t('remarkHelpDesc')}
            </Typography.Paragraph>
            {error && !fromServer && (
              <Alert
                type="warning" showIcon
                message={t('remarkLoadFailedTitle')}
                description={`${error} — ${t('remarkLoadFailedDesc')}`}
              />
            )}
            <Space size={4} wrap>
              <Typography.Text strong>{t('remarkSuffixLegend')}:</Typography.Text>
              <Tag color="blue">(h) — {t('remarkSuffixHours')}</Tag>
              <Tag color="green">(m) — {t('remarkSuffixMinutes')}</Tag>
              <Tag color="orange">(t) — {t('remarkSuffixTimes')}</Tag>
            </Space>
            {detectedFamily && (
              <Typography.Paragraph style={{ marginBottom: 0 }}>
                <Typography.Text type="secondary">{t('remarkDetectedFamily')}:</Typography.Text>{' '}
                <Tag color="purple">{defaultFamilyLabel(detectedFamily)}</Tag>
              </Typography.Paragraph>
            )}

            {groups.length === 0 ? (
              <Empty description={t('remarkNoPresets')} />
            ) : (
              <Tabs
                activeKey={activeKey} onChange={setActiveKey}
                items={tabItems} size="small"
              />
            )}

            {editMode && isAdmin && (
              <>
                <Typography.Title level={5} style={{ marginTop: 16, marginBottom: 4 }}>
                  {t('remarkKeywordsSection')}
                </Typography.Title>
                <Typography.Paragraph type="secondary" style={{ marginBottom: 8 }}>
                  {t('remarkKeywordsHelp')}
                </Typography.Paragraph>
                <Button
                  size="small" type="dashed" icon={<PlusOutlined />}
                  style={{ marginBottom: 8 }} onClick={() => openKeywordDialog('create')}
                >
                  {t('remarkAddKeyword')}
                </Button>
                <Table
                  size="small"
                  rowKey={(r) => r.id || `${r.keyword}-${r.family}`}
                  columns={keywordColumns}
                  dataSource={fromServer ? keywords : FALLBACK_FAMILY_KEYWORDS}
                  pagination={false}
                />
              </>
            )}
          </Space>
        </Spin>
      </Drawer>

      {/* Preset create/edit dialog */}
      <Modal
        title={presetDialog?.mode === 'create' ? t('remarkAddPreset') : t('remarkEditPreset')}
        open={!!presetDialog}
        onOk={submitPreset}
        onCancel={() => setPresetDialog(null)}
        okText={t('remarkSave')}
        cancelText={t('remarkCancel')}
        destroyOnClose
      >
        <Form form={presetForm} layout="vertical" preserve={false}>
          <Form.Item
            label={t('remarkFamily')}
            name="family"
            rules={[{ required: true, message: t('remarkFamilyRequired') }]}
          >
            <Input placeholder="LR6 / LR03 / LR61 / 9V / ..." />
          </Form.Item>
          <Form.Item
            label={t('remarkConditionText')}
            name="condition_text"
            rules={[{ required: true, message: t('remarkConditionRequired') }]}
          >
            <Input placeholder="e.g. 10ohm 24h/d-0.9V" />
          </Form.Item>
          <Form.Item label={t('remarkSuffixColumn')} name="suffix" initialValue="">
            <Select options={SUFFIX_OPTIONS} />
          </Form.Item>
        </Form>
      </Modal>

      {/* Keyword create/edit dialog */}
      <Modal
        title={keywordDialog?.mode === 'create' ? t('remarkAddKeyword') : t('remarkEditKeyword')}
        open={!!keywordDialog}
        onOk={submitKeyword}
        onCancel={() => setKeywordDialog(null)}
        okText={t('remarkSave')}
        cancelText={t('remarkCancel')}
        destroyOnClose
      >
        <Form form={keywordForm} layout="vertical" preserve={false}>
          <Form.Item
            label={t('remarkKeyword')}
            name="keyword"
            rules={[{ required: true, message: t('remarkKeywordRequired') }]}
            extra={t('remarkKeywordHint')}
          >
            <Input placeholder="LR03 / 6F22 / ..." />
          </Form.Item>
          <Form.Item
            label={t('remarkFamily')}
            name="family"
            rules={[{ required: true, message: t('remarkFamilyRequired') }]}
          >
            <AutoComplete
              options={familyOptions.map((o) => ({ value: o.value, label: o.label }))}
              placeholder="LR6 / LR03 / LR61 / 9V / ..."
              filterOption={(input, option) => String(option?.value || '')
                .toLowerCase()
                .includes(String(input || '').toLowerCase())}
              allowClear
            />
          </Form.Item>
          <Form.Item
            label={t('remarkSortOrder')}
            name="sort_order"
            initialValue={0}
            extra={t('remarkSortOrderHint')}
          >
            <Input type="number" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
