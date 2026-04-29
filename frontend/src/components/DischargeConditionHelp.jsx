import React, { useMemo, useState } from 'react';
import {
  Button, Drawer, Space, Tabs, Table, Tag, Tooltip, Typography, message,
} from 'antd';
import { CopyOutlined, QuestionCircleOutlined, ThunderboltOutlined } from '@ant-design/icons';
import {
  BATTERY_DISCHARGE_PRESETS,
  SUFFIX_INFO,
  detectBatteryFamily,
  formatPresetEntry,
} from '../constants/dischargeConditions';
import { useLang } from '../contexts/LangContext';

/**
 * Help drawer that lists known discharge conditions per battery family
 * (LR6, LR03, LR61, 9V) and lets the operator copy or apply a specific
 * condition to the current archive.
 *
 * Props:
 *   - batteryType  current battery type (used to default the active tab)
 *   - onApply      called with the formatted condition string when a
 *                   row's "Apply" button is clicked. Optional; when omitted
 *                   only the Copy button is shown.
 *   - buttonProps  extra props forwarded to the trigger button.
 */
export default function DischargeConditionHelp({
  batteryType,
  onApply,
  buttonProps = {},
}) {
  const { t } = useLang();
  const [open, setOpen] = useState(false);

  const detected = useMemo(() => detectBatteryFamily(batteryType), [batteryType]);
  const defaultKey = detected?.family || BATTERY_DISCHARGE_PRESETS[0].family;
  const [activeKey, setActiveKey] = useState(defaultKey);

  // When the trigger battery type changes while the drawer is closed, reset
  // the active tab so the next open lands on the relevant family.
  React.useEffect(() => {
    if (!open) setActiveKey(defaultKey);
  }, [defaultKey, open]);

  const handleCopy = async (text) => {
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        // Fallback for non-secure contexts.
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

  const renderTable = (preset) => {
    const data = preset.conditions.map((c, i) => ({
      key: `${preset.family}-${i}`,
      ...c,
      formatted: formatPresetEntry(c),
    }));

    const columns = [
      {
        title: '#',
        key: 'idx',
        width: 48,
        render: (_v, _r, idx) => idx + 1,
      },
      {
        title: t('remarkConditionText'),
        dataIndex: 'formatted',
        key: 'formatted',
        render: (text) => (
          <Typography.Text style={{ fontFamily: 'monospace', fontSize: 12 }}>
            {text}
          </Typography.Text>
        ),
      },
      {
        title: t('remarkSuffixColumn'),
        dataIndex: 'suffix',
        key: 'suffix',
        width: 110,
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
        width: 180,
        render: (_v, record) => (
          <Space size={4}>
            <Tooltip title={t('remarkCopy')}>
              <Button
                size="small"
                icon={<CopyOutlined />}
                onClick={() => handleCopy(record.formatted)}
              />
            </Tooltip>
            {onApply && (
              <Tooltip title={t('remarkApply')}>
                <Button
                  size="small"
                  type="primary"
                  ghost
                  icon={<ThunderboltOutlined />}
                  onClick={() => {
                    onApply(record.formatted, record);
                    message.success(t('remarkApplied'));
                  }}
                >
                  {t('remarkApply')}
                </Button>
              </Tooltip>
            )}
          </Space>
        ),
      },
    ];

    return (
      <Table
        size="small"
        rowKey="key"
        columns={columns}
        dataSource={data}
        pagination={false}
        scroll={{ y: 420 }}
      />
    );
  };

  const tabItems = BATTERY_DISCHARGE_PRESETS.map((preset) => ({
    key: preset.family,
    label: preset.label,
    children: renderTable(preset),
  }));

  return (
    <>
      <Tooltip title={t('remarkHelpButton')}>
        <Button
          size="small"
          icon={<QuestionCircleOutlined />}
          onClick={() => setOpen(true)}
          {...buttonProps}
        >
          {t('remarkHelpButton')}
        </Button>
      </Tooltip>
      <Drawer
        title={t('remarkHelpTitle')}
        placement="right"
        width={720}
        open={open}
        onClose={() => setOpen(false)}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
            {t('remarkHelpDesc')}
          </Typography.Paragraph>
          <Space size={4} wrap>
            <Typography.Text strong>{t('remarkSuffixLegend')}:</Typography.Text>
            <Tag color="blue">(h) — {t('remarkSuffixHours')}</Tag>
            <Tag color="green">(m) — {t('remarkSuffixMinutes')}</Tag>
            <Tag color="orange">(t) — {t('remarkSuffixTimes')}</Tag>
          </Space>
          {detected && (
            <Typography.Paragraph style={{ marginBottom: 0 }}>
              <Typography.Text type="secondary">
                {t('remarkDetectedFamily')}:
              </Typography.Text>{' '}
              <Tag color="purple">{detected.label}</Tag>
            </Typography.Paragraph>
          )}
          <Tabs
            activeKey={activeKey}
            onChange={setActiveKey}
            items={tabItems}
            size="small"
          />
        </Space>
      </Drawer>
    </>
  );
}
