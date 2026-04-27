import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Empty, Input, Select, Space, Spin, Table, Typography } from 'antd';
import { fetchChannels, fetchTelemetry } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const SHOW_ALL_VALUE = 0;

function safeNum(value) {
  if (value === null || value === undefined || value === '--' || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export default function DMPHistoryTab({ stationId, selection }) {
  const { t } = useLang();
  const [channelLoading, setChannelLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [channels, setChannels] = useState([]);
  const [selectedBaty, setSelectedBaty] = useState(SHOW_ALL_VALUE);
  const [allRows, setAllRows] = useState([]);

  useEffect(() => {
    setSelectedBaty(SHOW_ALL_VALUE);
  }, [selection?.id]);

  // Load channels for the selected batch
  useEffect(() => {
    let active = true;
    setChannels([]);
    if (!stationId || !selection?.id) {
      setChannelLoading(false);
      return () => {};
    }
    const load = async () => {
      setChannelLoading(true);
      setError('');
      try {
        const result = await fetchChannels(stationId, selection.id);
        if (!active) return;
        setChannels(
          (result || []).filter(
            (ch) => ch.baty != null && Number.isFinite(Number(ch.baty)) && Number(ch.baty) > 0,
          ),
        );
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load channels');
      } finally {
        if (!active) return;
        setChannelLoading(false);
      }
    };
    load();
    return () => { active = false; };
  }, [stationId, selection?.id]);

  // Load telemetry based on selected channel (0 = all channels merged)
  useEffect(() => {
    let active = true;
    setAllRows([]);
    if (!stationId || !selection?.id || !channels.length) {
      setLoading(false);
      return () => {};
    }

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        let rows;
        if (selectedBaty === SHOW_ALL_VALUE) {
          // Fetch all channels and merge
          const results = await Promise.all(
            channels.map((ch) =>
              fetchTelemetry(stationId, ch.cdmc, ch.baty)
                .then((r) => (r || []).map((row) => ({ ...row, baty: ch.baty })))
                .catch(() => []),
            ),
          );
          rows = results.flat();
        } else {
          const ch = channels.find((c) => Number(c.baty) === selectedBaty);
          if (!ch) { rows = []; }
          else {
            const r = await fetchTelemetry(stationId, ch.cdmc, ch.baty);
            rows = (r || []).map((row) => ({ ...row, baty: ch.baty }));
          }
        }
        if (!active) return;
        setAllRows(rows);
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load telemetry');
      } finally {
        if (!active) return;
        setLoading(false);
      }
    };

    load();
    return () => { active = false; };
  }, [stationId, selection?.id, selectedBaty, channels]);

  const channelOptions = useMemo(() => {
    const sorted = [...channels].sort((a, b) => Number(a.baty) - Number(b.baty));
    return [
      { value: SHOW_ALL_VALUE, label: t('dmpAllChannels') },
      ...sorted.map((ch) => ({ value: Number(ch.baty), label: `CH ${ch.baty}` })),
    ];
  }, [channels, t]);

  const showBatyColumn = selectedBaty === SHOW_ALL_VALUE && channels.length > 0;

  const filteredRows = useMemo(() => {
    const keyword = search.trim().toLowerCase();
    const normalized = (allRows || []).map((row, index) => ({
      ...row,
      key: `${index}`,
      baty: row.baty,
      TIM: safeNum(row.TIM ?? row.tim),
      VOLT: safeNum(row.VOLT ?? row.volt ?? row.Volt),
      Im: safeNum(row.Im ?? row.IM ?? row.im),
    })).filter((row) => row.TIM !== null && row.VOLT !== null);

    if (!keyword) return normalized;
    return normalized.filter((row) =>
      String(row.TIM ?? '').toLowerCase().includes(keyword)
      || String(row.VOLT ?? '').toLowerCase().includes(keyword)
      || String(row.Im ?? '').toLowerCase().includes(keyword)
      || (row.baty != null && String(row.baty).toLowerCase().includes(keyword)),
    );
  }, [allRows, search]);

  const columns = useMemo(() => [
    { title: '#', key: 'idx', width: 60, render: (_value, _record, index) => index + 1 },
    ...(showBatyColumn ? [{
      title: t('dmpChannel'),
      dataIndex: 'baty',
      key: 'baty',
      width: 100,
      sorter: (a, b) => (Number(a.baty) || 0) - (Number(b.baty) || 0),
      render: (value) => (value != null ? `CH ${value}` : '-'),
    }] : []),
    {
      title: t('dmpTimeH'),
      dataIndex: 'TIM',
      key: 'TIM',
      sorter: (a, b) => Number(a.TIM) - Number(b.TIM),
      defaultSortOrder: showBatyColumn ? undefined : 'ascend',
      render: (value) => `${Number(value).toFixed(4)} h`,
    },
    {
      title: t('dmpVoltage'),
      dataIndex: 'VOLT',
      key: 'VOLT',
      sorter: (a, b) => Number(a.VOLT) - Number(b.VOLT),
      render: (value) => Number(value).toFixed(4),
    },
    {
      title: t('dmpCurrent'),
      dataIndex: 'Im',
      key: 'Im',
      sorter: (a, b) => Number(a.Im ?? 0) - Number(b.Im ?? 0),
      render: (value) => (value != null ? Number(value).toFixed(4) : '-'),
    },
  ], [showBatyColumn, t]);

  if (!selection) {
    return <Empty description={t('dmpSelectBatchToView')} />;
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Space wrap>
        <Select
          style={{ width: 220 }}
          value={selectedBaty}
          options={channelOptions}
          onChange={setSelectedBaty}
          loading={channelLoading}
        />
        <Input.Search
          allowClear
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder={t('dmpSearchTelemetry')}
          style={{ width: 260 }}
        />
      </Space>

      {loading ? (
        <Spin />
      ) : filteredRows.length === 0 ? (
        <Empty description={t('dmpNoData')} />
      ) : (
        <Table
          size="small"
          columns={columns}
          dataSource={filteredRows}
          pagination={{ pageSize: 100 }}
          scroll={{ y: 500 }}
          summary={() => (
            <Table.Summary.Row>
              <Table.Summary.Cell colSpan={columns.length} index={0}>
                <Typography.Text type="secondary">{t('dmpTotalRows', { count: filteredRows.length })}</Typography.Text>
              </Table.Summary.Cell>
            </Table.Summary.Row>
          )}
        />
      )}
    </Space>
  );
}
