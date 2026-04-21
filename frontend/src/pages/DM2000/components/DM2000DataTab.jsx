import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Empty, Input, Select, Space, Spin, Table, Typography } from 'antd';
import { fetchDM2000AverageCurve, fetchDM2000Batteries, fetchDM2000Curve } from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export default function DM2000DataTab({ stationId, selection, selectedBaty, onBatyChange }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [batteryLoading, setBatteryLoading] = useState(false);
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [rows, setRows] = useState([]);
  const [batteries, setBatteries] = useState([]);

  useEffect(() => {
    let active = true;
    setBatteries([]);
    if (!stationId || !selection?.archname) {
      setBatteryLoading(false);
      return () => {};
    }

    const load = async () => {
      setBatteryLoading(true);
      try {
        const result = await fetchDM2000Batteries(stationId, selection.archname);
        if (!active) return;
        setBatteries(
          (result || [])
            .map((row) => Number(row?.baty ?? row?.BATY ?? row?.Baty ?? row))
            .filter((value) => Number.isFinite(value) && value > 0),
        );
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load batteries');
      } finally {
        if (!active) return;
        setBatteryLoading(false);
      }
    };

    load();
    return () => {
      active = false;
    };
  }, [stationId, selection?.archname]);

  useEffect(() => {
    let active = true;
    setRows([]);
    if (!stationId || !selection?.archname) {
      setLoading(false);
      return () => {};
    }

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const result = selectedBaty > 0
          ? await fetchDM2000Curve(stationId, selection.archname, selectedBaty)
          : await fetchDM2000AverageCurve(stationId, selection.archname);
        if (!active) return;
        setRows(result || []);
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load table data');
      } finally {
        if (!active) return;
        setLoading(false);
      }
    };

    load();
    return () => {
      active = false;
    };
  }, [stationId, selection?.archname, selectedBaty]);

  const batteryOptions = useMemo(() => {
    const unique = [...new Set(batteries)].sort((a, b) => a - b);
    return [
      { value: 0, label: t('dm2000BatteryAverage') },
      ...unique.map((value) => ({ value, label: `${value}#` })),
    ];
  }, [batteries, t]);

  const filteredRows = useMemo(() => {
    const keyword = search.trim().toLowerCase();
    const normalized = (rows || []).map((row, index) => ({
      ...row,
      key: `${index}`,
      TIM: safeNum(row.TIM),
      VOLT: safeNum(row.VOLT),
    })).filter((row) => row.TIM !== null && row.VOLT !== null);

    if (!keyword) return normalized;
    return normalized.filter((row) => (
      String(row.TIM).toLowerCase().includes(keyword)
      || String(row.VOLT).toLowerCase().includes(keyword)
    ));
  }, [rows, search]);

  const columns = [
    { title: '#', key: 'idx', width: 80, render: (_value, _record, index) => index + 1 },
    {
      title: t('dm2000TimeMin'),
      dataIndex: 'TIM',
      key: 'TIM',
      sorter: (a, b) => Number(a.TIM) - Number(b.TIM),
      defaultSortOrder: 'ascend',
      render: (value) => `${Number(value).toFixed(4)} min`,
    },
    {
      title: t('dm2000VoltV'),
      dataIndex: 'VOLT',
      key: 'VOLT',
      sorter: (a, b) => Number(a.VOLT) - Number(b.VOLT),
      render: (value) => Number(value).toFixed(4),
    },
  ];

  if (!selection) {
    return <Empty description={t('dm2000SelectArchive')} />;
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Space wrap>
        <Select
          style={{ width: 220 }}
          value={selectedBaty}
          options={batteryOptions}
          onChange={onBatyChange}
          loading={batteryLoading}
        />
        <Input.Search
          allowClear
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder={t('dm2000Search')}
          style={{ width: 260 }}
        />
      </Space>

      {loading ? (
        <Spin />
      ) : filteredRows.length === 0 ? (
        <Empty description={t('dm2000NoData')} />
      ) : (
        <Table
          size="small"
          columns={columns}
          dataSource={filteredRows}
          pagination={{ pageSize: 100 }}
          scroll={{ y: 500 }}
          summary={() => (
            <Table.Summary.Row>
              <Table.Summary.Cell colSpan={3} index={0}>
                <Typography.Text type="secondary">{t('dmpTotalRows', { count: filteredRows.length })}</Typography.Text>
              </Table.Summary.Cell>
            </Table.Summary.Row>
          )}
        />
      )}
    </Space>
  );
}
