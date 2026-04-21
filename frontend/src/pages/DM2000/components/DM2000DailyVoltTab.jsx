import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Empty, Select, Space, Spin, Table } from 'antd';
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { fetchDM2000DailyVoltage } from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export default function DM2000DailyVoltTab({ stationId, selection }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [rows, setRows] = useState([]);
  const [selectedBaty, setSelectedBaty] = useState(1);
  const lastArchRef = useRef(null);

  useEffect(() => {
    if (!selection?.archname) {
      lastArchRef.current = null;
      return;
    }
    if (lastArchRef.current !== selection.archname) {
      lastArchRef.current = selection.archname;
      setSelectedBaty(1);
    }
  }, [selection?.archname]);

  useEffect(() => {
    let active = true;
    setRows([]);
    if (!stationId || !selection?.archname || !selectedBaty) {
      setLoading(false);
      return () => {};
    }

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const result = await fetchDM2000DailyVoltage(stationId, selection.archname, selectedBaty);
        if (!active) return;
        setRows(result || []);
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load daily voltage');
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

  const batteryOptions = useMemo(() => Array.from({ length: 9 }).map((_, index) => ({
    value: index + 1,
    label: `${index + 1}#`,
  })), []);

  const dataSource = useMemo(() => (rows || []).map((row, index) => ({
    key: `${index}`,
    date: row.date || row.DATE || row.dat || row.DAT,
    voltage: safeNum(row.volt || row.VOLT || row.voltage),
  })).filter((row) => row.date && row.voltage !== null), [rows]);

  if (!selection) {
    return <Empty description={t('dm2000SelectArchive')} />;
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Select
        style={{ width: 220 }}
        value={selectedBaty || 1}
        options={batteryOptions}
        onChange={setSelectedBaty}
      />

      {loading ? (
        <Spin />
      ) : dataSource.length === 0 ? (
        <Empty description={t('dm2000NoData')} />
      ) : (
        <>
          <Table
            size="small"
            dataSource={dataSource}
            pagination={{ pageSize: 50 }}
            columns={[
              {
                title: t('dm2000StartDate'),
                dataIndex: 'date',
                key: 'date',
                sorter: (a, b) => String(a.date).localeCompare(String(b.date)),
                defaultSortOrder: 'ascend',
              },
              {
                title: t('dm2000VoltV'),
                dataIndex: 'voltage',
                key: 'voltage',
                sorter: (a, b) => Number(a.voltage) - Number(b.voltage),
                render: (value) => Number(value).toFixed(4),
              },
            ]}
            scroll={{ y: 300 }}
          />

          <div style={{ width: '100%', height: 300 }}>
            <ResponsiveContainer>
              <LineChart data={dataSource}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis domain={[0.90, 1.60]} unit="V" />
                <Tooltip formatter={(value) => `${Number(value).toFixed(4)} V`} />
                <Line type="monotone" dataKey="voltage" stroke="#1677ff" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </Space>
  );
}
