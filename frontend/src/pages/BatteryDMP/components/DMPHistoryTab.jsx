import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Empty, Input, Spin, Table, Typography } from 'antd';
import { fetchTelemetry } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const monoStyle = { fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace' };

function format4(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return num.toFixed(4);
}

export default function DMPHistoryTab({ stationId, selection }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [telemetry, setTelemetry] = useState([]);
  const [search, setSearch] = useState('');

  useEffect(() => {
    if (!stationId || !selection?.cdmc || selection.channel == null) {
      setTelemetry([]);
      setError('');
      return;
    }

    let mounted = true;
    setLoading(true);
    setError('');

    fetchTelemetry(stationId, selection.cdmc, selection.channel)
      .then((rows) => {
        if (!mounted) return;
        setTelemetry(rows);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err.message || 'Failed to load telemetry');
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, [stationId, selection]);

  const filteredRows = useMemo(() => {
    const keyword = search.trim().toLowerCase();
    if (!keyword) return telemetry;
    return telemetry.filter((row) => (
      String(row.baty).toLowerCase().includes(keyword)
      || String(row.TIM).toLowerCase().includes(keyword)
      || String(row.VOLT).toLowerCase().includes(keyword)
      || String(row.Im).toLowerCase().includes(keyword)
    ));
  }, [telemetry, search]);

  const columns = [
    {
      title: '#',
      key: 'index',
      width: 90,
      render: (_value, _record, index) => index + 1,
    },
    {
      title: t('dmpChannel'),
      dataIndex: 'baty',
      key: 'baty',
      sorter: (a, b) => Number(a.baty) - Number(b.baty),
      render: (value) => <span style={monoStyle}>{value}</span>,
    },
    {
      title: t('dmpTimeH'),
      dataIndex: 'TIM',
      key: 'TIM',
      sorter: (a, b) => Number(a.TIM) - Number(b.TIM),
      defaultSortOrder: 'ascend',
      render: (value) => <span style={monoStyle}>{format4(value)} h</span>,
    },
    {
      title: t('dmpVoltage'),
      dataIndex: 'VOLT',
      key: 'VOLT',
      sorter: (a, b) => Number(a.VOLT) - Number(b.VOLT),
      render: (value) => <span style={monoStyle}>{format4(value)}</span>,
    },
    {
      title: t('dmpCurrent'),
      dataIndex: 'Im',
      key: 'Im',
      sorter: (a, b) => Number(a.Im) - Number(b.Im),
      render: (value) => <span style={monoStyle}>{format4(value)}</span>,
    },
  ];

  if (!stationId) {
    return <Empty description={t('dmpSelectStationToHistory')} />;
  }

  if (!selection) {
    return <Empty description={t('dmpSelectChannelToHistory')} />;
  }

  if (loading) {
    return <Spin />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {error && <Alert type="error" message={error} showIcon />}
      <Input.Search
        allowClear
        value={search}
        onChange={(event) => setSearch(event.target.value)}
        placeholder={t('dmpSearchTelemetry')}
        style={{ maxWidth: 320 }}
      />

      {filteredRows.length === 0 ? (
        <Empty description={t('dmpNoData')} />
      ) : (
        <Table
          rowKey={(_record, index) => index}
          columns={columns}
          dataSource={filteredRows}
          pagination={{ pageSize: 200, showSizeChanger: true }}
          scroll={{ x: 900, y: 500 }}
          size="small"
          summary={() => (
            <Table.Summary.Row>
              <Table.Summary.Cell index={0} colSpan={5}>
                <Typography.Text type="secondary">{t('dmpTotalRows', { count: filteredRows.length })}</Typography.Text>
              </Table.Summary.Cell>
            </Table.Summary.Row>
          )}
        />
      )}
    </div>
  );
}
