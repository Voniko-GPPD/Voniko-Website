import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Empty, Row, Select, Spin, Statistic } from 'antd';
import {
  Brush,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { fetchStats, fetchTelemetry } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const statsKeys = ['VOLT_MAX', 'VOLT_MIN', 'VOLT_AVG', 'IM_MAX', 'IM_MIN', 'IM_AVG'];

export default function DMPChartTab({ stationId, selection }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [telemetry, setTelemetry] = useState([]);
  const [stats, setStats] = useState({});
  const [visibleLines, setVisibleLines] = useState(['VOLT', 'Im']);

  useEffect(() => {
    if (!stationId || !selection?.cdmc || selection.channel == null) {
      setTelemetry([]);
      setStats({});
      setError('');
      return;
    }

    let mounted = true;
    setLoading(true);
    setError('');

    Promise.all([
      fetchTelemetry(stationId, selection.cdmc, selection.channel),
      fetchStats(stationId, selection.cdmc, selection.channel),
    ])
      .then(([telemetryRows, statsData]) => {
        if (!mounted) return;
        setTelemetry(telemetryRows);
        setStats(statsData || {});
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err.message || 'Failed to load chart data');
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, [stationId, selection]);

  const chartData = useMemo(
    () => telemetry.map((row, index) => ({
      index,
      TIM: Number(row.TIM),
      VOLT: Number(row.VOLT),
      Im: Number(row.Im),
    })),
    [telemetry]
  );

  if (!stationId) {
    return <Empty description={t('dmpSelectStationToChart')} />;
  }

  if (!selection) {
    return <Empty description={t('dmpSelectChannelToChart')} />;
  }

  if (loading) {
    return <Spin />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {error && <Alert type="error" message={error} showIcon />}

      <Row gutter={[12, 12]}>
        {statsKeys.map((key) => (
          <Col xs={24} sm={12} md={8} lg={8} xl={4} key={key}>
            <Card size="small">
              <Statistic title={key} value={stats[key]} precision={4} />
            </Card>
          </Col>
        ))}
      </Row>

      <Select
        mode="multiple"
        value={visibleLines}
        onChange={setVisibleLines}
        options={[
          { value: 'VOLT', label: t('dmpVoltage') },
          { value: 'Im', label: t('dmpCurrent') },
        ]}
        style={{ width: 260 }}
      />

      {chartData.length === 0 ? (
        <Empty description={t('dmpNoTelemetry')} />
      ) : (
        <div style={{ width: '100%', height: 520 }}>
          <ResponsiveContainer>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="TIM" type="number" domain={['dataMin', 'dataMax']} />
              <YAxis yAxisId="left" orientation="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip />
              <Legend />
              {visibleLines.includes('VOLT') && <Line yAxisId="left" type="monotone" dataKey="VOLT" stroke="#1677ff" dot={false} />}
              {visibleLines.includes('Im') && <Line yAxisId="right" type="monotone" dataKey="Im" stroke="#52c41a" dot={false} />}
              <Brush dataKey="index" height={28} stroke="#999" travellerWidth={8} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
