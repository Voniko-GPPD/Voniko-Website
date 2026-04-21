import React, { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Empty, Row, Select, Spin, Statistic, Switch, Typography } from 'antd';
import {
  Brush,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  fetchDM2000AverageCurve,
  fetchDM2000Batteries,
  fetchDM2000Curve,
  fetchDM2000Stats,
} from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const thresholds = [1.40, 1.35, 1.30, 1.25, 1.20, 1.15, 1.10, 1.05, 1.00, 0.95, 0.90];
const statItems = [
  { key: 'VOLT_MAX', title: 'VOLT MAX' },
  { key: 'VOLT_MIN', title: 'VOLT MIN' },
  { key: 'VOLT_AVG', title: 'VOLT AVG' },
  { key: 'DURATION_MIN', title: 'DURATION MIN' },
];

function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export default function DM2000CurveTab({ stationId, selection, selectedBaty, onBatyChange }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [statsLoading, setStatsLoading] = useState(false);
  const [batteryLoading, setBatteryLoading] = useState(false);
  const [error, setError] = useState('');
  const [curve, setCurve] = useState([]);
  const [stats, setStats] = useState({});
  const [batteries, setBatteries] = useState([]);
  const [showThresholds, setShowThresholds] = useState(true);

  useEffect(() => {
    let active = true;
    setBatteries([]);
    if (!stationId || !selection?.archname) {
      setBatteryLoading(false);
      return () => {};
    }

    const load = async () => {
      setBatteryLoading(true);
      setError('');
      try {
        const rows = await fetchDM2000Batteries(stationId, selection.archname);
        if (!active) return;
        setBatteries((rows || []).map((value) => Number(value)).filter((value) => Number.isFinite(value)));
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
    setCurve([]);
    setStats({});
    if (!stationId || !selection?.archname) {
      setLoading(false);
      setStatsLoading(false);
      return () => {};
    }

    const load = async () => {
      setLoading(true);
      setStatsLoading(true);
      setError('');
      try {
        const [curveRows, statsRows] = await Promise.all([
          selectedBaty > 0
            ? fetchDM2000Curve(stationId, selection.archname, selectedBaty)
            : fetchDM2000AverageCurve(stationId, selection.archname),
          fetchDM2000Stats(stationId, selection.archname, selectedBaty),
        ]);
        if (!active) return;
        setCurve(curveRows || []);
        setStats(statsRows || {});
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load curve data');
      } finally {
        if (!active) return;
        setLoading(false);
        setStatsLoading(false);
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

  const chartData = useMemo(() => (curve || [])
    .map((row) => ({ TIM: safeNum(row.TIM), VOLT: safeNum(row.VOLT) }))
    .filter((row) => row.TIM !== null && row.VOLT !== null), [curve]);

  if (!selection) {
    return <Empty description={t('dm2000SelectArchive')} />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Row gutter={[12, 12]} align="middle">
        <Col>
          <Typography.Text strong>{t('dm2000BatteryNo')}:</Typography.Text>
        </Col>
        <Col>
          <Select
            style={{ width: 220 }}
            value={selectedBaty}
            options={batteryOptions}
            onChange={onBatyChange}
            loading={batteryLoading}
          />
        </Col>
        <Col>
          <Switch checked={showThresholds} onChange={setShowThresholds} />
        </Col>
        <Col>
          <Typography.Text>{t('dm2000ShowThresholds')}</Typography.Text>
        </Col>
      </Row>

      <Row gutter={[12, 12]}>
        {statItems.map((item) => (
          <Col xs={24} sm={12} md={6} key={item.key}>
            <Card size="small" loading={statsLoading}>
              <Statistic title={item.title} value={stats[item.key]} precision={4} />
            </Card>
          </Col>
        ))}
      </Row>

      {loading ? (
        <Spin />
      ) : chartData.length === 0 ? (
        <Empty description={t('dm2000NoData')} />
      ) : (
        <div style={{ width: '100%', height: 480 }}>
          <ResponsiveContainer>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="TIM" type="number" domain={['dataMin', 'dataMax']} label={{ value: t('dm2000TimeMin'), position: 'insideBottom', offset: -5 }} />
              <YAxis domain={[0.90, 1.60]} unit="V" />
              <Tooltip
                formatter={(value) => [`${Number(value).toFixed(4)} V`, t('dm2000VoltV')]}
                labelFormatter={(label) => `${Number(label).toFixed(4)} min`}
              />
              {showThresholds && thresholds.map((value) => (
                <ReferenceLine key={value} y={value} stroke="#ccc" strokeDasharray="4 4" />
              ))}
              <Line type="monotone" dataKey="VOLT" stroke="#1677ff" strokeWidth={1.5} dot={false} />
              <Brush dataKey="TIM" height={24} stroke="#999" travellerWidth={8} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
