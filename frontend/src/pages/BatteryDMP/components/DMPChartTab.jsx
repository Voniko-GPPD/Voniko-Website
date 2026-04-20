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

function safeNum(val) {
  if (val === null || val === undefined || val === '--' || val === '') return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

export default function DMPChartTab({ stationId, selection }) {
  const { t } = useLang();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [telemetry, setTelemetry] = useState([]);
  const [stats, setStats] = useState({});
  const [visibleLines, setVisibleLines] = useState(['VOLT', 'Im']);

  useEffect(() => {
    if (!stationId || !selection) {
      setTelemetry([]);
      setStats({});
      setError('');
      return;
    }
    if (!selection.cdmc) {
      setError(t('dmpMissingCdmcDetailed'));
      setTelemetry([]);
      setStats({});
      return;
    }
    if (selection.channel == null) {
      setTelemetry([]);
      setStats({});
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
        setError(err.message || 'Failed to load telemetry data');
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });

    fetchStats(stationId, selection.cdmc, selection.channel)
      .then((data) => {
        if (!mounted) return;
        setStats(data || {});
      })
      .catch(() => {
        if (!mounted) return;
        setStats({});
      });

    return () => {
      mounted = false;
    };
  }, [stationId, selection, t]);

  const chartData = useMemo(
    () => telemetry
      .map((row, index) => ({
        index,
        TIM: safeNum(row.TIM),
        VOLT: safeNum(row.VOLT),
        Im: safeNum(row.Im),
      }))
      .filter((d) => d.TIM !== null),
    [telemetry]
  );

  const tooltipFormatter = (value, name) => {
    const num = Number(value);
    if (name === 'VOLT') {
      return [`VOLT: ${Number.isFinite(num) ? num.toFixed(4) : '-'}V`, t('dmpVoltage')];
    }
    if (name === 'Im') {
      return [`Im: ${Number.isFinite(num) ? num.toFixed(4) : '-'}mA`, t('dmpCurrent')];
    }
    return [value, name];
  };

  const tooltipLabelFormatter = (label) => {
    const tim = Number(label);
    return `TIM: ${Number.isFinite(tim) ? tim.toFixed(4) : '-'}h`;
  };

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
        <div style={{ width: '100%', height: 480 }}>
          <ResponsiveContainer>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="TIM" type="number" domain={['dataMin', 'dataMax']} label={{ value: t('dmpTimeH'), position: 'insideBottom', offset: -5 }} />
              <YAxis yAxisId="left" orientation="left" domain={[0.9, 1.85]} unit="V" label={{ value: 'V', angle: -90, position: 'insideLeft' }} />
              <YAxis yAxisId="right" orientation="right" unit="mA" label={{ value: 'mA', angle: 90, position: 'insideRight' }} />
              <Tooltip formatter={tooltipFormatter} labelFormatter={tooltipLabelFormatter} />
              <Legend />
              {visibleLines.includes('VOLT') && <Line yAxisId="left" type="monotone" dataKey="VOLT" stroke="#1677ff" strokeWidth={1.5} dot={false} />}
              {visibleLines.includes('Im') && <Line yAxisId="right" type="monotone" dataKey="Im" stroke="#ff4d4f" strokeWidth={1.5} dot={false} />}
              <Brush dataKey="index" height={28} stroke="#999" travellerWidth={8} />
            </LineChart>
          </ResponsiveContainer>
          <div style={{ marginTop: 8, color: 'rgba(0,0,0,0.45)', fontSize: 12 }}>{t('dmpZoomControl')}</div>
        </div>
      )}
    </div>
  );
}
