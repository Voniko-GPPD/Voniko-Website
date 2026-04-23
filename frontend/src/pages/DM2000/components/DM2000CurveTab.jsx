import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Button, Card, Col, Empty, Row, Select, Spin, Statistic, Switch, Typography, notification } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import {
  Brush,
  CartesianGrid,
  Legend,
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

const SHOW_ALL_VALUE = -1;
const thresholds = [1.40, 1.35, 1.30, 1.25, 1.20, 1.15, 1.10, 1.05, 1.00, 0.95, 0.90];
const statItems = [
  { key: 'VOLT_MAX', title: 'VOLT MAX', suffix: 'V' },
  { key: 'VOLT_MIN', title: 'VOLT MIN', suffix: 'V' },
  { key: 'VOLT_AVG', title: 'VOLT AVG', suffix: 'V' },
  { key: 'DURATION_MIN', title: 'Duration', suffix: 'min' },
];
const BATTERY_COLORS = [
  '#1677ff', '#f5222d', '#52c41a', '#faad14', '#722ed1',
  '#13c2c2', '#eb2f96', '#fa8c16', '#a0d911', '#2f54eb',
];

function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function downloadChartAsPng(containerRef, filename) {
  const container = containerRef.current;
  if (!container) {
    notification.warning({ message: 'Chart container not found' });
    return;
  }
  const svg = container.querySelector('svg');
  if (!svg) {
    notification.warning({ message: 'Chart SVG not available' });
    return;
  }

  const { width, height } = svg.getBoundingClientRect();
  const svgData = new XMLSerializer().serializeToString(svg);
  const canvas = document.createElement('canvas');
  canvas.width = width * 2;
  canvas.height = height * 2;
  const ctx = canvas.getContext('2d');
  ctx.scale(2, 2);
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, width, height);

  const img = new Image();
  img.onload = () => {
    ctx.drawImage(img, 0, 0, width, height);
    const a = document.createElement('a');
    a.download = filename;
    a.href = canvas.toDataURL('image/png');
    document.body.appendChild(a);
    a.click();
    a.remove();
  };
  img.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svgData)}`;
}

export default function DM2000CurveTab({ stationId, selection }) {
  const { t } = useLang();
  const chartRef = useRef(null);
  const [loading, setLoading] = useState(false);
  const [statsLoading, setStatsLoading] = useState(false);
  const [batteryLoading, setBatteryLoading] = useState(false);
  const [error, setError] = useState('');
  const [curve, setCurve] = useState([]);
  const [stats, setStats] = useState({});
  const [batteries, setBatteries] = useState([]);
  const [showThresholds, setShowThresholds] = useState(true);
  const [selectedBaty, setSelectedBaty] = useState(SHOW_ALL_VALUE);
  const [allCurves, setAllCurves] = useState({});

  useEffect(() => {
    setSelectedBaty(SHOW_ALL_VALUE);
  }, [selection?.archname]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setBatteries([]);
    if (!stationId || !selection?.archname) {
      setBatteryLoading(false);
      return () => {};
    }

    const load = async () => {
      setBatteryLoading(true);
      setError('');
      try {
        const rows = await fetchDM2000Batteries(stationId, selection.archname, { signal: controller.signal });
        if (!active) return;
        setBatteries(
          (rows || [])
            .map((row) => Number(row?.baty ?? row?.BATY ?? row?.Baty ?? row))
            .filter((value) => Number.isFinite(value) && value > 0),
        );
      } catch (err) {
        if (!active || err.name === 'AbortError') return;
        setError(err.message || 'Failed to load batteries');
      } finally {
        if (!active) return;
        setBatteryLoading(false);
      }
    };

    load();
    return () => {
      active = false;
      controller.abort();
    };
  }, [stationId, selection?.archname]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setCurve([]);
    setStats({});
    setAllCurves({});
    if (!stationId || !selection?.archname) {
      setLoading(false);
      setStatsLoading(false);
      return () => {};
    }

    if (selectedBaty === SHOW_ALL_VALUE) {
      if (batteries.length === 0) {
        setLoading(false);
        return () => { active = false; controller.abort(); };
      }
      const loadAll = async () => {
        setLoading(true);
        setStatsLoading(true);
        setError('');
        try {
          const [curveResults, statsResults] = await Promise.all([
            Promise.all(
              batteries.map((baty) =>
                fetchDM2000Curve(stationId, selection.archname, baty, { signal: controller.signal }).then((rows) => ({ baty, rows: rows || [] })),
              ),
            ),
            Promise.all(
              batteries.map((baty) =>
                fetchDM2000Stats(stationId, selection.archname, baty, { signal: controller.signal }).then((s) => ({ baty, stats: s || {} })),
              ),
            ),
          ]);
          if (!active) return;
          const curveMap = {};
          curveResults.forEach(({ baty, rows }) => {
            curveMap[baty] = rows.map((row) => ({ TIM: safeNum(row.TIM), VOLT: safeNum(row.VOLT) })).filter((r) => r.TIM !== null && r.VOLT !== null);
          });
          setAllCurves(curveMap);
          const allStats = {};
          statsResults.forEach(({ baty, stats: s }) => {
            allStats[baty] = s;
          });
          const allVoltMax = statsResults.map(({ stats: s }) => safeNum(s.VOLT_MAX)).filter((v) => v !== null);
          const allVoltMin = statsResults.map(({ stats: s }) => safeNum(s.VOLT_MIN)).filter((v) => v !== null);
          const allVoltAvg = statsResults.map(({ stats: s }) => safeNum(s.VOLT_AVG)).filter((v) => v !== null);
          const allDuration = statsResults.map(({ stats: s }) => safeNum(s.DURATION_MIN)).filter((v) => v !== null);
          setStats(allVoltMax.length > 0 ? {
            VOLT_MAX: Math.max(...allVoltMax),
            VOLT_MIN: Math.min(...allVoltMin),
            VOLT_AVG: allVoltAvg.reduce((acc, v) => acc + v, 0) / allVoltAvg.length,
            DURATION_MIN: allDuration.length > 0 ? Math.max(...allDuration) : null,
          } : {});
        } catch (err) {
          if (!active || err.name === 'AbortError') return;
          setError(err.message || 'Failed to load curve data');
        } finally {
          if (!active) return;
          setLoading(false);
          setStatsLoading(false);
        }
      };
      loadAll();
      return () => { active = false; controller.abort(); };
    }

    const load = async () => {
      setLoading(true);
      setStatsLoading(true);
      setError('');
      try {
        const [curveRows, statsRows] = await Promise.all([
          selectedBaty > 0
            ? fetchDM2000Curve(stationId, selection.archname, selectedBaty, { signal: controller.signal })
            : fetchDM2000AverageCurve(stationId, selection.archname, { signal: controller.signal }),
          fetchDM2000Stats(stationId, selection.archname, selectedBaty, { signal: controller.signal }),
        ]);
        if (!active) return;
        setCurve(curveRows || []);
        setStats(statsRows || {});
      } catch (err) {
        if (!active || err.name === 'AbortError') return;
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
      controller.abort();
    };
  }, [stationId, selection?.archname, selectedBaty, batteries]);

  const batteryOptions = useMemo(() => {
    const unique = [...new Set(batteries)].sort((a, b) => a - b);
    return [
      { value: SHOW_ALL_VALUE, label: t('dm2000ShowAll') },
      ...unique.map((value) => ({ value, label: `${value}#` })),
    ];
  }, [batteries, t]);

  const chartData = useMemo(() => (curve || [])
    .map((row) => ({ TIM: safeNum(row.TIM), VOLT: safeNum(row.VOLT) }))
    .filter((row) => row.TIM !== null && row.VOLT !== null), [curve]);

  const multilineChartData = useMemo(() => {
    if (selectedBaty !== SHOW_ALL_VALUE || Object.keys(allCurves).length === 0) return null;
    const timMap = new Map();
    Object.entries(allCurves).forEach(([baty, rows]) => {
      rows.forEach((row) => {
        if (!timMap.has(row.TIM)) timMap.set(row.TIM, { TIM: row.TIM });
        timMap.get(row.TIM)[`VOLT_${baty}`] = row.VOLT;
      });
    });
    return [...timMap.values()].sort((a, b) => a.TIM - b.TIM);
  }, [selectedBaty, allCurves]);

  const sortedBatteries = useMemo(() => Object.keys(allCurves).map(Number).sort((a, b) => a - b), [allCurves]);

  if (!selection) {
    return <Empty description={t('dm2000SelectArchive')} />;
  }

  const chartFilename = `dm2000_chart_${selection?.archname || 'chart'}.png`;

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
            onChange={setSelectedBaty}
            loading={batteryLoading}
          />
        </Col>
        <Col>
          <Switch checked={showThresholds} onChange={setShowThresholds} />
        </Col>
        <Col>
          <Typography.Text>{t('dm2000ShowThresholds')}</Typography.Text>
        </Col>
        <Col>
          <Button
            icon={<DownloadOutlined />}
            size="small"
            onClick={() => downloadChartAsPng(chartRef, chartFilename)}
            disabled={loading || (selectedBaty === SHOW_ALL_VALUE ? !multilineChartData : chartData.length === 0)}
          >
            {t('dm2000DownloadChart')}
          </Button>
        </Col>
      </Row>

      <Row gutter={[12, 12]}>
        {statItems.map((item) => (
          <Col xs={24} sm={12} md={6} key={item.key}>
            <Card size="small" loading={statsLoading}>
              <Statistic
                title={item.title}
                value={stats[item.key] != null ? Number(stats[item.key]).toFixed(4) : '-'}
                suffix={item.suffix}
              />
            </Card>
          </Col>
        ))}
      </Row>

      {loading ? (
        <Spin />
      ) : selectedBaty === SHOW_ALL_VALUE ? (
        multilineChartData && multilineChartData.length > 0 ? (
          <div ref={chartRef} style={{ width: '100%', height: 480 }}>
            <ResponsiveContainer>
              <LineChart data={multilineChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="TIM" type="number" domain={['dataMin', 'dataMax']} label={{ value: t('dm2000TimeMin'), position: 'insideBottom', offset: -5 }} />
                <YAxis domain={['auto', 'auto']} unit="V" />
                <Tooltip
                  formatter={(value, name) => [`${Number(value).toFixed(4)} V`, name]}
                  labelFormatter={(label) => `${Number(label).toFixed(4)} min`}
                />
                <Legend />
                {showThresholds && thresholds.map((value) => (
                  <ReferenceLine key={value} y={value} stroke="#ccc" strokeDasharray="4 4" />
                ))}
                {sortedBatteries.map((baty, index) => (
                  <Line
                    key={baty}
                    type="monotone"
                    dataKey={`VOLT_${baty}`}
                    name={`${baty}#`}
                    stroke={BATTERY_COLORS[index % BATTERY_COLORS.length]}
                    strokeWidth={1.5}
                    dot={false}
                    connectNulls
                  />
                ))}
                <Brush dataKey="TIM" height={24} stroke="#999" travellerWidth={8} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <Empty description={t('dm2000NoData')} />
        )
      ) : chartData.length === 0 ? (
        <Empty description={t('dm2000NoData')} />
      ) : (
        <div ref={chartRef} style={{ width: '100%', height: 480 }}>
          <ResponsiveContainer>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="TIM" type="number" domain={['dataMin', 'dataMax']} label={{ value: t('dm2000TimeMin'), position: 'insideBottom', offset: -5 }} />
              <YAxis domain={[0.90, 'auto']} unit="V" />
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
