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
import { fetchChannels, fetchStats, fetchTelemetry } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const SHOW_ALL_VALUE = -1;
const voltThresholds = [1.40, 1.35, 1.30, 1.25, 1.20, 1.15, 1.10, 1.05, 1.00, 0.95, 0.90];
const STATS_PRECISION = 10000;
const CHANNEL_COLORS = [
  '#1677ff', '#f5222d', '#52c41a', '#faad14', '#722ed1',
  '#13c2c2', '#eb2f96', '#fa8c16', '#a0d911', '#2f54eb',
];

const singleStatItems = [
  { key: 'VOLT_MAX', title: 'VOLT MAX', suffix: 'V' },
  { key: 'VOLT_MIN', title: 'VOLT MIN', suffix: 'V' },
  { key: 'VOLT_AVG', title: 'VOLT AVG', suffix: 'V' },
  { key: 'IM_MAX', title: 'Im MAX', suffix: 'mA' },
  { key: 'IM_MIN', title: 'Im MIN', suffix: 'mA' },
  { key: 'IM_AVG', title: 'Im AVG', suffix: 'mA' },
];

const batchStatItems = [
  { key: 'VOLT_MAX', title: 'VOLT MAX', suffix: 'V' },
  { key: 'VOLT_MIN', title: 'VOLT MIN', suffix: 'V' },
  { key: 'IM_MAX', title: 'Im MAX', suffix: 'mA' },
  { key: 'IM_MIN', title: 'Im MIN', suffix: 'mA' },
];

function safeNum(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function computeStatsFromRows(rows) {
  const voltVals = rows.map((r) => safeNum(r.VOLT ?? r.volt ?? r.Volt)).filter((v) => v !== null);
  const imVals = rows.map((r) => safeNum(r.Im ?? r.IM ?? r.im)).filter((v) => v !== null);
  const imActive = imVals.filter((v) => v > 0);
  const agg = (arr) => {
    if (!arr.length) return { max: null, min: null, avg: null };
    const sum = arr.reduce((a, b) => a + b, 0);
    return {
      max: Math.round(Math.max(...arr) * STATS_PRECISION) / STATS_PRECISION,
      min: Math.round(Math.min(...arr) * STATS_PRECISION) / STATS_PRECISION,
      avg: Math.round((sum / arr.length) * STATS_PRECISION) / STATS_PRECISION,
    };
  };
  const v = agg(voltVals);
  const i = agg(imVals);
  const ia = agg(imActive);
  return {
    VOLT_MAX: v.max, VOLT_MIN: v.min, VOLT_AVG: v.avg,
    IM_MAX: i.max, IM_MIN: i.min, IM_AVG: ia.avg,
  };
}

function downloadChartAsPng(containerRef, filename) {
  const container = containerRef.current;
  if (!container) { notification.warning({ message: 'Chart container not found' }); return; }
  const svg = container.querySelector('svg');
  if (!svg) { notification.warning({ message: 'Chart SVG not available' }); return; }
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

export default function DMPChartTab({ stationId, selection }) {
  const { t } = useLang();
  const chartRef = useRef(null);
  const [channelLoading, setChannelLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [statsLoading, setStatsLoading] = useState(false);
  const [error, setError] = useState('');
  const [channels, setChannels] = useState([]);
  const [selectedBaty, setSelectedBaty] = useState(SHOW_ALL_VALUE);
  const [telemetry, setTelemetry] = useState([]);
  const [stats, setStats] = useState({});
  const [allChannelData, setAllChannelData] = useState({});
  const [showThresholds, setShowThresholds] = useState(true);

  // Reset channel selection when selection (batch) changes
  useEffect(() => {
    setSelectedBaty(SHOW_ALL_VALUE);
  }, [selection?.id]);

  // Load channels for selected batch
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
          (result || [])
            .filter((ch) => ch.baty != null && Number.isFinite(Number(ch.baty)) && Number(ch.baty) > 0),
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

  // Load telemetry / stats based on selected channel
  useEffect(() => {
    let active = true;
    setTelemetry([]);
    setStats({});
    setAllChannelData({});
    if (!stationId || !selection?.id) {
      setLoading(false);
      setStatsLoading(false);
      return () => {};
    }

    if (selectedBaty === SHOW_ALL_VALUE) {
      if (!channels.length) {
        setLoading(false);
        return () => { active = false; };
      }
      const loadAll = async () => {
        setLoading(true);
        setStatsLoading(true);
        setError('');
        try {
          const [telResults, statsResults] = await Promise.all([
            Promise.all(
              channels.map((ch) =>
                fetchTelemetry(stationId, ch.cdmc, ch.baty)
                  .then((rows) => ({ baty: ch.baty, rows: rows || [] }))
                  .catch(() => ({ baty: ch.baty, rows: [] })),
              ),
            ),
            Promise.all(
              channels.map((ch) =>
                fetchStats(stationId, ch.cdmc, ch.baty)
                  .then((s) => ({ baty: ch.baty, stats: s || {} }))
                  .catch(() => ({ baty: ch.baty, stats: {} })),
              ),
            ),
          ]);
          if (!active) return;
          const dataMap = {};
          telResults.forEach(({ baty, rows }) => {
            dataMap[baty] = rows
              .map((r) => ({ TIM: safeNum(r.TIM ?? r.tim), VOLT: safeNum(r.VOLT ?? r.volt) }))
              .filter((r) => r.TIM !== null && r.VOLT !== null);
          });
          setAllChannelData(dataMap);
          const allVoltMax = statsResults.map(({ stats: s }) => safeNum(s.VOLT_MAX)).filter((v) => v !== null);
          const allVoltMin = statsResults.map(({ stats: s }) => safeNum(s.VOLT_MIN)).filter((v) => v !== null);
          const allImMax = statsResults.map(({ stats: s }) => safeNum(s.IM_MAX)).filter((v) => v !== null);
          const allImMin = statsResults.map(({ stats: s }) => safeNum(s.IM_MIN)).filter((v) => v !== null);
          setStats(allVoltMax.length > 0 ? {
            VOLT_MAX: Math.max(...allVoltMax),
            VOLT_MIN: Math.min(...allVoltMin),
            IM_MAX: allImMax.length > 0 ? Math.max(...allImMax) : null,
            IM_MIN: allImMin.length > 0 ? Math.min(...allImMin) : null,
          } : {});
        } catch (err) {
          if (!active) return;
          setError(err.message || 'Failed to load chart data');
        } finally {
          if (!active) return;
          setLoading(false);
          setStatsLoading(false);
        }
      };
      loadAll();
      return () => { active = false; };
    }

    // Single channel
    const ch = channels.find((c) => Number(c.baty) === selectedBaty);
    if (!ch) {
      setLoading(false);
      setStatsLoading(false);
      return () => { active = false; };
    }

    const loadSingle = async () => {
      setLoading(true);
      setStatsLoading(true);
      setError('');
      try {
        const [rows, statsData] = await Promise.all([
          fetchTelemetry(stationId, ch.cdmc, ch.baty),
          fetchStats(stationId, ch.cdmc, ch.baty),
        ]);
        if (!active) return;
        setTelemetry(rows || []);
        setStats(statsData || computeStatsFromRows(rows || []));
      } catch (err) {
        if (!active) return;
        setError(err.message || 'Failed to load chart data');
      } finally {
        if (!active) return;
        setLoading(false);
        setStatsLoading(false);
      }
    };
    loadSingle();
    return () => { active = false; };
  }, [stationId, selection?.id, selectedBaty, channels]);

  const channelOptions = useMemo(() => {
    const sorted = [...channels].sort((a, b) => Number(a.baty) - Number(b.baty));
    return [
      { value: SHOW_ALL_VALUE, label: t('dmpAllChannels') },
      ...sorted.map((ch) => ({ value: Number(ch.baty), label: `CH ${ch.baty}` })),
    ];
  }, [channels, t]);

  // Single channel chart data
  const chartData = useMemo(
    () => telemetry
      .map((row, index) => ({
        index,
        TIM: safeNum(row.TIM ?? row.tim),
        VOLT: safeNum(row.VOLT ?? row.volt ?? row.Volt),
        Im: safeNum(row.Im ?? row.IM ?? row.im),
      }))
      .filter((d) => d.TIM !== null),
    [telemetry],
  );

  // All-channel (show all) chart data merged by row index
  const multilineChartData = useMemo(() => {
    if (selectedBaty !== SHOW_ALL_VALUE || Object.keys(allChannelData).length === 0) return null;
    const timMap = new Map();
    Object.entries(allChannelData).forEach(([baty, rows]) => {
      rows.forEach((row) => {
        if (!timMap.has(row.TIM)) timMap.set(row.TIM, { TIM: row.TIM });
        timMap.get(row.TIM)[`VOLT_${baty}`] = row.VOLT;
      });
    });
    return [...timMap.values()].sort((a, b) => a.TIM - b.TIM);
  }, [selectedBaty, allChannelData]);

  const sortedBatyKeys = useMemo(
    () => Object.keys(allChannelData).map(Number).sort((a, b) => a - b),
    [allChannelData],
  );

  const activeStatItems = selectedBaty === SHOW_ALL_VALUE ? batchStatItems : singleStatItems;
  const chartFilename = `dmp_chart_${selection?.id || 'chart'}.png`;

  if (!selection) {
    return <Empty description={t('dmpSelectBatchToChart')} />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Row gutter={[12, 12]} align="middle">
        <Col>
          <Typography.Text strong>{t('dmpChannel')}:</Typography.Text>
        </Col>
        <Col>
          <Select
            style={{ width: 220 }}
            value={selectedBaty}
            options={channelOptions}
            onChange={setSelectedBaty}
            loading={channelLoading}
            placeholder={t('dmpSelectChannel')}
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
        {activeStatItems.map((item) => (
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
                <XAxis
                  dataKey="TIM"
                  type="number"
                  domain={['dataMin', 'dataMax']}
                  label={{ value: t('dmpTimeH'), position: 'insideBottom', offset: -5 }}
                />
                <YAxis domain={['auto', 'auto']} unit="V" />
                <Tooltip
                  formatter={(value, name) => [`${Number(value).toFixed(4)} V`, name]}
                  labelFormatter={(label) => `${Number(label).toFixed(4)} h`}
                />
                <Legend />
                {showThresholds && voltThresholds.map((value) => (
                  <ReferenceLine key={value} y={value} stroke="#ccc" strokeDasharray="4 4" />
                ))}
                {sortedBatyKeys.map((baty, index) => (
                  <Line
                    key={baty}
                    type="monotone"
                    dataKey={`VOLT_${baty}`}
                    name={`CH ${baty}`}
                    stroke={CHANNEL_COLORS[index % CHANNEL_COLORS.length]}
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
          <Empty description={t('dmpNoTelemetry')} />
        )
      ) : chartData.length === 0 ? (
        <Empty description={t('dmpNoTelemetry')} />
      ) : (
        <div ref={chartRef} style={{ width: '100%', height: 480 }}>
          <ResponsiveContainer>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="TIM"
                type="number"
                domain={['dataMin', 'dataMax']}
                label={{ value: t('dmpTimeH'), position: 'insideBottom', offset: -5 }}
              />
              <YAxis yAxisId="left" orientation="left" domain={[0.9, 1.85]} unit="V" label={{ value: 'V', angle: -90, position: 'insideLeft' }} />
              <YAxis yAxisId="right" orientation="right" unit="mA" label={{ value: 'mA', angle: 90, position: 'insideRight' }} />
              <Tooltip
                formatter={(value, name) => {
                  const num = Number(value);
                  if (name === 'VOLT') return [`${Number.isFinite(num) ? num.toFixed(4) : '-'} V`, t('dmpVoltage')];
                  if (name === 'Im') return [`${Number.isFinite(num) ? num.toFixed(4) : '-'} mA`, t('dmpCurrent')];
                  return [value, name];
                }}
                labelFormatter={(label) => `${Number(label).toFixed(4)} h`}
              />
              <Legend />
              {showThresholds && voltThresholds.map((value) => (
                <ReferenceLine key={value} yAxisId="left" y={value} stroke="#ccc" strokeDasharray="4 4" />
              ))}
              <Line yAxisId="left" type="monotone" dataKey="VOLT" name="VOLT" stroke="#1677ff" strokeWidth={1.5} dot={false} connectNulls />
              <Line yAxisId="right" type="monotone" dataKey="Im" name="Im" stroke="#ff4d4f" strokeWidth={1.5} dot={false} connectNulls />
              <Brush dataKey="index" height={28} stroke="#999" travellerWidth={8} />
            </LineChart>
          </ResponsiveContainer>
          <div style={{ marginTop: 8, color: 'rgba(0,0,0,0.45)', fontSize: 12 }}>{t('dmpZoomControl')}</div>
        </div>
      )}
    </div>
  );
}
