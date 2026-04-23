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
import { fetchChannels, fetchTelemetry } from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';

const singleStatsKeys = ['VOLT_MAX', 'VOLT_MIN', 'VOLT_AVG', 'IM_MAX', 'IM_MIN', 'IM_AVG'];
const batchStatsKeys = ['VOLT_MAX', 'VOLT_MIN', 'IM_MAX', 'IM_MIN'];
const STATS_PRECISION = 10000;

const CHANNEL_PALETTE = [
  '#1677ff', '#ff4d4f', '#52c41a', '#fa8c16', '#722ed1',
  '#13c2c2', '#eb2f96', '#faad14', '#2f54eb', '#a0d911',
];

function safeNum(val) {
  if (val === null || val === undefined || val === '--' || val === '') return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

function computeStats(telemetry) {
  const voltVals = telemetry.map((r) => safeNum(r.VOLT ?? r.volt ?? r.Volt)).filter((v) => v !== null);
  const imVals = telemetry.map((r) => safeNum(r.Im ?? r.IM ?? r.im)).filter((v) => v !== null);
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
    VOLT_MAX: v.max,
    VOLT_MIN: v.min,
    VOLT_AVG: v.avg,
    IM_MAX: i.max,
    IM_MIN: i.min,
    IM_AVG: ia.avg,
  };
}

function computeBatchStats(allChannelTelemetry) {
  const perChannel = allChannelTelemetry.map((ch) => computeStats(ch.rows));
  const avg = (vals) => {
    const valid = vals.filter((v) => v !== null);
    if (!valid.length) return null;
    return Math.round((valid.reduce((a, b) => a + b, 0) / valid.length) * STATS_PRECISION) / STATS_PRECISION;
  };
  return {
    VOLT_MAX: avg(perChannel.map((s) => s.VOLT_MAX)),
    VOLT_MIN: avg(perChannel.map((s) => s.VOLT_MIN)),
    IM_MAX: avg(perChannel.map((s) => s.IM_MAX)),
    IM_MIN: avg(perChannel.map((s) => s.IM_MIN)),
  };
}

export default function DMPChartTab({ stationId, selection }) {
  const { t } = useLang();
  const isBatch = selection?.isBatch === true;

  // Single-channel state
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [telemetry, setTelemetry] = useState([]);
  const [stats, setStats] = useState({});
  const [visibleLines, setVisibleLines] = useState(['VOLT', 'Im']);

  // Batch state
  const [batchChannelTelemetry, setBatchChannelTelemetry] = useState([]);

  // Single-channel effect
  useEffect(() => {
    if (isBatch) return;
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

    const abortController = new AbortController();
    setLoading(true);
    setError('');

    fetchTelemetry(stationId, selection.cdmc, selection.channel, abortController.signal)
      .then((rows) => {
        setTelemetry(rows);
        setStats(computeStats(rows));
      })
      .catch((err) => {
        if (err.name === 'AbortError') return;
        setError(err.message || 'Failed to load telemetry data');
      })
      .finally(() => {
        if (abortController.signal.aborted) return;
        setLoading(false);
      });

    return () => {
      abortController.abort();
    };
  }, [stationId, selection, isBatch, t]);

  // Batch effect: fetch all channels then all telemetry in parallel
  useEffect(() => {
    if (!isBatch) return;
    if (!stationId || !selection?.batchId) {
      setBatchChannelTelemetry([]);
      setError('');
      return;
    }

    let mounted = true;
    setLoading(true);
    setError('');
    setBatchChannelTelemetry([]);

    fetchChannels(stationId, selection.batchId)
      .then((channels) => {
        if (!mounted) return;
        if (!channels.length) {
          setLoading(false);
          return;
        }
        const failedChannels = [];
        return Promise.all(
          channels.map((ch) =>
            fetchTelemetry(stationId, ch.cdmc, ch.baty)
              .then((rows) => ({ channel: ch.baty, rows }))
              .catch((err) => {
                failedChannels.push({ channel: ch.baty, reason: err.message || 'Unknown error' });
                return { channel: ch.baty, rows: [] };
              })
          )
        ).then((results) => {
          if (!mounted) return;
          setBatchChannelTelemetry(results);
          if (failedChannels.length) {
            setError(`Failed to load channels: ${failedChannels.map((f) => `CH${f.channel}`).join(', ')}`);
          }
          setLoading(false);
        });
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err.message || 'Failed to load batch data');
        setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, [stationId, selection, isBatch]);

  // Single-channel chart data
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

  // Batch chart data: merged by row index, each channel gets its own VOLT_N / Im_N key
  const batchChartData = useMemo(() => {
    if (!batchChannelTelemetry.length) return [];
    const lengths = batchChannelTelemetry.map((ch) => ch.rows.length);
    const minLen = lengths.length ? Math.min(...lengths) : 0;
    if (minLen === 0) return [];
    return Array.from({ length: minLen }, (_, i) => {
      const point = {
        index: i,
        TIM: safeNum(batchChannelTelemetry[0].rows[i]?.TIM),
      };
      batchChannelTelemetry.forEach((ch) => {
        point[`VOLT_${ch.channel}`] = safeNum(ch.rows[i]?.VOLT);
        point[`Im_${ch.channel}`] = safeNum(ch.rows[i]?.Im);
      });
      return point;
    }).filter((d) => d.TIM !== null);
  }, [batchChannelTelemetry]);

  const batchStats = useMemo(
    () => (batchChannelTelemetry.length ? computeBatchStats(batchChannelTelemetry) : {}),
    [batchChannelTelemetry]
  );

  const tooltipFormatter = (value, name) => {
    const num = Number(value);
    if (name === 'VOLT') {
      return [`VOLT: ${Number.isFinite(num) ? num.toFixed(4) : '-'}V`, t('dmpVoltage')];
    }
    if (name === 'Im') {
      return [`Im: ${Number.isFinite(num) ? num.toFixed(4) : '-'}mA`, t('dmpCurrent')];
    }
    if (name.startsWith('VOLT_')) {
      return [`${Number.isFinite(num) ? num.toFixed(4) : '-'}V`, name];
    }
    if (name.startsWith('Im_')) {
      return [`${Number.isFinite(num) ? num.toFixed(4) : '-'}mA`, name];
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

  if (isBatch) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        {error && <Alert type="error" message={error} showIcon />}

        <Row gutter={[12, 12]}>
          {batchStatsKeys.map((key) => (
            <Col xs={24} sm={12} md={6} lg={6} xl={6} key={key}>
              <Card size="small">
                <Statistic title={key} value={batchStats[key]} precision={4} />
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

        {batchChartData.length === 0 ? (
          <Empty description={t('dmpNoTelemetry')} />
        ) : (
          <div style={{ width: '100%', height: 480 }}>
            <ResponsiveContainer>
              <LineChart data={batchChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="TIM" type="number" domain={['dataMin', 'dataMax']} label={{ value: t('dmpTimeH'), position: 'insideBottom', offset: -5 }} />
                <YAxis yAxisId="left" orientation="left" domain={[0.9, 1.85]} unit="V" label={{ value: 'V', angle: -90, position: 'insideLeft' }} />
                <YAxis yAxisId="right" orientation="right" unit="mA" label={{ value: 'mA', angle: 90, position: 'insideRight' }} />
                <Tooltip formatter={tooltipFormatter} labelFormatter={tooltipLabelFormatter} />
                <Legend />
                {batchChannelTelemetry.map((ch, idx) => {
                  const color = CHANNEL_PALETTE[idx % CHANNEL_PALETTE.length];
                  return (
                    <React.Fragment key={ch.channel}>
                      {visibleLines.includes('VOLT') && (
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey={`VOLT_${ch.channel}`}
                          stroke={color}
                          strokeWidth={1.5}
                          dot={false}
                          name={`VOLT CH${ch.channel}`}
                        />
                      )}
                      {visibleLines.includes('Im') && (
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey={`Im_${ch.channel}`}
                          stroke={color}
                          strokeWidth={1.5}
                          strokeDasharray="4 2"
                          dot={false}
                          name={`Im CH${ch.channel}`}
                        />
                      )}
                    </React.Fragment>
                  );
                })}
                <Brush dataKey="index" height={28} stroke="#999" travellerWidth={8} />
              </LineChart>
            </ResponsiveContainer>
            <div style={{ marginTop: 8, color: 'rgba(0,0,0,0.45)', fontSize: 12 }}>{t('dmpZoomControl')}</div>
          </div>
        )}
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {error && <Alert type="error" message={error} showIcon />}

      <Row gutter={[12, 12]}>
        {singleStatsKeys.map((key) => (
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
