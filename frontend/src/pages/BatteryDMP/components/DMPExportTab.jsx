import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert, Breadcrumb, Button, Card, Checkbox, Col, Empty,
  Form, Input, Row, Select, Space, Spin, Typography, notification,
} from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import {
  downloadSimpleReport,
  fetchChannels,
  fetchStats,
  fetchTelemetry,
} from '../../../api/dmpApi';
import { useLang } from '../../../contexts/LangContext';
import DischargeConditionHelp from '../../../components/DischargeConditionHelp';
import { composeDischargeCondition } from '../../../constants/dischargeConditions';

function safeNum(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function fmt(value, decimals = 3) {
  const n = safeNum(value);
  return n != null ? n.toFixed(decimals) : '-';
}

/** Derive an evenly-spaced (0.05 V step) descending list of voltage thresholds
 *  for the given telemetry curves, bounded by the optional endpoint voltage. */
function deriveThresholds(telemetryByBaty, endpointVoltage) {
  let maxV = null;
  let minV = null;
  Object.values(telemetryByBaty).forEach((rows) => {
    (rows || []).forEach((r) => {
      const v = safeNum(r.VOLT ?? r.volt ?? r.Volt);
      if (v == null) return;
      if (maxV == null || v > maxV) maxV = v;
      if (minV == null || v < minV) minV = v;
    });
  });
  if (maxV == null || minV == null) return [];
  const ep = endpointVoltage != null ? endpointVoltage : minV;
  const lowBound = ep > maxV ? minV : ep;
  const step = 0.05;
  const top = Math.floor(maxV / step) * step;
  let bot = Math.ceil(lowBound / step) * step;
  if (bot > top) bot = top;
  const out = [];
  let v = top;
  while (v >= bot - 1e-9 && out.length < 80) {
    out.push(Number(v.toFixed(3)));
    v -= step;
  }
  return out;
}

/** Time (in hours) at which the discharge curve first reaches voltage ``thr``. */
function timeAtVoltage(rows, thr) {
  const points = (rows || [])
    .map((r) => ({ t: safeNum(r.TIM), v: safeNum(r.VOLT ?? r.volt ?? r.Volt) }))
    .filter((p) => p.t != null && p.v != null)
    .sort((a, b) => a.t - b.t);
  if (points.length < 2) return null;
  for (let i = 1; i < points.length; i++) {
    const p1 = points[i - 1];
    const p2 = points[i];
    if (p1.v >= thr && p2.v <= thr) {
      if (p1.v === p2.v) return p1.t;
      return p1.t + (p2.t - p1.t) * ((p1.v - thr) / (p1.v - p2.v));
    }
  }
  return null;
}

/** Number of discharge "times" (1-based position of the first time-sorted
 *  telemetry sample whose voltage is <= ``thr``). Returns null if the
 *  threshold is never reached. */
function countAtVoltage(rows, thr) {
  const points = (rows || [])
    .map((r) => ({ t: safeNum(r.TIM), v: safeNum(r.VOLT ?? r.volt ?? r.Volt) }))
    .filter((p) => p.t != null && p.v != null)
    .sort((a, b) => a.t - b.t);
  if (points.length < 2) return null;
  for (let i = 1; i < points.length; i++) {
    if (points[i - 1].v >= thr && points[i].v <= thr) {
      return i + 1;
    }
  }
  return null;
}

/** Integrate Im (mA) over TIM (hours) using the trapezoidal rule → SOt mAh. */
function computeSotMah(rows) {
  const points = (rows || [])
    .map((r) => ({ t: safeNum(r.TIM), i: safeNum(r.Im ?? r.IM ?? r.im) }))
    .filter((p) => p.t != null && p.i != null)
    .sort((a, b) => a.t - b.t);
  if (points.length < 2) return null;
  let sum = 0;
  for (let i = 0; i < points.length - 1; i++) {
    const p1 = points[i];
    const p2 = points[i + 1];
    if (p2.t > p1.t) sum += ((p1.i + p2.i) / 2) * (p2.t - p1.t);
  }
  return sum > 0 ? sum : null;
}

/** First non-null voltage in the telemetry (open-circuit reading). */
function firstVoltage(rows) {
  for (const r of rows || []) {
    const v = safeNum(r.VOLT ?? r.volt ?? r.Volt);
    if (v != null) return v;
  }
  return null;
}

/** Last non-null voltage in the telemetry (final closed-circuit reading). */
function lastVoltage(rows) {
  if (!rows) return null;
  for (let i = rows.length - 1; i >= 0; i--) {
    const v = safeNum(rows[i].VOLT ?? rows[i].volt ?? rows[i].Volt);
    if (v != null) return v;
  }
  return null;
}

export default function DMPExportTab({ stationId, selection }) {
  const { t } = useLang();
  const [downloading, setDownloading] = useState(false);
  const [channelLoading, setChannelLoading] = useState(false);
  const [error, setError] = useState('');
  const [channels, setChannels] = useState([]);
  const [exportBatys, setExportBatys] = useState([]);
  const [telemetryMap, setTelemetryMap] = useState({});
  const [statsMap, setStatsMap] = useState({});
  const [archiveFields, setArchiveFields] = useState({});
  const [reportEndpoint, setReportEndpoint] = useState(null);
  // Tracks batteries that already have an in-flight or completed fetch so we
  // don't refetch on every toggle.
  const requestedBatysRef = useRef(new Set());

  // Reset state and seed archive info when the selected batch changes.
  useEffect(() => {
    setExportBatys([]);
    setTelemetryMap({});
    setStatsMap({});
    setReportEndpoint(null);
    requestedBatysRef.current = new Set();
    if (selection) {
      // For DMP, para_pub.fdfs already contains the canonical discharge
      // condition string (e.g. "(1500mW2s,650mW28s)10T/h,24h/d") — use it
      // as-is and only fall back to recomposing from fzdz/jstj/zzdy when
      // fdfs is empty (so we never silently rewrite the stored value).
      const rawFdfs = String(selection.fdfs || '').trim();
      const dischargeCondition = rawFdfs || composeDischargeCondition({
        load: selection.fzdz || selection.fz2 || '',
        cycle: selection.jstj || '',
        endpoint: selection.zzdy || '',
      });
      setArchiveFields({
        archname: selection.id || '',
        name: selection.name || selection.dcmc || '',
        startdate: selection.fdrq || '',
        enddate: selection.jsrq || selection.fdjssj || '',
        dcxh: selection.dcxh || '',
        fdfs: selection.fdfs || '',
        manufacturer: selection.manufacturer || selection.scdw || '',
        madedate: selection.madedate || selection.scrq || '',
        serialno: selection.serialno || selection.dcph || '',
        remarks: selection.remarks || selection.bz || '',
        voltage_type: selection.dylx || selection.fdlx || '',
        trademark: selection.sbmc || selection.trademark || '',
        load_resistance: selection.fzdz || selection.fz2 || '',
        endpoint_voltage: selection.zzdy || '',
        dis_condition: selection.jstj || selection.hjwd || selection.wd || '',
        discharge_condition: dischargeCondition,
        min_duration: selection.fdts || '',
      });
    } else {
      setArchiveFields({});
    }
  }, [selection?.id]);

  // Load channels for the selected batch.
  useEffect(() => {
    setChannels([]);
    if (!stationId || !selection?.id) {
      setChannelLoading(false);
      return () => {};
    }
    let active = true;
    setChannelLoading(true);
    setError('');
    fetchChannels(stationId, selection.id)
      .then((rows) => {
        if (!active) return;
        const sorted = (rows || [])
          .filter((ch) => ch.baty != null && Number.isFinite(Number(ch.baty)) && Number(ch.baty) > 0)
          .sort((a, b) => Number(a.baty) - Number(b.baty));
        setChannels(sorted);
      })
      .catch((err) => {
        if (!active) return;
        setError(err.message || 'Failed to load channels');
      })
      .finally(() => {
        if (!active) return;
        setChannelLoading(false);
      });
    return () => { active = false; };
  }, [stationId, selection?.id]);

  // Build a lookup for cdmc per channel; this is what fetchTelemetry/fetchStats
  // need to address the right .mdb file.
  const cdmcByBaty = useMemo(() => {
    const map = {};
    channels.forEach((ch) => {
      if (ch.cdmc) map[Number(ch.baty)] = ch.cdmc;
    });
    return map;
  }, [channels]);

  const batteryOptions = useMemo(
    () => channels.map((ch) => ({ value: Number(ch.baty), label: `${ch.baty}#` })),
    [channels],
  );

  // Lazily load telemetry + stats for newly checked batteries.
  useEffect(() => {
    if (!stationId || exportBatys.length === 0) return undefined;
    const missing = exportBatys.filter((b) => !requestedBatysRef.current.has(b));
    if (missing.length === 0) return undefined;
    missing.forEach((b) => requestedBatysRef.current.add(b));

    let active = true;
    const controller = new AbortController();
    Promise.all(missing.map(async (b) => {
      const cdmc = cdmcByBaty[b];
      if (!cdmc) return [b, { telemetry: [], stats: {} }];
      try {
        const [telemetry, stats] = await Promise.all([
          fetchTelemetry(stationId, cdmc, b, controller.signal).catch(() => []),
          fetchStats(stationId, cdmc, b, controller.signal).catch(() => ({})),
        ]);
        return [b, { telemetry: telemetry || [], stats: stats || {} }];
      } catch {
        return [b, { telemetry: [], stats: {} }];
      }
    })).then((entries) => {
      if (!active) return;
      const telUpdates = {};
      const statUpdates = {};
      entries.forEach(([b, { telemetry, stats }]) => {
        telUpdates[b] = telemetry;
        statUpdates[b] = stats;
      });
      setTelemetryMap((prev) => ({ ...prev, ...telUpdates }));
      setStatsMap((prev) => ({ ...prev, ...statUpdates }));
    }).catch(() => {});
    return () => { active = false; controller.abort(); };
  }, [stationId, exportBatys, cdmcByBaty]);

  const setField = (key) => (e) => setArchiveFields((prev) => ({ ...prev, [key]: e.target.value }));
  const setSelectField = (key) => (value) => setArchiveFields((prev) => ({ ...prev, [key]: value ?? '' }));

  const handleDownload = async () => {
    if (!stationId || !selection?.id) return;
    if (exportBatys.length === 0) {
      notification.warning({ message: t('dm2000SelectAtLeastOne') });
      return;
    }
    setDownloading(true);
    try {
      // cdmc is a fallback for channels with no per-channel cdmc; backend will
      // re-resolve from para_singl when it can.
      const fallbackCdmc = channels.find((ch) => ch.cdmc)?.cdmc;
      await downloadSimpleReport({
        stationId,
        batchId: selection.id,
        cdmc: fallbackCdmc,
        batys: exportBatys,
        overrideBatteryType: archiveFields.dcxh || undefined,
        overrideManufacturer: archiveFields.manufacturer || undefined,
        endpointCutoff: reportEndpoint != null ? reportEndpoint : undefined,
      });
      notification.success({ message: t('dmpReportDownloaded') });
    } catch (err) {
      notification.error({ message: t('dmpReportDownloadFailed'), description: err.message });
    } finally {
      setDownloading(false);
    }
  };

  if (!stationId) {
    return <Empty description={t('dmpSelectStationToExport')} />;
  }

  if (!selection) {
    return <Empty description={t('dmpSelectBatchToExport')} />;
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Breadcrumb
        items={[
          { title: `${t('dm2000Type')}: ${selection.dcxh || '-'}` },
          { title: `${t('dm2000StartDate')}: ${selection.fdrq || '-'}` },
          { title: `${t('dmpBatchId')}: ${selection.id || '-'}` },
        ]}
      />

      <Card size="small" title={t('dm2000SelectBattery')}>
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          {channelLoading ? <Spin size="small" /> : null}
          <Space wrap>
            <Button size="small" onClick={() => setExportBatys(batteryOptions.map((item) => item.value))}>
              {t('dm2000SelectAll')}
            </Button>
            <Button size="small" onClick={() => setExportBatys([])}>
              {t('dm2000ClearAll')}
            </Button>
          </Space>
          <Checkbox.Group
            options={batteryOptions}
            value={exportBatys}
            onChange={(values) => setExportBatys(values)}
          />
        </Space>
      </Card>

      <Card size="small" title={t('dm2000ArchiveInfo')}>
        <Row gutter={[16, 8]}>
          <Col xs={24} sm={8}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000Type')}>
                <Input value={archiveFields.dcxh || ''} onChange={setField('dcxh')} />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={8}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000Manufacturer')}>
                <Input value={archiveFields.manufacturer || ''} onChange={setField('manufacturer')} />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={8}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000MadeDate')}>
                <Input value={archiveFields.madedate || ''} onChange={setField('madedate')} />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={12}>
            <Form layout="vertical" size="small">
              <Form.Item
                label={(
                  <Space size={6}>
                    {t('remarkDischargeCondition')}
                    <DischargeConditionHelp
                      batteryType={archiveFields.dcxh}
                      onApply={(text) => setArchiveFields((prev) => ({ ...prev, discharge_condition: text }))}
                    />
                  </Space>
                )}
              >
                <Input
                  value={archiveFields.discharge_condition || ''}
                  onChange={setField('discharge_condition')}
                  placeholder="e.g. 10ohm 24h/d-0.9V (h)"
                />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={12}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000Remarks')}>
                <Input
                  value={archiveFields.remarks || ''}
                  onChange={setField('remarks')}
                />
              </Form.Item>
            </Form>
          </Col>
        </Row>
      </Card>

      <Card size="small" title={t('dm2000ReportPreviewTitle')}>
        {exportBatys.length === 0 ? (
          <Typography.Text type="secondary">{t('dm2000SelectAtLeastOne')}</Typography.Text>
        ) : (
          <ReportPreview
            archiveFields={archiveFields}
            previewBatys={exportBatys}
            telemetryMap={telemetryMap}
            statsMap={statsMap}
            reportEndpoint={reportEndpoint}
            setReportEndpoint={setReportEndpoint}
            t={t}
          />
        )}
      </Card>

      <Space wrap>
        <Button
          type="primary"
          icon={<DownloadOutlined />}
          onClick={handleDownload}
          loading={downloading}
          disabled={exportBatys.length === 0}
        >
          {t('dmpDownloadReport')}
        </Button>
      </Space>
    </Space>
  );
}

function ReportPreview({ archiveFields, previewBatys, telemetryMap, statsMap, reportEndpoint, setReportEndpoint, t }) {
  const cellStyle = {
    border: '1px solid #d9d9d9',
    padding: '3px 6px',
    fontSize: 12,
    whiteSpace: 'nowrap',
  };
  const headerCellStyle = { ...cellStyle, background: '#fafafa', fontWeight: 600 };
  const labelStyle = { ...cellStyle, background: '#f5f5f5', color: '#666' };

  const appendUnit = (val, unit) => {
    if (!val) return '-';
    const s = String(val).trim();
    if (!s || s === '-') return '-';
    if (s.toLowerCase().endsWith(unit.toLowerCase())) return s;
    return `${s} ${unit}`;
  };

  const numCols = previewBatys.length + 3; // label + No.1..N + Max + Min + Avge

  // Per-battery derived values, memoised to avoid recomputation while toggling.
  const perBat = useMemo(() => {
    const out = {};
    previewBatys.forEach((b) => {
      const rows = telemetryMap[b];
      if (!rows) {
        out[b] = { ocv: null, fcv: null, sot: null, ready: false };
        return;
      }
      const stats = statsMap[b] || {};
      const ocv = firstVoltage(rows) ?? safeNum(stats.VOLT_MAX);
      const fcv = lastVoltage(rows) ?? safeNum(stats.VOLT_MIN);
      const sot = computeSotMah(rows);
      out[b] = { ocv, fcv, sot, ready: true };
    });
    return out;
  }, [previewBatys, telemetryMap, statsMap]);

  // Endpoint voltage parsed from archive metadata (used as TAV lower bound).
  const epFromArchive = useMemo(() => {
    const raw = String(archiveFields.endpoint_voltage || '').trim();
    if (!raw) return null;
    const token = raw.split(' ')[0].replace(/[^0-9.\-]+$/g, '');
    return safeNum(token);
  }, [archiveFields.endpoint_voltage]);

  const thresholds = useMemo(
    () => deriveThresholds(telemetryMap, epFromArchive),
    [telemetryMap, epFromArchive],
  );

  const endpointOptions = useMemo(() => [
    { label: t('dm2000DurationEndpointAll'), value: null },
    ...thresholds.map((v) => ({ label: v.toFixed(3), value: v })),
  ], [thresholds, t]);

  const visibleThresholds = useMemo(
    () => (reportEndpoint != null ? thresholds.filter((v) => v >= reportEndpoint - 1e-4) : thresholds),
    [thresholds, reportEndpoint],
  );

  const aggVals = (fn) => previewBatys
    .map((b) => safeNum(fn(b)))
    .filter((v) => v != null);

  const rowAgg = (fn, decimals = 3) => {
    const vals = aggVals(fn);
    if (vals.length === 0) return ['-', '-', '-'];
    return [
      Math.max(...vals).toFixed(decimals),
      Math.min(...vals).toFixed(decimals),
      (vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(decimals),
    ];
  };

  const [ocvMax, ocvMin, ocvAvg] = rowAgg((b) => perBat[b]?.ocv);
  const [fcvMax, fcvMin, fcvAvg] = rowAgg((b) => perBat[b]?.fcv);

  const cellValue = (val) => {
    const n = safeNum(val);
    if (n != null) return n.toFixed(3);
    return val == null ? '…' : '-';
  };

  return (
    <Space direction="vertical" size={8} style={{ width: '100%' }}>
      <Space align="center" wrap>
        <Typography.Text strong>{t('dm2000DurationEndpoint')}:</Typography.Text>
        <Select
          size="small"
          style={{ minWidth: 120 }}
          value={reportEndpoint}
          onChange={setReportEndpoint}
          options={endpointOptions}
        />
      </Space>
      <div style={{ overflowX: 'auto', fontFamily: 'Arial, sans-serif' }}>
        <table style={{ borderCollapse: 'collapse', minWidth: 600 }}>
          <tbody>
            <tr>
              <td colSpan={numCols} style={{ ...cellStyle, textAlign: 'center', fontWeight: 700, fontSize: 15, padding: '6px 8px' }}>
                Battery Discharge Curve
              </td>
            </tr>
            {/* Info rows: left/right pairs */}
            <tr>
              <td style={labelStyle}>{t('dm2000Name')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.name || '-'}</td>
              <td style={labelStyle}>{t('dm2000ArchName')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.archname || '-'}</td>
            </tr>
            <tr>
              <td style={labelStyle}>{t('dm2000Type')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.dcxh || '-'}</td>
              <td style={labelStyle}>{t('dm2000DisCondition')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.discharge_condition || archiveFields.fdfs || '-'}</td>
            </tr>
            <tr>
              <td style={labelStyle}>{t('dm2000VoltageType')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.voltage_type || '-'}</td>
              <td style={labelStyle}>{t('dm2000LoadResistance')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{appendUnit(archiveFields.load_resistance, 'ohm')}</td>
            </tr>
            <tr>
              <td style={labelStyle}>{t('dm2000Trademark')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.trademark || '-'}</td>
              <td style={labelStyle}>{t('dm2000EndpointVoltage')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{appendUnit(archiveFields.endpoint_voltage, 'V')}</td>
            </tr>
            <tr>
              <td style={labelStyle}>{t('dm2000SerialNo')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.serialno || '-'}</td>
              <td style={labelStyle}>{t('dm2000Manufacturer')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.manufacturer || '-'}</td>
            </tr>
            <tr>
              <td style={labelStyle}>{t('dm2000MadeDate')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.madedate || '-'}</td>
              <td style={labelStyle}>{t('dm2000StartDate')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.startdate || '-'}</td>
            </tr>
            <tr>
              <td style={labelStyle}>{t('dm2000MinDuration')}</td>
              <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.min_duration || '-'}</td>
              <td style={labelStyle}>{t('dm2000EndDate')}</td>
              <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.enddate || '-'}</td>
            </tr>
            {/* Battery column headers */}
            <tr>
              <td style={headerCellStyle}></td>
              {previewBatys.map((b) => <td key={b} style={headerCellStyle}>No.{b}</td>)}
              <td style={headerCellStyle}>Max</td>
              <td style={headerCellStyle}>Min</td>
              <td style={headerCellStyle}>Avge</td>
            </tr>
            <tr>
              <td style={labelStyle}>OCV V</td>
              {previewBatys.map((b) => <td key={b} style={cellStyle}>{cellValue(perBat[b]?.ready ? perBat[b].ocv : null)}</td>)}
              <td style={cellStyle}>{ocvMax}</td>
              <td style={cellStyle}>{ocvMin}</td>
              <td style={cellStyle}>{ocvAvg}</td>
            </tr>
            <tr>
              <td style={labelStyle}>CCV V</td>
              {previewBatys.map((b) => <td key={b} style={cellStyle}>{cellValue(perBat[b]?.ready ? perBat[b].fcv : null)}</td>)}
              <td style={cellStyle}>{fcvMax}</td>
              <td style={cellStyle}>{fcvMin}</td>
              <td style={cellStyle}>{fcvAvg}</td>
            </tr>
            <tr>
              <td colSpan={numCols} style={{ ...cellStyle, fontStyle: 'italic', background: '#f0f5ff' }}>
                The Duration of Series Designated Voltage (Unit: times)
              </td>
            </tr>
            {visibleThresholds.map((threshold) => {
              const cellVals = previewBatys.map((b) => {
                const rows = telemetryMap[b];
                if (!rows) return null;
                return countAtVoltage(rows, threshold);
              });
              const numericVals = cellVals.filter((v) => v != null);
              if (numericVals.length === 0 && cellVals.every((v) => v == null)) return null;
              const maxV = numericVals.length > 0 ? Math.max(...numericVals) : '-';
              const minV = numericVals.length > 0 ? Math.min(...numericVals) : '-';
              const avgV = numericVals.length > 0
                ? Math.round(numericVals.reduce((s, v) => s + v, 0) / numericVals.length)
                : '-';
              return (
                <tr key={threshold}>
                  <td style={labelStyle}>{threshold.toFixed(3)}</td>
                  {previewBatys.map((b, idx) => (
                    <td key={b} style={cellStyle}>{cellVals[idx] != null ? cellVals[idx] : '-'}</td>
                  ))}
                  <td style={cellStyle}>{maxV}</td>
                  <td style={cellStyle}>{minV}</td>
                  <td style={cellStyle}>{avgV}</td>
                </tr>
              );
            })}
            <tr>
              <td style={labelStyle}>{t('dm2000Remarks')}</td>
              <td colSpan={numCols - 1} style={cellStyle}>{archiveFields.remarks || '-'}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </Space>
  );
}
