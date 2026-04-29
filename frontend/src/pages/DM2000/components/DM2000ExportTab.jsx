import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert, Button, Card, Checkbox, Col, Empty,
  Form, Input, Row, Select, Space, Spin, Typography, notification,
} from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import {
  downloadDM2000SimpleReport,
  fetchDM2000Batteries, fetchDM2000Config,
  fetchDM2000Stats, fetchDM2000TimeAtVoltage,
  fetchDM2000Options, addDM2000Option,
} from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';
import { useAuth } from '../../../contexts/AuthContext';
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

/** Extract a numeric value from a battery row, trying multiple key spellings. */
function getBatteryField(row, ...keys) {
  for (const k of keys) {
    const v = row?.[k] ?? row?.[k.toUpperCase()] ?? row?.[k.toLowerCase()];
    if (v != null && v !== '' && v !== '--') return v;
  }
  return null;
}

export default function DM2000ExportTab({ stationId, selection }) {
  const { t } = useLang();
  const { isAdmin } = useAuth();
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState('');
  const [exportBatys, setExportBatys] = useState([0]);
  const [batteries, setBatteries] = useState([]);
  /** Raw ls_pam2 rows keyed by baty number */
  const [batteryParams, setBatteryParams] = useState({});
  const [archiveFields, setArchiveFields] = useState({});
  const [previewStats, setPreviewStats] = useState({});
  const [previewTimeAtVolt, setPreviewTimeAtVolt] = useState({});
  const [companyName, setCompanyName] = useState('');
  const [reportEndpoint, setReportEndpoint] = useState(null);
  const [typeOptions, setTypeOptions] = useState([]);
  const [manufacturerOptions, setManufacturerOptions] = useState([]);
  const [newTypeInput, setNewTypeInput] = useState('');
  const [newMfgInput, setNewMfgInput] = useState('');
  // Tracks batteries that already have an in-flight or completed fetch so we
  // don't re-fetch on every toggle and don't depend on previewStats/
  // previewTimeAtVolt inside the fetch effect.
  const requestedBatysRef = useRef(new Set());

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    if (!stationId) {
      setLoading(false);
      return () => {};
    }

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const configResult = await fetchDM2000Config(stationId, { signal: controller.signal }).catch(() => ({}));
        if (!active) return;
        setCompanyName(configResult?.company || '');
      } catch (err) {
        if (!active || err.name === 'AbortError') return;
        setError(err.message || 'Failed to load config');
      } finally {
        if (!active) return;
        setLoading(false);
      }
    };

    load();
    return () => {
      active = false;
      controller.abort();
    };
  }, [stationId]);

  useEffect(() => {
    let active = true;
    fetchDM2000Options('type').then((opts) => {
      if (!active) return;
      setTypeOptions(opts.map((o) => ({ label: o.value, value: o.value })));
    }).catch((err) => { console.error('Failed to load type options', err); });
    fetchDM2000Options('manufacturer').then((opts) => {
      if (!active) return;
      setManufacturerOptions(opts.map((o) => ({ label: o.value, value: o.value })));
    }).catch((err) => { console.error('Failed to load manufacturer options', err); });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    setExportBatys([0]);
    setPreviewStats({});
    setPreviewTimeAtVolt({});
    setBatteryParams({});
    setReportEndpoint(null);
    requestedBatysRef.current = new Set();
    if (selection) {
      // Compose the canonical "Discharge Condition" string. For DM2000, the
      // load resistance, dis-condition cycle (fdfs/dis_condition) and
      // endpoint voltage are stored in separate columns and need to be
      // glued together. composeDischargeCondition() detects when the cycle
      // string is already complete and avoids double-prepending.
      const composed = composeDischargeCondition({
        load: selection.load_resistance || '',
        cycle: selection.fdfs || selection.dis_condition || '',
        endpoint: selection.endpoint_voltage || '',
      });
      setArchiveFields({
        archname: selection.archname || '',
        name: selection.name || '',
        startdate: selection.startdate || '',
        enddate: selection.enddate || '',
        dcxh: selection.dcxh || '',
        voltage_type: selection.voltage_type || '',
        trademark: selection.trademark || '',
        manufacturer: selection.manufacturer || '',
        madedate: selection.madedate || '',
        serialno: selection.serialno || '',
        unifrate: selection.unifrate || '',
        fdfs: selection.fdfs || '',
        load_resistance: selection.load_resistance || '',
        endpoint_voltage: selection.endpoint_voltage || '',
        dis_condition: selection.dis_condition || '',
        discharge_condition: composed,
        min_duration: selection.min_duration || '',
        remarks: selection.remarks || '',
      });
    }
  }, [selection?.archname]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setBatteries([]);
    setBatteryParams({});
    if (!stationId || !selection?.archname) return () => {};

    fetchDM2000Batteries(stationId, selection.archname, { signal: controller.signal })
      .then((rows) => {
        if (!active) return;
        const nums = (rows || [])
          // baty = archname-based schema; gpp = cdid-based schema (para_singl.gpp = pin/position number)
          .map((row) => Number(row?.baty ?? row?.BATY ?? row?.Baty ?? row?.gpp ?? row))
          .filter((v) => Number.isFinite(v) && v > 0);
        setBatteries(nums);
        // Build a map of baty → ls_pam2 row for OCV/FCV/SOt
        const params = {};
        (rows || []).forEach((row) => {
          const b = Number(row?.baty ?? row?.BATY ?? row?.Baty ?? row?.gpp ?? row);
          if (Number.isFinite(b) && b > 0) params[b] = row;
        });
        setBatteryParams(params);
      })
      .catch((err) => {
        if (!active || err.name === 'AbortError') return;
      });
    return () => { active = false; controller.abort(); };
  }, [stationId, selection?.archname]);

  const batteryOptions = useMemo(() => [
    { label: `${t('dm2000BatteryAverage')} (0)`, value: 0 },
    ...batteries.map((b) => ({ label: `${b}#`, value: b })),
  ], [batteries, t]);

  const previewBatys = useMemo(
    () => exportBatys.filter((b) => b > 0),
    [exportBatys],
  );

  // Collect all unique voltage threshold (sj) values available across preview batteries.
  // Used to populate the endpoint dropdown options.
  const endpointOptions = useMemo(() => {
    const sjSet = new Set();
    previewBatys.forEach((b) => {
      const rows = previewTimeAtVolt[b];
      if (!rows) return;
      rows.forEach((r) => {
        const sj = safeNum(r.sj ?? r.SJ);
        if (sj != null) sjSet.add(sj);
      });
    });
    const sorted = Array.from(sjSet).sort((a, b) => b - a); // descending
    return [
      { label: t('dm2000DurationEndpointAll'), value: null },
      ...sorted.map((v) => ({ label: v.toFixed(3), value: v })),
    ];
  }, [previewBatys, previewTimeAtVolt, t]);


  // form fills in incrementally as the user toggles checkboxes.
  useEffect(() => {
    if (!stationId || !selection?.archname || previewBatys.length === 0) return undefined;
    const missing = previewBatys.filter((b) => !requestedBatysRef.current.has(b));
    if (missing.length === 0) return undefined;
    missing.forEach((b) => requestedBatysRef.current.add(b));

    let active = true;
    const controller = new AbortController();
    Promise.all([
      Promise.all(missing.map((baty) =>
        fetchDM2000Stats(stationId, selection.archname, baty, { signal: controller.signal })
          .then((s) => [baty, s || {}])
          .catch((err) => {
            if (err.name === 'AbortError') throw err;
            return [baty, {}];
          }),
      )),
      Promise.all(missing.map((baty) =>
        fetchDM2000TimeAtVoltage(stationId, selection.archname, baty, { signal: controller.signal })
          .then((rows) => [baty, rows || []])
          .catch((err) => {
            if (err.name === 'AbortError') throw err;
            return [baty, []];
          }),
      )),
    ]).then(([statsEntries, timeEntries]) => {
      if (!active) return;
      setPreviewStats((prev) => ({ ...prev, ...Object.fromEntries(statsEntries) }));
      setPreviewTimeAtVolt((prev) => ({ ...prev, ...Object.fromEntries(timeEntries) }));
    }).catch(() => {});
    return () => { active = false; controller.abort(); };
  }, [stationId, selection?.archname, previewBatys]);

  const setField = (key) => (e) => setArchiveFields((prev) => ({ ...prev, [key]: e.target.value }));
  const setSelectField = (key) => (value) => setArchiveFields((prev) => ({ ...prev, [key]: value ?? '' }));

  const handleAddTypeOption = async () => {
    const val = newTypeInput.trim();
    if (!val) return;
    try {
      await addDM2000Option('type', val);
      setTypeOptions((prev) => {
        if (prev.some((o) => o.value === val)) return prev;
        return [...prev, { label: val, value: val }];
      });
      setArchiveFields((prev) => ({ ...prev, dcxh: val }));
      setNewTypeInput('');
    } catch (err) {
      notification.error({ message: err.message });
    }
  };

  const handleAddMfgOption = async () => {
    const val = newMfgInput.trim();
    if (!val) return;
    try {
      await addDM2000Option('manufacturer', val);
      setManufacturerOptions((prev) => {
        if (prev.some((o) => o.value === val)) return prev;
        return [...prev, { label: val, value: val }];
      });
      setArchiveFields((prev) => ({ ...prev, manufacturer: val }));
      setNewMfgInput('');
    } catch (err) {
      notification.error({ message: err.message });
    }
  };

  const handleDownloadPreview = async () => {
    if (!stationId || !selection?.archname) return;
    if (previewBatys.length === 0) {
      notification.warning({ message: t('dm2000SelectAtLeastOne') });
      return;
    }
    setDownloading(true);
    try {
      await downloadDM2000SimpleReport({
        stationId,
        archname: selection.archname,
        batys: previewBatys,
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

  if (!selection) {
    return <Empty description={t('dm2000SelectArchive')} />;
  }

  if (loading) {
    return <Spin />;
  }

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {error && <Alert type="error" showIcon message={error} />}

      <Card size="small" title={t('dm2000SelectBattery')}>
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
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
                <Select
                  style={{ width: '100%' }}
                  value={archiveFields.dcxh || undefined}
                  onChange={setSelectField('dcxh')}
                  allowClear
                  showSearch
                  options={typeOptions}
                  dropdownRender={isAdmin ? (menu) => (
                    <>
                      {menu}
                      <div style={{ display: 'flex', gap: 8, padding: '8px 8px 4px' }}>
                        <Input
                          size="small"
                          value={newTypeInput}
                          onChange={(e) => setNewTypeInput(e.target.value)}
                          onPressEnter={handleAddTypeOption}
                          placeholder={t('dm2000AddOption')}
                          style={{ flex: 1 }}
                        />
                        <Button size="small" type="text" icon={<PlusOutlined />} onClick={handleAddTypeOption} />
                      </div>
                    </>
                  ) : undefined}
                />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={8}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000Manufacturer')}>
                <Select
                  style={{ width: '100%' }}
                  value={archiveFields.manufacturer || undefined}
                  onChange={setSelectField('manufacturer')}
                  allowClear
                  showSearch
                  options={manufacturerOptions}
                  dropdownRender={isAdmin ? (menu) => (
                    <>
                      {menu}
                      <div style={{ display: 'flex', gap: 8, padding: '8px 8px 4px' }}>
                        <Input
                          size="small"
                          value={newMfgInput}
                          onChange={(e) => setNewMfgInput(e.target.value)}
                          onPressEnter={handleAddMfgOption}
                          placeholder={t('dm2000AddOption')}
                          style={{ flex: 1 }}
                        />
                        <Button size="small" type="text" icon={<PlusOutlined />} onClick={handleAddMfgOption} />
                      </div>
                    </>
                  ) : undefined}
                />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={8}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000MadeDate')}>
                <Input value={archiveFields.madedate} onChange={setField('madedate')} />
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
        {previewBatys.length === 0 ? (
          <Typography.Text type="secondary">
            {t('dm2000SelectAtLeastOne')}
          </Typography.Text>
        ) : (
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
            <ReportPreview
              archiveFields={archiveFields}
              companyName={companyName}
              statsMap={previewStats}
              timeAtVoltMap={previewTimeAtVolt}
              batteryParams={batteryParams}
              previewBatys={previewBatys}
              reportEndpoint={reportEndpoint}
              t={t}
            />
          </Space>
        )}
      </Card>

      <Space wrap>
        <Button
          onClick={handleDownloadPreview}
          loading={downloading}
          disabled={!stationId || !selection?.archname || previewBatys.length === 0}
        >
          {t('dm2000DownloadPreview')}
        </Button>
      </Space>

    </Space>
  );
}

function ReportPreview({ archiveFields, companyName, statsMap, timeAtVoltMap, batteryParams, previewBatys, reportEndpoint, t }) {
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

  // Derive voltage thresholds dynamically from the actual time-at-voltage data,
  // sorted descending, so all available rows are shown regardless of range.
  const dynamicThresholds = useMemo(() => {
    const sjSet = new Set();
    previewBatys.forEach((b) => {
      const rows = timeAtVoltMap[b];
      if (!rows) return;
      rows.forEach((r) => {
        const sj = safeNum(r.sj ?? r.SJ);
        if (sj != null) sjSet.add(sj);
      });
    });
    return Array.from(sjSet).sort((a, b) => b - a); // descending
  }, [previewBatys, timeAtVoltMap]);

  // Apply user-selected endpoint cutoff (only show rows where threshold >= cutoff).
  const visibleThresholds = useMemo(
    () => (reportEndpoint != null ? dynamicThresholds.filter((v) => v >= reportEndpoint - 0.0001) : dynamicThresholds),
    [dynamicThresholds, reportEndpoint],
  );

  const getTimeAtVolt = (baty, threshold) => {
    const rows = timeAtVoltMap[baty];
    if (!rows) return '…';
    const row = rows.find((r) => {
      const sj = safeNum(r.sj ?? r.SJ);
      return sj != null && Math.abs(sj - threshold) < 0.001;
    });
    if (!row) return '-';
    // The time-at-voltage endpoint returns values in minutes; the report
    // preview and DM2000 Excel export both display them in hours.
    const val = safeNum(row.minutes ?? row.MINUTES);
    return val != null ? (val / 60).toFixed(3) : '-';
  };

  const fmtStat = (baty, key) => {
    if (!(baty in statsMap)) return '…';
    return fmt(statsMap[baty]?.[key], 3);
  };

  /** Get OCV for a battery: prefer dedicated OCV key from stats (ls_pam2),
   *  fallback to ls_pam2 row from batteries endpoint. */
  const getOcv = (baty) => {
    if (baty in statsMap && statsMap[baty]?.OCV != null) return fmt(statsMap[baty].OCV, 3);
    const row = batteryParams[baty];
    const v = getBatteryField(row, 'ocv', 'OCV');
    return v != null ? fmt(v, 3) : '-';
  };

  /** Get FCV: prefer dedicated FCV key from stats (ls_pam2), fallback to ls_pam2 row. */
  const getFcv = (baty) => {
    if (baty in statsMap && statsMap[baty]?.FCV != null) return fmt(statsMap[baty].FCV, 3);
    const row = batteryParams[baty];
    const v = getBatteryField(row, 'fcv', 'FCV');
    return v != null ? fmt(v, 3) : '-';
  };

  /** Get SOt mAh: try ls_pam2 row first, then compute from time-at-voltage data. */
  const getSot = (baty) => {
    const row = batteryParams[baty];
    const stored = getBatteryField(row, 'sot_mah', 'sot', 'SOT', 'sotmah', 'sh', 'rql', 'capacity');
    if (stored != null) return fmt(stored, 3);
    // Compute from time-at-voltage data when not stored in ls_pam2
    const tav = timeAtVoltMap[baty];
    if (!tav || tav.length === 0) return '-';
    const r = safeNum(archiveFields.load_resistance);
    if (!r || r <= 0) return '-';
    const fcvRaw = statsMap[baty]?.FCV ?? getBatteryField(row, 'fcv', 'FCV');
    const fcv = safeNum(fcvRaw);
    const points = tav
      .map((entry) => ({ v: safeNum(entry.sj ?? entry.SJ), t: safeNum(entry.minutes ?? entry.MINUTES) }))
      .filter((p) => p.v != null && p.t != null && p.t >= 0)
      .sort((a, b) => a.t - b.t);
    if (points.length < 2) return '-';
    let totalMah = 0;
    // Include initial segment from FCV (at t=0) to first threshold
    if (fcv != null && points[0].t > 0 && fcv > points[0].v) {
      const dtH = points[0].t / 60;
      const vAvg = (fcv + points[0].v) / 2;
      totalMah += (vAvg / r) * 1000 * dtH;
    }
    for (let i = 0; i < points.length - 1; i++) {
      const dt = points[i + 1].t - points[i].t;
      if (dt <= 0) continue;
      const vAvg = (points[i].v + points[i + 1].v) / 2;
      totalMah += (vAvg / r) * 1000 * (dt / 60);
    }
    return totalMah > 0 ? fmt(totalMah, 3) : '-';
  };

  // Aggregate helpers
  const aggVals = (fn) => {
    const vals = previewBatys.map((b) => {
      const raw = fn(b);
      return raw == null ? null : safeNum(raw);
    }).filter((v) => v != null);
    return vals;
  };

  const rowAgg = (fn, decimals = 3) => {
    const vals = aggVals(fn);
    if (vals.length === 0) return ['-', '-', '-'];
    return [
      Math.max(...vals).toFixed(decimals),
      Math.min(...vals).toFixed(decimals),
      (vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(decimals),
    ];
  };

  const [ocvMax, ocvMin, ocvAvg] = rowAgg((b) => statsMap[b]?.OCV ?? getBatteryField(batteryParams[b], 'ocv', 'OCV'), 3);
  const [fcvMax, fcvMin, fcvAvg] = rowAgg((b) => statsMap[b]?.FCV ?? getBatteryField(batteryParams[b], 'fcv', 'FCV'), 3);
  const [sotMax, sotMin, sotAvg] = rowAgg((b) => safeNum(getSot(b)), 3);

  /** Compute Uniform Rate = (1 - (Max - Min) / Avg) × 100% at endpoint voltage.
   *  When the user has selected a specific Duration End-point, that voltage is used.
   *  Falls back to the stored archiveFields.endpoint_voltage when no end-point is
   *  selected ("All"), and to archiveFields.unifrate when time data is unavailable
   *  or when the stored value is already a percentage (> 1 after stripping %). */
  const computedUnifRate = useMemo(() => {
    const stored = archiveFields.unifrate || '';
    const storedNum = safeNum(typeof stored === 'string' ? stored.replace('%', '') : stored);
    // yfws is stored as a whole integer in [0, 9]; any other value is a percentage
    const storedIsPct = storedNum != null && !(Number.isInteger(storedNum) && storedNum >= 0 && storedNum <= 9);

    if (!storedIsPct) {
      // Prefer the user-selected end-point; fall back to the archive's endpoint_voltage.
      const ep = reportEndpoint != null
        ? reportEndpoint
        : safeNum(typeof (archiveFields.endpoint_voltage || '') === 'string'
            ? (archiveFields.endpoint_voltage || '').split(' ')[0]
            : archiveFields.endpoint_voltage);
      if (ep != null) {
        const times = previewBatys.map((b) => {
          const rows = timeAtVoltMap[b];
          if (!rows) return null;
          const row = rows.find((r) => {
            const sj = safeNum(r.sj ?? r.SJ);
            return sj != null && Math.abs(sj - ep) < 0.001;
          });
          if (!row) return null;
          const val = safeNum(row.minutes ?? row.MINUTES);
          return val != null && val >= 0 ? val : null;
        }).filter((v) => v != null);

        if (times.length >= 2) {
          const maxT = Math.max(...times);
          const minT = Math.min(...times);
          const avgT = times.reduce((s, v) => s + v, 0) / times.length;
          if (avgT > 0) {
            return `${((1 - (maxT - minT) / avgT) * 100).toFixed(2)} %`;
          }
        }
      }
      return stored || '-';
    }
    return storedNum != null ? `${storedNum.toFixed(2)} %` : stored;
  }, [archiveFields.unifrate, archiveFields.endpoint_voltage, previewBatys, timeAtVoltMap, reportEndpoint]);

  return (
    <div style={{ overflowX: 'auto', fontFamily: 'Arial, sans-serif' }}>
      <table style={{ borderCollapse: 'collapse', minWidth: 600 }}>
        <tbody>
          {/* Title */}
          <tr>
            <td colSpan={numCols} style={{ ...cellStyle, textAlign: 'center', fontWeight: 700, fontSize: 15, padding: '6px 8px' }}>
              Battery Discharge Curve
            </td>
          </tr>
          {/* Company */}
          {companyName && (
            <tr>
              <td colSpan={numCols} style={{ ...cellStyle, textAlign: 'center', fontStyle: 'italic' }}>
                {companyName}
              </td>
            </tr>
          )}
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
            <td style={labelStyle}>{t('dm2000UnifRate')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{computedUnifRate}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000Manufacturer')}</td>
            <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.manufacturer || '-'}</td>
            <td style={labelStyle}>{t('dm2000StartDate')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.startdate || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000MadeDate')}</td>
            <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.madedate || '-'}</td>
            <td style={labelStyle}>{t('dm2000EndDate')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.enddate || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000MinDuration')}</td>
            <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.min_duration || '-'}</td>
            <td style={labelStyle}>{t('dm2000Temperature')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.dis_condition || '-'}</td>
          </tr>
          {/* Measure Instrument row */}
          <tr>
            <td colSpan={numCols} style={{ ...cellStyle, fontStyle: 'italic' }}>
              {`${t('dm2000MeasureInstrument')}: Type DM2000 Automatic Discharge Test System (V6.22)`}
            </td>
          </tr>
          {/* Battery column headers */}
          <tr>
            <td style={headerCellStyle}></td>
            {previewBatys.map((b) => <td key={b} style={headerCellStyle}>No.{b}</td>)}
            <td style={headerCellStyle}>Max</td>
            <td style={headerCellStyle}>Min</td>
            <td style={headerCellStyle}>Avge</td>
          </tr>
          {/* OCV V */}
          <tr>
            <td style={labelStyle}>OCV V</td>
            {previewBatys.map((b) => <td key={b} style={cellStyle}>{getOcv(b)}</td>)}
            <td style={cellStyle}>{ocvMax}</td>
            <td style={cellStyle}>{ocvMin}</td>
            <td style={cellStyle}>{ocvAvg}</td>
          </tr>
          {/* FCV V */}
          <tr>
            <td style={labelStyle}>FCV V</td>
            {previewBatys.map((b) => <td key={b} style={cellStyle}>{getFcv(b)}</td>)}
            <td style={cellStyle}>{fcvMax}</td>
            <td style={cellStyle}>{fcvMin}</td>
            <td style={cellStyle}>{fcvAvg}</td>
          </tr>
          {/* SOt mAh */}
          <tr>
            <td style={labelStyle}>SOt mAh</td>
            {previewBatys.map((b) => <td key={b} style={cellStyle}>{getSot(b)}</td>)}
            <td style={cellStyle}>{sotMax}</td>
            <td style={cellStyle}>{sotMin}</td>
            <td style={cellStyle}>{sotAvg}</td>
          </tr>
          {/* Duration of Series Designated Voltage */}
          <tr>
            <td colSpan={numCols} style={{ ...cellStyle, fontStyle: 'italic', background: '#f0f5ff' }}>
              The Duration of Series Designated Voltage (Unit: hour)
            </td>
          </tr>
          {visibleThresholds.map((threshold) => {
            const cellVals = previewBatys.map((b) => getTimeAtVolt(b, threshold));
            const numericVals = cellVals
              .map((v) => safeNum(v))
              .filter((v) => v != null);
            // Hide rows where no battery has data for this threshold
            if (numericVals.length === 0 && cellVals.every((v) => v === '-')) return null;
            return (
              <tr key={threshold}>
                <td style={labelStyle}>{threshold.toFixed(3)}</td>
                {previewBatys.map((b, idx) => (
                  <td key={b} style={cellStyle}>{cellVals[idx]}</td>
                ))}
                <td style={cellStyle}>{numericVals.length > 0 ? Math.max(...numericVals).toFixed(3) : '-'}</td>
                <td style={cellStyle}>{numericVals.length > 0 ? Math.min(...numericVals).toFixed(3) : '-'}</td>
                <td style={cellStyle}>{numericVals.length > 0 ? (numericVals.reduce((s, v) => s + v, 0) / numericVals.length).toFixed(3) : '-'}</td>
              </tr>
            );
          })}
          {/* Remarks footer */}
          <tr>
            <td style={labelStyle}>{t('dm2000Remarks')}</td>
            <td colSpan={numCols - 1} style={cellStyle}>{archiveFields.remarks || '-'}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}
