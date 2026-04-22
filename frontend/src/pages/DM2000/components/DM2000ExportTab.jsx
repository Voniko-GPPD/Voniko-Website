import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert, Button, Card, Checkbox, Col, Empty,
  Form, Input, Radio, Row, Space, Spin, Typography, notification,
} from 'antd';
import {
  downloadDM2000Report, fetchDM2000Batteries, fetchDM2000Config,
  fetchDM2000Stats, fetchDM2000Templates, fetchDM2000TimeAtVoltage,
} from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const PREVIEW_THRESHOLDS = [1.40, 1.35, 1.30, 1.25, 1.20, 1.15, 1.10, 1.05, 1.00, 0.95, 0.90];

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
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState('');
  const [templates, setTemplates] = useState([]);
  const [templateName, setTemplateName] = useState('');
  const [exportBatys, setExportBatys] = useState([0]);
  const [batteries, setBatteries] = useState([]);
  /** Raw ls_pam2 rows keyed by baty number */
  const [batteryParams, setBatteryParams] = useState({});
  const [archiveFields, setArchiveFields] = useState({});
  const [previewStats, setPreviewStats] = useState({});
  const [previewTimeAtVolt, setPreviewTimeAtVolt] = useState({});
  const [companyName, setCompanyName] = useState('');
  // Tracks batteries that already have an in-flight or completed fetch so we
  // don't re-fetch on every toggle and don't depend on previewStats/
  // previewTimeAtVolt inside the fetch effect.
  const requestedBatysRef = useRef(new Set());

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setTemplates([]);
    setTemplateName('');
    if (!stationId) {
      setLoading(false);
      return () => {};
    }

    const load = async () => {
      setLoading(true);
      setError('');
      try {
        const [templateResult, configResult] = await Promise.all([
          fetchDM2000Templates(stationId, { signal: controller.signal }),
          fetchDM2000Config(stationId, { signal: controller.signal }).catch(() => ({})),
        ]);
        if (!active) return;
        setTemplates(templateResult || []);
        if ((templateResult || []).length > 0) {
          setTemplateName(templateResult[0]);
        }
        setCompanyName(configResult?.company || '');
      } catch (err) {
        if (!active || err.name === 'AbortError') return;
        setError(err.message || 'Failed to load templates');
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
    setExportBatys([0]);
    setPreviewStats({});
    setPreviewTimeAtVolt({});
    setBatteryParams({});
    requestedBatysRef.current = new Set();
    if (selection) {
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

  // Fetch stats / time-at-voltage for any newly selected battery so the preview
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

  const handleDownload = async () => {
    if (!stationId || !selection?.archname || !templateName) return;
    if (exportBatys.length === 0) {
      notification.warning({ message: t('dm2000SelectAtLeastOne') });
      return;
    }
    setDownloading(true);
    try {
      for (const baty of exportBatys) {
        await downloadDM2000Report({
          stationId,
          archname: selection.archname,
          baty,
          templateName,
          overrideArchname: archiveFields.archname !== selection.archname ? archiveFields.archname : undefined,
          overrideStartDate: archiveFields.startdate !== selection.startdate ? archiveFields.startdate : undefined,
          overrideBatteryType: archiveFields.dcxh !== selection.dcxh ? archiveFields.dcxh : undefined,
          overrideManufacturer: archiveFields.manufacturer !== selection.manufacturer ? archiveFields.manufacturer : undefined,
          overrideSerialNo: archiveFields.serialno !== selection.serialno ? archiveFields.serialno : undefined,
          overrideRemarks: archiveFields.remarks !== selection.remarks ? archiveFields.remarks : undefined,
        });
      }
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

      <Card size="small" title={t('dmpExportTab')}>
        {templates.length === 0 ? (
          <Empty description={t('dmpNoTemplates')} />
        ) : (
          <Radio.Group
            value={templateName}
            onChange={(event) => setTemplateName(event.target.value)}
            style={{ width: '100%' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              {templates.map((name) => (
                <Card key={name} size="small">
                  <Radio value={name}>{name}</Radio>
                </Card>
              ))}
            </Space>
          </Radio.Group>
        )}
      </Card>

      <Card size="small" title={t('dm2000EditableFields')}>
        <Row gutter={[16, 8]}>
          <Col xs={24} sm={12}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000ArchName')}>
                <Input value={archiveFields.archname} onChange={setField('archname')} />
              </Form.Item>
              <Form.Item label={t('dm2000Name')}>
                <Input value={archiveFields.name} onChange={setField('name')} />
              </Form.Item>
              <Form.Item label={t('dm2000Type')}>
                <Input value={archiveFields.dcxh} onChange={setField('dcxh')} />
              </Form.Item>
              <Form.Item label={t('dm2000VoltageType')}>
                <Input value={archiveFields.voltage_type} onChange={setField('voltage_type')} />
              </Form.Item>
              <Form.Item label={t('dm2000Trademark')}>
                <Input value={archiveFields.trademark} onChange={setField('trademark')} />
              </Form.Item>
              <Form.Item label={t('dm2000SerialNo')}>
                <Input value={archiveFields.serialno} onChange={setField('serialno')} />
              </Form.Item>
              <Form.Item label={t('dm2000Manufacturer')}>
                <Input value={archiveFields.manufacturer} onChange={setField('manufacturer')} />
              </Form.Item>
              <Form.Item label={t('dm2000MadeDate')}>
                <Input value={archiveFields.madedate} onChange={setField('madedate')} />
              </Form.Item>
              <Form.Item label={t('dm2000MinDuration')}>
                <Input value={archiveFields.min_duration} onChange={setField('min_duration')} />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={12}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000StartDate')}>
                <Input value={archiveFields.startdate} onChange={setField('startdate')} />
              </Form.Item>
              <Form.Item label={t('dm2000EndDate')}>
                <Input value={archiveFields.enddate} onChange={setField('enddate')} />
              </Form.Item>
              <Form.Item label={t('dm2000DisCondition')}>
                <Input value={archiveFields.fdfs} onChange={setField('fdfs')} />
              </Form.Item>
              <Form.Item label={t('dm2000LoadResistance')}>
                <Input value={archiveFields.load_resistance} onChange={setField('load_resistance')} />
              </Form.Item>
              <Form.Item label={t('dm2000EndpointVoltage')}>
                <Input value={archiveFields.endpoint_voltage} onChange={setField('endpoint_voltage')} />
              </Form.Item>
              <Form.Item label={t('dm2000UnifRate')}>
                <Input value={archiveFields.unifrate} onChange={setField('unifrate')} />
              </Form.Item>
              <Form.Item label={t('dm2000Temperature')}>
                <Input value={archiveFields.dis_condition} onChange={setField('dis_condition')} />
              </Form.Item>
              <Form.Item label={t('dm2000Remarks')}>
                <Input value={archiveFields.remarks} onChange={setField('remarks')} />
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
          <ReportPreview
            archiveFields={archiveFields}
            companyName={companyName}
            statsMap={previewStats}
            timeAtVoltMap={previewTimeAtVolt}
            batteryParams={batteryParams}
            previewBatys={previewBatys}
            t={t}
          />
        )}
      </Card>

      <Button
        type="primary"
        onClick={handleDownload}
        loading={downloading}
        disabled={!stationId || !selection?.archname || !templateName || exportBatys.length === 0}
      >
        {`${t('dm2000DownloadReport')} (${exportBatys.length} ${t('dm2000BatteryUnit')})`}
      </Button>
    </Space>
  );
}

function ReportPreview({ archiveFields, companyName, statsMap, timeAtVoltMap, batteryParams, previewBatys, t }) {
  const cellStyle = {
    border: '1px solid #d9d9d9',
    padding: '3px 6px',
    fontSize: 12,
    whiteSpace: 'nowrap',
  };
  const headerCellStyle = { ...cellStyle, background: '#fafafa', fontWeight: 600 };
  const labelStyle = { ...cellStyle, background: '#f5f5f5', color: '#666' };

  const numCols = previewBatys.length + 3; // label + No.1..N + Max + Min + Avge

  const getTimeAtVolt = (baty, threshold) => {
    const rows = timeAtVoltMap[baty];
    if (!rows) return '…';
    const row = rows.find((r) => {
      const sj = safeNum(r.sj ?? r.SJ);
      return sj != null && Math.abs(sj - threshold) < 0.001;
    });
    if (!row) return '-';
    const val = safeNum(row.minutes ?? row.MINUTES);
    return val != null ? val.toFixed(1) : '-';
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

  /** Get SOt mAh from ls_pam2. */
  const getSot = (baty) => {
    const row = batteryParams[baty];
    const v = getBatteryField(row, 'sot', 'SOT', 'sot_mah', 'sotmah', 'sh', 'rql', 'capacity');
    return v != null ? fmt(v, 3) : '-';
  };

  // Aggregate helpers
  const aggVals = (fn) => {
    const vals = previewBatys.map((b) => safeNum(fn(b))).filter((v) => v != null);
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

  const [ocvMax, ocvMin, ocvAvg] = rowAgg((b) => statsMap[b]?.OCV ?? safeNum(getBatteryField(batteryParams[b], 'ocv', 'OCV')), 3);
  const [fcvMax, fcvMin, fcvAvg] = rowAgg((b) => statsMap[b]?.FCV ?? safeNum(getBatteryField(batteryParams[b], 'fcv', 'FCV')), 3);
  const [sotMax, sotMin, sotAvg] = rowAgg((b) => safeNum(getBatteryField(batteryParams[b], 'sot', 'SOT', 'sot_mah', 'sotmah', 'sh', 'rql', 'capacity')), 3);

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
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.fdfs || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000VoltageType')}</td>
            <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.voltage_type || '-'}</td>
            <td style={labelStyle}>{t('dm2000LoadResistance')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.load_resistance || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000Trademark')}</td>
            <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.trademark || '-'}</td>
            <td style={labelStyle}>{t('dm2000EndpointVoltage')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.endpoint_voltage || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000SerialNo')}</td>
            <td colSpan={Math.floor((numCols - 1) / 2)} style={cellStyle}>{archiveFields.serialno || '-'}</td>
            <td style={labelStyle}>{t('dm2000UnifRate')}</td>
            <td colSpan={numCols - Math.floor((numCols - 1) / 2) - 2} style={cellStyle}>{archiveFields.unifrate || '-'}</td>
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
          {PREVIEW_THRESHOLDS.map((threshold) => {
            const cellVals = previewBatys.map((b) => getTimeAtVolt(b, threshold));
            const numericVals = cellVals
              .map((v) => safeNum(v))
              .filter((v) => v != null);
            return (
              <tr key={threshold}>
                <td style={labelStyle}>{threshold.toFixed(3)}</td>
                {previewBatys.map((b, idx) => (
                  <td key={b} style={cellStyle}>{cellVals[idx]}</td>
                ))}
                <td style={cellStyle}>{numericVals.length > 0 ? Math.max(...numericVals).toFixed(1) : '-'}</td>
                <td style={cellStyle}>{numericVals.length > 0 ? Math.min(...numericVals).toFixed(1) : '-'}</td>
                <td style={cellStyle}>{numericVals.length > 0 ? (numericVals.reduce((s, v) => s + v, 0) / numericVals.length).toFixed(1) : '-'}</td>
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
