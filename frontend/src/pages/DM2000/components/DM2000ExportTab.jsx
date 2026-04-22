import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert, Button, Card, Checkbox, Col, Empty,
  Form, Input, Radio, Row, Space, Spin, Typography, notification,
} from 'antd';
import {
  downloadDM2000Report, fetchDM2000Batteries, fetchDM2000Stats,
  fetchDM2000Templates, fetchDM2000TimeAtVoltage,
} from '../../../api/dm2000Api';
import { useLang } from '../../../contexts/LangContext';

const PREVIEW_THRESHOLDS = [1.40, 1.35, 1.30, 1.25, 1.20, 1.15, 1.10, 1.05, 1.00, 0.95, 0.90];

function safeNum(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function fmt(value, decimals = 4) {
  const n = safeNum(value);
  return n != null ? n.toFixed(decimals) : '-';
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
  const [archiveFields, setArchiveFields] = useState({});
  const [previewStats, setPreviewStats] = useState({});
  const [previewTimeAtVolt, setPreviewTimeAtVolt] = useState({});
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
        const result = await fetchDM2000Templates(stationId, { signal: controller.signal });
        if (!active) return;
        setTemplates(result || []);
        if ((result || []).length > 0) {
          setTemplateName(result[0]);
        }
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
    requestedBatysRef.current = new Set();
    if (selection) {
      setArchiveFields({
        archname: selection.archname || '',
        startdate: selection.startdate || '',
        dcxh: selection.dcxh || '',
        manufacturer: selection.manufacturer || '',
        serialno: selection.serialno || '',
        remarks: selection.remarks || '',
      });
    }
  }, [selection?.archname]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setBatteries([]);
    if (!stationId || !selection?.archname) return () => {};

    fetchDM2000Batteries(stationId, selection.archname, { signal: controller.signal })
      .then((rows) => {
        if (!active) return;
        setBatteries(
          (rows || [])
            .map((row) => Number(row?.baty ?? row?.BATY ?? row?.Baty ?? row))
            .filter((v) => Number.isFinite(v) && v > 0),
        );
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
  // form fills in incrementally as the user toggles checkboxes. Already-loaded
  // batteries are tracked in a ref to avoid re-fetching on each toggle.
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
              <Form.Item label={t('dm2000StartDate')}>
                <Input value={archiveFields.startdate} onChange={setField('startdate')} />
              </Form.Item>
              <Form.Item label={t('dm2000SerialNo')}>
                <Input value={archiveFields.serialno} onChange={setField('serialno')} />
              </Form.Item>
            </Form>
          </Col>
          <Col xs={24} sm={12}>
            <Form layout="vertical" size="small">
              <Form.Item label={t('dm2000Type')}>
                <Input value={archiveFields.dcxh} onChange={setField('dcxh')} />
              </Form.Item>
              <Form.Item label={t('dm2000Manufacturer')}>
                <Input value={archiveFields.manufacturer} onChange={setField('manufacturer')} />
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
            statsMap={previewStats}
            timeAtVoltMap={previewTimeAtVolt}
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

function ReportPreview({ archiveFields, statsMap, timeAtVoltMap, previewBatys, t }) {
  const cellStyle = {
    border: '1px solid #d9d9d9',
    padding: '3px 6px',
    fontSize: 12,
    whiteSpace: 'nowrap',
  };
  const headerCellStyle = { ...cellStyle, background: '#fafafa', fontWeight: 600 };
  const labelStyle = { ...cellStyle, background: '#f5f5f5', color: '#666' };

  const getTimeAtVolt = (baty, threshold) => {
    const rows = timeAtVoltMap[baty];
    if (!rows) return '…';
    // Rows from archname-based schema have sj (voltage) + minutes (duration) columns
    // Rows from cdid-based schema have tim_vot{baty} aliased as minutes
    const row = rows.find((r) => {
      const sj = safeNum(r.sj ?? r.SJ);
      return sj != null && Math.abs(sj - threshold) < 0.001;
    });
    if (!row) return '-';
    const val = safeNum(row.minutes ?? row.MINUTES);
    return val != null ? val.toFixed(1) : '-';
  };

  const fmtCell = (baty, key) => {
    if (!(baty in statsMap)) return '…';
    return fmt(statsMap[baty]?.[key]);
  };

  return (
    <div style={{ overflowX: 'auto', fontFamily: 'Arial, sans-serif' }}>
      <table style={{ borderCollapse: 'collapse', minWidth: 600 }}>
        <tbody>
          <tr>
            <td colSpan={previewBatys.length + 3} style={{ ...cellStyle, textAlign: 'center', fontWeight: 700, fontSize: 15, padding: '6px 8px' }}>
              Battery Discharge Curve
            </td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000ArchName')}</td>
            <td colSpan={2} style={cellStyle}>{archiveFields.archname || '-'}</td>
            <td style={labelStyle}>{t('dm2000StartDate')}</td>
            <td colSpan={Math.max(previewBatys.length - 1, 1)} style={cellStyle}>{archiveFields.startdate || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000Type')}</td>
            <td colSpan={2} style={cellStyle}>{archiveFields.dcxh || '-'}</td>
            <td style={labelStyle}>{t('dm2000Manufacturer')}</td>
            <td colSpan={Math.max(previewBatys.length - 1, 1)} style={cellStyle}>{archiveFields.manufacturer || '-'}</td>
          </tr>
          <tr>
            <td style={labelStyle}>{t('dm2000SerialNo')}</td>
            <td colSpan={2} style={cellStyle}>{archiveFields.serialno || '-'}</td>
            <td style={labelStyle}>{t('dm2000Remarks')}</td>
            <td colSpan={Math.max(previewBatys.length - 1, 1)} style={cellStyle}>{archiveFields.remarks || '-'}</td>
          </tr>
          <tr>
            <td style={headerCellStyle}></td>
            {previewBatys.map((b) => <td key={b} style={headerCellStyle}>No.{b}</td>)}
            <td style={headerCellStyle}>Max</td>
            <td style={headerCellStyle}>Min</td>
            <td style={headerCellStyle}>Avge</td>
          </tr>
          {['VOLT_MAX', 'VOLT_MIN', 'VOLT_AVG', 'DURATION_MIN'].map((key) => {
            const label = { VOLT_MAX: 'VOLT MAX (V)', VOLT_MIN: 'VOLT MIN (V)', VOLT_AVG: 'VOLT AVG (V)', DURATION_MIN: 'Duration (min)' }[key];
            const vals = previewBatys.map((b) => safeNum(statsMap[b]?.[key])).filter((v) => v != null);
            return (
              <tr key={key}>
                <td style={labelStyle}>{label}</td>
                {previewBatys.map((b) => <td key={b} style={cellStyle}>{fmtCell(b, key)}</td>)}
                <td style={cellStyle}>{vals.length > 0 ? Math.max(...vals).toFixed(4) : '-'}</td>
                <td style={cellStyle}>{vals.length > 0 ? Math.min(...vals).toFixed(4) : '-'}</td>
                <td style={cellStyle}>{vals.length > 0 ? (vals.reduce((s, v) => s + v, 0) / vals.length).toFixed(4) : '-'}</td>
              </tr>
            );
          })}
          <tr>
            <td colSpan={previewBatys.length + 3} style={{ ...cellStyle, fontStyle: 'italic', background: '#f0f5ff' }}>
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
          <tr>
            <td style={labelStyle}>{t('dm2000Remarks')}</td>
            <td colSpan={previewBatys.length + 2} style={cellStyle}>{archiveFields.remarks || '-'}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}
