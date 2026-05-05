import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Card, Form, Select, Input, InputNumber, DatePicker, Button, Table, Tabs,
  Badge, notification, Tooltip, Space, Row, Col, Divider, Tag, Checkbox,
  Upload, Collapse, Modal, Popover, Radio,
} from 'antd';
import {
  ReloadOutlined, DownloadOutlined, DeleteOutlined, PlayCircleOutlined,
  StopOutlined, DisconnectOutlined, ApiOutlined, InboxOutlined, QuestionCircleOutlined,
  ExportOutlined, FullscreenOutlined, InfoCircleOutlined, PlusOutlined,
} from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import dayjs from 'dayjs';
import { useLang } from '../../contexts/LangContext';
import { uploadTemplate, getTemplateInfo, downloadReportFromTemplate, uploadArchive, getArchiveInfo, downloadArchiveReport, getStations, getBatteryTypes, createBatteryType, deleteBatteryType, getBatteryProductLines, createBatteryProductLine, deleteBatteryProductLine, getBatteryPresets, upsertBatteryPreset, deleteBatteryPreset, getOrderHistory, saveOrderHistorySnapshot, deleteOrderHistorySnapshot, clearOrderHistory } from '../../api/battery';

const { Option } = Select;
const { RangePicker } = DatePicker;

const STATUS_COLORS = {
  'Waiting...': '#8c8c8c',
  'Testing...': '#1677ff',
  'Done': '#52c41a',
  'Remove': '#52c41a',
  'Saving...': '#d48806',
  'Stopped': '#8c8c8c',
  'Error': '#f5222d',
};

function getStatusColor(text) {
  if (!text) return '#595959';
  for (const [key, color] of Object.entries(STATUS_COLORS)) {
    if (text.includes(key)) return color;
  }
  return '#595959';
}

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const Y_AXIS_PADDING_RATIO = 0.1;
const MIN_Y_AXIS_PADDING = 0.01;
const ZOOM_MODAL_TABLE_SCROLL_Y = 'calc(80vh - 120px)';
const ZOOM_CHART_DATA_ZOOM = [{ type: 'inside', filterMode: 'none' }, { type: 'slider', height: 20, bottom: 4 }];

function parseStandard(str) {
  if (!str || !str.trim()) return null;
  const cleaned = str.replace(/\+\/-/g, '±').replace(/\s/g, '');
  const matchFull = cleaned.match(/^([0-9.]+)±([0-9.]+)$/);
  if (matchFull) {
    return { center: parseFloat(matchFull[1]), tolerance: parseFloat(matchFull[2]) };
  }
  const matchSimple = cleaned.match(/^([0-9.]+)$/);
  if (matchSimple) {
    return { center: parseFloat(matchSimple[1]), tolerance: 0 };
  }
  return null;
}

function getInitialSession() {
  try {
    return JSON.parse(localStorage.getItem('battery_session') || '{}');
  } catch {
    return {};
  }
}

function normalizeOrderId(value) {
  return String(value || '').trim().toLowerCase();
}

function makePresetKey(batteryType, productLine) {
  return `${batteryType}_${productLine}`;
}

function mapPresetsResponse(presets) {
  const presetsMap = {};
  for (const p of (presets || [])) {
    presetsMap[makePresetKey(p.battery_type, p.product_line)] = {
      batteryType: p.battery_type,
      productLine: p.product_line,
      resistance: p.resistance,
      ocvTime: p.ocv_time,
      loadTime: p.load_time,
      kCoeff: p.k_coeff,
      ocvMin: p.ocv_min,
      ocvMax: p.ocv_max,
      ccvMin: p.ccv_min,
      ccvMax: p.ccv_max,
      diaMin: p.dia_min ?? null,
      diaMax: p.dia_max ?? null,
      heiMin: p.hei_min ?? null,
      heiMax: p.hei_max ?? null,
    };
  }
  return presetsMap;
}

function buildSnapshotSignature(snapshot) {
  return JSON.stringify({
    orderId: normalizeOrderId(snapshot.orderId),
    testDate: snapshot.testDate || null,
    batteryType: snapshot.batteryType || '',
    productLine: snapshot.productLine || '',
    records: snapshot.records || [],
    chartSeriesByBattery: snapshot.chartSeriesByBattery || {},
    readingsByBattery: snapshot.readingsByBattery || {},
  });
}

function dedupeOrderHistory(items) {
  const uniqueSnapshots = new Map();
  (items || []).forEach((snapshot) => {
    const key = normalizeOrderId(snapshot?.orderId) || snapshot?._snapshotId;
    if (!key) return;
    const existing = uniqueSnapshots.get(key);
    const currentTime = dayjs(snapshot?._savedAt).valueOf() || 0;
    const existingTime = dayjs(existing?._savedAt).valueOf() || 0;
    if (!existing || currentTime >= existingTime) {
      uniqueSnapshots.set(key, snapshot);
    }
  });
  return Array.from(uniqueSnapshots.values())
    .sort((left, right) => (dayjs(right?._savedAt).valueOf() || 0) - (dayjs(left?._savedAt).valueOf() || 0))
    .slice(0, 50);
}

function RowWithPopover({ record, readingsByBattery, chartSeriesByBattery, buildMiniChartOption, ...rowProps }) {
  const seriesByBattery = record && chartSeriesByBattery && chartSeriesByBattery[record.id];
  const hasReadings = record && (
    (readingsByBattery && (readingsByBattery[record.id] || []).length > 0) ||
    ((seriesByBattery?.ocv || []).length > 0 || (seriesByBattery?.ccv || []).length > 0)
  );
  if (!hasReadings) {
    return <tr {...rowProps} />;
  }
  const popoverContent = (
    <div style={{ width: 900, borderRadius: 6, padding: 4, border: '1px solid #303030', background: '#141414' }}>
      <ReactECharts
        option={buildMiniChartOption(record.id)}
        style={{ height: 450, width: 900 }}
        notMerge
      />
    </div>
  );
  return (
    <Popover
      content={popoverContent}
      overlayInnerStyle={{ padding: 0 }}
      overlayStyle={{ maxWidth: 940 }}
      placement="left"
      mouseEnterDelay={0.3}
    >
      <tr {...rowProps} />
    </Popover>
  );
}

export default function BatteryPage() {
  const { t } = useLang();

  // Connection state
  const [ports, setPorts] = useState([]);
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);

  // Test state
  const [running, setRunning] = useState(false);

  // Form params
  const [port, setPort] = useState('');
  const [baudRate, setBaudRate] = useState(115200);
  const [simMode, setSimMode] = useState(false);
  const [orderId, setOrderId] = useState(() => getInitialSession().orderId || '');
  const [testDate, setTestDate] = useState(() => {
    const saved = getInitialSession();
    return saved.testDate ? dayjs(saved.testDate) : dayjs();
  });
  const [resistance, setResistance] = useState(3.9);
  const [ocvTime, setOcvTime] = useState(1);
  const [loadTime, setLoadTime] = useState(0.3);
  const [kCoeff, setKCoeff] = useState(1.0);
  const [batteryType, setBatteryType] = useState(() => getInitialSession().batteryType || 'LR6');
  const [productLine, setProductLine] = useState(() => getInitialSession().productLine || 'UD+');
  const [ocvMin, setOcvMin] = useState(() => getInitialSession().ocvMin ?? null);
  const [ocvMax, setOcvMax] = useState(() => getInitialSession().ocvMax ?? null);
  const [ccvMin, setCcvMin] = useState(() => getInitialSession().ccvMin ?? null);
  const [ccvMax, setCcvMax] = useState(() => getInitialSession().ccvMax ?? null);
  const [diaMin, setDiaMin] = useState(() => getInitialSession().diaMin ?? null);
  const [diaMax, setDiaMax] = useState(() => getInitialSession().diaMax ?? null);
  const [heiMin, setHeiMin] = useState(() => getInitialSession().heiMin ?? null);
  const [heiMax, setHeiMax] = useState(() => getInitialSession().heiMax ?? null);
  const [presets, setPresets] = useState({});
  const [batteryTypes, setBatteryTypes] = useState([]);
  const [productLines, setProductLines] = useState([]);
  const [setupForm, setSetupForm] = useState({
    batteryType: '', productLine: '',
    resistance: 3.9, ocvTime: 1.0, loadTime: 0.3, kCoeff: 1.0,
    ocvMin: null, ocvMax: null, ccvMin: null, ccvMax: null,
    diaMin: null, diaMax: null, heiMin: null, heiMax: null,
  });
  const [newTypeName, setNewTypeName] = useState('');
  const [newLineName, setNewLineName] = useState('');
  const [typeLineLoading, setTypeLineLoading] = useState(false);
  const [typeSearchText, setTypeSearchText] = useState('');
  const [lineSearchText, setLineSearchText] = useState('');

  // Display
  const [statusText, setStatusText] = useState('Waiting...');
  const [statusColor, setStatusColor] = useState('#ffffff');

  // Chart
  const [chartData, setChartData] = useState(() => getInitialSession().chartData || []);
  const [chartDataOCV, setChartDataOCV] = useState(() => getInitialSession().chartDataOCV || []);
  const [chartDataCCV, setChartDataCCV] = useState(() => getInitialSession().chartDataCCV || []);
  const [chartSeriesByBattery, setChartSeriesByBattery] = useState(() => getInitialSession().chartSeriesByBattery || {});
  const [autoScroll, setAutoScroll] = useState(true);
  const [legendSelected, setLegendSelected] = useState({ OCV: true, CCV: true });
  const voltChartRef = useRef(null);

  // Results
  const [records, setRecords] = useState(() => getInitialSession().records || []);

  // Out-of-spec blocking modal
  const [outOfSpecModal, setOutOfSpecModal] = useState(null);

  // Session started flag — locks all parameters once the first test is started.
  // Only reset when "Xóa phiên làm việc" is used to begin a new order.
  const [sessionStarted, setSessionStarted] = useState(() => getInitialSession().sessionStarted || false);

  // Tracks the battery ID currently being retested (for chart display)
  const [retestingBatteryId, setRetestingBatteryId] = useState(null);

  // Physical dimensions (caliper)
  const [caliperPhase, setCaliperPhase] = useState(false); // true after OCV/CCV is done
  const [caliperSingleMode, setCaliperSingleMode] = useState(false); // true when measuring only one battery
  const [caliperDia, setCaliperDia] = useState('');
  const [caliperHei, setCaliperHei] = useState('');
  const [caliperBuffer, setCaliperBuffer] = useState('');
  const [caliperMode, setCaliperMode] = useState('dia'); // 'dia' | 'hei'
  const [caliperIndex, setCaliperIndex] = useState(0); // index of battery currently being measured
  const caliperInputRef = useRef(null);
  // Refs to access current caliper values inside WS callback without stale closures
  const caliperDiaRef = useRef('');
  const caliperHeiRef = useRef('');
  const caliperIndexRef = useRef(0);
  const caliperSingleModeRef = useRef(false);
  const recordsLengthRef = useRef(0);
  const recordsRef = useRef([]);
  useEffect(() => { caliperDiaRef.current = caliperDia; }, [caliperDia]);
  useEffect(() => { caliperHeiRef.current = caliperHei; }, [caliperHei]);
  useEffect(() => { caliperIndexRef.current = caliperIndex; }, [caliperIndex]);
  useEffect(() => { caliperSingleModeRef.current = caliperSingleMode; }, [caliperSingleMode]);
  useEffect(() => { recordsLengthRef.current = records.length; recordsRef.current = records; }, [records]);

  // Readings grouped by battery id for mini chart popover
  const [readingsByBattery, setReadingsByBattery] = useState(() => getInitialSession().readingsByBattery || {});

  // Order history — each entry is a full order snapshot saved on session clear
  const [orderHistory, setOrderHistory] = useState([]);
  const [orderHistoryLoading, setOrderHistoryLoading] = useState(false);
  const [chartZoomVisible, setChartZoomVisible] = useState(false);
  const [tableZoomVisible, setTableZoomVisible] = useState(false);

  useEffect(() => {
    setOrderHistoryLoading(true);
    getOrderHistory().then((res) => {
      setOrderHistory(dedupeOrderHistory(res.data.items || []));
    }).catch((e) => {
      notification.error({ message: 'Không thể tải lịch sử đơn hàng', description: e?.message });
    }).finally(() => setOrderHistoryLoading(false));
  }, []);

  // Order history UI state
  const [pageTab, setPageTab] = useState('test');
  const [historySearchOrder, setHistorySearchOrder] = useState('');
  const [historyTypeFilter, setHistoryTypeFilter] = useState('');
  const [historyLineFilter, setHistoryLineFilter] = useState('');
  const [historyDateRange, setHistoryDateRange] = useState(null);
  const [loadedSnapshotId, setLoadedSnapshotId] = useState(null);
  const loadedSnapshotIdRef = useRef(null);
  const loadedSnapshotSignatureRef = useRef(null);
  const loadedSnapshotOrderIdRef = useRef('');

  // Resume session modal
  const [resumeModalVisible, setResumeModalVisible] = useState(false);
  const [savedSessionInfo, setSavedSessionInfo] = useState(null);

  // Duplicate order ID blocking modal (shown when user types a code that already exists in history)
  const [duplicateOrderWarningVisible, setDuplicateOrderWarningVisible] = useState(false);
  const [duplicateMatchingSnapshot, setDuplicateMatchingSnapshot] = useState(null);

  // Excel report template
  const [templateName, setTemplateName] = useState(() => localStorage.getItem('battery_template_name') || null);
  const [downloadingTemplate, setDownloadingTemplate] = useState(false);

  // Archive
  const [archiveName, setArchiveName] = useState(() => localStorage.getItem('battery_archive_name') || null);
  const [downloadingArchive, setDownloadingArchive] = useState(false);

  // Station selection
  const [stations, setStations] = useState([]);
  const [selectedStation, setSelectedStation] = useState(null);
  const [stationsLoading, setStationsLoading] = useState(false);
  const selectedStationRef = useRef(null);
  useEffect(() => { selectedStationRef.current = selectedStation; }, [selectedStation]);
  useEffect(() => { loadedSnapshotIdRef.current = loadedSnapshotId; }, [loadedSnapshotId]);

  // WebSocket
  const wsRef = useRef(null);
  const retryCountRef = useRef(0);
  const retryTimerRef = useRef(null);
  const mountedRef = useRef(true);
  const pendingNewSessionRef = useRef(false);
  // Holds a record to retest after the current test stops (used by "Đo lại ngay")
  const pendingRetestRecordRef = useRef(null);
  // Flag: after a retest completes, auto-restart the full test from the next battery
  const autoRestartAfterRetestRef = useRef(false);
  // Maps batteryId -> number of retests done (for "đo X lần không đạt" display)
  const retestCountMapRef = useRef({});
  const orderIdRef = useRef(orderId);
  useEffect(() => { orderIdRef.current = orderId; }, [orderId]);
  const batteryTypeRef = useRef(batteryType);
  useEffect(() => { batteryTypeRef.current = batteryType; }, [batteryType]);
  const productLineRef = useRef(productLine);
  useEffect(() => { productLineRef.current = productLine; }, [productLine]);

  // Poll station list every 30s
  useEffect(() => {
    const fetchStations = async () => {
      setStationsLoading(true);
      try {
        const res = await getStations();
        setStations(res.data.stations || []);
      } catch (_) { }
      finally { setStationsLoading(false); }
    };
    fetchStations();
    const interval = setInterval(fetchStations, 30_000);
    return () => clearInterval(interval);
  }, []);

  // Caliper HID input handler — captures keystrokes from USB wireless receiver
  useEffect(() => {
    const handleCaliperKey = (e) => {
      // Skip if caliper phase is not active
      if (!caliperPhase) return;
      // Skip if a real input/textarea/select is focused
      const tag = e.target.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

      if (e.key === 'Enter') {
        const val = parseFloat(caliperBuffer.replace(',', '.'));
        if (!isNaN(val) && val > 0) {
          if (caliperMode === 'dia') {
            setCaliperDia(val.toFixed(2));
            setCaliperMode('hei'); // auto switch to next measurement
          } else {
            // Hei done — auto-save to current record index
            const heiVal = val.toFixed(2);
            const idx = caliperIndexRef.current;
            const diaVal = caliperDiaRef.current;
            const savedDia = diaVal !== '' ? parseFloat(diaVal) : null;
            const savedHei = parseFloat(heiVal);
            setRecords(prev => {
              if (idx >= prev.length) return prev;
              const updated = [...prev];
              const rec = { ...updated[idx] };
              if (savedDia !== null) rec.dia = savedDia;
              rec.hei = savedHei;
              updated[idx] = rec;
              return updated;
            });

            // Dim out-of-spec check
            const spec_dia = diaSpecRef.current;
            const spec_hei = heiSpecRef.current;
            const diaBad = spec_dia && savedDia !== null && (savedDia < spec_dia.min || savedDia > spec_dia.max);
            const heiBad = spec_hei && (savedHei < spec_hei.min || savedHei > spec_hei.max);
            if (diaBad || heiBad) {
              const parts = [];
              if (diaBad) parts.push(`Đường kính ${savedDia.toFixed(2)} mm (chuẩn: ${spec_dia.min} – ${spec_dia.max})`);
              if (heiBad) parts.push(`Chiều cao ${savedHei.toFixed(2)} mm (chuẩn: ${spec_hei.min} – ${spec_hei.max})`);
              const currentRecord = recordsRef.current[idx];
              setCaliperBuffer('');
              setCaliperDia('');
              setCaliperHei('');
              setCaliperMode('dia');
              setCaliperPhase(false);
              setOutOfSpecModal({
                record: currentRecord,
                parts,
                retryCount: 0,
                type: 'dim',
                batIdx: idx,
                wasInSingleMode: caliperSingleModeRef.current,
              });
              setCaliperSingleMode(false);
              return;
            }

            if (caliperSingleModeRef.current) {
              // Single mode — stop after measuring one battery
              setCaliperPhase(false);
              setCaliperBuffer('');
              setCaliperDia('');
              setCaliperHei('');
              setCaliperMode('dia');
              setCaliperSingleMode(false);
            } else {
              const nextIdx = idx + 1;
              setCaliperIndex(nextIdx);
              setCaliperDia('');
              setCaliperHei('');
              setCaliperMode('dia');
              if (nextIdx >= recordsLengthRef.current) {
                // All batteries measured
                setCaliperPhase(false);
                setCaliperBuffer('');
              }
            }
          }
        }
        setCaliperBuffer('');
      } else if (e.key === 'Escape') {
        setCaliperBuffer('');
      } else if (e.key === 'Backspace' || e.key === 'Delete') {
        setCaliperBuffer((prev) => prev.slice(0, -1));
      } else if (/^[\d]$/.test(e.key)) {
        setCaliperBuffer((prev) => prev + e.key);
      } else if ((e.key === '.' || e.key === ',') && !/[.,]/.test(caliperBuffer)) {
        // Only allow one decimal separator
        setCaliperBuffer((prev) => prev + e.key);
      }
    };
    window.addEventListener('keydown', handleCaliperKey);
    return () => window.removeEventListener('keydown', handleCaliperKey);
  }, [caliperBuffer, caliperMode, caliperPhase]);

  const buildParams = useCallback(() => ({
    order_id: orderId,
    date: testDate ? testDate.format('YYYY-MM-DD') : dayjs().format('YYYY-MM-DD'),
    resistance: parseFloat(resistance),
    ocv_time: parseFloat(ocvTime),
    load_time: parseFloat(loadTime),
    coeff: parseFloat(kCoeff),
    battery_type: batteryType,
    product_line: productLine,
    ocv_standard_min: ocvMin,
    ocv_standard_max: ocvMax,
    ccv_standard_min: ccvMin,
    ccv_standard_max: ccvMax,
  }), [orderId, testDate, resistance, ocvTime, loadTime, kCoeff, batteryType, productLine, ocvMin, ocvMax, ccvMin, ccvMax]);

  const sendMsg = useCallback((msg) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ ...msg, stationId: selectedStationRef.current?.id }));
    }
  }, []);

  const handleWsMessage = useCallback((event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }

    switch (msg.type) {
      case 'ports':
        setPorts(msg.ports || []);
        break;

      case 'connect_result':
        setConnecting(false);
        if (msg.ok) {
          setConnected(true);
          notification.success({ message: t('batteryConnectSuccess'), description: msg.message });
        } else {
          setConnected(false);
          notification.error({ message: t('batteryConnectFailed'), description: msg.message });
        }
        break;

      case 'disconnected':
        setConnected(false);
        setRunning(false);
        setStatusText('Waiting...');
        setStatusColor('#ffffff');
        break;

      case 'test_started':
        setRunning(true);
        setStatusText('Testing...');
        setStatusColor(getStatusColor('Testing...'));
        notification.info({ message: t('batteryTestStarted') });
        // Clear accumulated chart data when starting a fresh test (not a retest).
        // Without this, new readings for battery IDs 1, 2, … are appended to the
        // old data from the previous run.  Because each measurement resets elapsed
        // time to 0, the combined array becomes non-monotonic and ECharts draws a
        // chaotic zigzag line.
        if (retestingBatteryIdRef.current === null) {
          setChartData([]);
          setChartDataOCV([]);
          setChartDataCCV([]);
          setChartSeriesByBattery({});
          setReadingsByBattery({});
        }
        break;

      case 'test_stopped':
        setRunning(false);
        setRetestingBatteryId(null);
        setStatusText('Stopped');
        setStatusColor(getStatusColor('Stopped'));
        // Suppress the "stopped" notification when an immediate retest is pending
        if (!pendingRetestRecordRef.current) {
          notification.info({ message: t('batteryTestStopped') });
        }
        break;

      case 'reading':
        if (msg.elapsed !== undefined && msg.voltage !== undefined) {
          setChartData((prev) => [...prev, [msg.elapsed, msg.voltage]]);
          if (msg.phase === 'ocv') {
            setChartDataOCV((prev) => [...prev, [msg.elapsed, msg.voltage]]);
          } else if (msg.phase === 'ccv') {
            setChartDataCCV((prev) => [...prev, [msg.elapsed, msg.voltage]]);
          }
          if (msg.battery_id !== undefined) {
            const bid = msg.battery_id;
            setReadingsByBattery((prev) => {
              const id = bid;
              const list = prev[id] || [];
              return { ...prev, [id]: [...list, { t: msg.elapsed, v: msg.voltage, phase: msg.phase }] };
            });
            setChartSeriesByBattery((prev) => {
              const entry = prev[bid] || { ocv: [], ccv: [] };
              const point = [msg.elapsed, msg.voltage];
              if (msg.phase === 'ocv') {
                return { ...prev, [bid]: { ...entry, ocv: [...entry.ocv, point] } };
              } else if (msg.phase === 'ccv') {
                return { ...prev, [bid]: { ...entry, ccv: [...entry.ccv, point] } };
              }
              return prev;
            });
          }
        }
        break;

      case 'record':
        if (msg.record) {
          const dia = caliperDiaRef.current || null;
          const hei = caliperHeiRef.current || null;
          const enrichedRecord = { ...msg.record, dia: dia ? parseFloat(dia) : null, hei: hei ? parseFloat(hei) : null };
          setRecords((prev) => {
            const idx = prev.findIndex((r) => r.id === msg.record.id);
            if (idx >= 0) {
              const updated = [...prev];
              updated[idx] = enrichedRecord;
              return updated;
            }
            return [...prev, enrichedRecord];
          });
          // Reset caliper values after saving to record
          setCaliperDia('');
          setCaliperHei('');
          // Out-of-spec check — runs for every record (new or updated, retest or not).
          // Uses refs to avoid stale-closure issues inside the WS callback.
          {
            const rec = enrichedRecord;
            const spec_ocv = ocvSpecRef.current;
            const spec_ccv = ccvSpecRef.current;
            const ocvBad = spec_ocv && rec.ocv != null && (rec.ocv < spec_ocv.min || rec.ocv > spec_ocv.max);
            const ccvBad = spec_ccv && rec.ccv != null && (rec.ccv < spec_ccv.min || rec.ccv > spec_ccv.max);
            if (ocvBad || ccvBad) {
              const parts = [];
              if (ocvBad) parts.push(`OCV ${rec.ocv.toFixed(3)}V (spec: ${spec_ocv.min} - ${spec_ocv.max})`);
              if (ccvBad) parts.push(`CCV ${rec.ccv.toFixed(3)}V (spec: ${spec_ccv.min} - ${spec_ccv.max})`);
              if (msg.record.is_retest) {
                // When "Đo lại ngay" triggered this retest (auto-restart pending),
                // skip the modal — auto-restart will continue to the next battery
                // regardless of whether the retested battery is within spec or not.
                if (!autoRestartAfterRetestRef.current) {
                  const retryCount = retestCountMapRef.current[rec.id] || 0;
                  setOutOfSpecModal({ record: rec, parts, retryCount });
                }
              } else {
                setOutOfSpecModal({ record: rec, parts, retryCount: 0 });
                if (runningRef.current) {
                  sendMsg({ action: 'stop' });
                }
              }
            }
          }
        }
        break;

      case 'status':
        if (msg.text) {
          setStatusText(msg.text);
          setStatusColor(getStatusColor(msg.text));
          // When the hardware naturally stops (e.g. after retest completes), also update running state
          if (msg.text === 'Stopped') {
            setRunning(false);
            setRetestingBatteryId(null);
          }
        } else if (msg.data) {
          const text = msg.data.status_text || 'Waiting...';
          setStatusText(text);
          setStatusColor(getStatusColor(text));
          if (msg.data.records) setRecords(msg.data.records);
        }
        break;

      case 'session_cleared':
        setChartData([]);
        setChartDataOCV([]);
        setChartDataCCV([]);
        setChartSeriesByBattery({});
        setRecords([]);
        setReadingsByBattery({});
        setRetestingBatteryId(null);
        setSessionStarted(false);
        pendingRetestRecordRef.current = null;
        autoRestartAfterRetestRef.current = false;
        retestCountMapRef.current = {};
        notification.success({ message: t('batterySessionCleared') });
        break;

      case 'error':
        notification.error({ message: t('error'), description: msg.message });
        break;

      default:
        break;
    }
  }, [t]);

  const connectWs = useCallback(() => {
    if (!mountedRef.current) return;
    const token = localStorage.getItem('accessToken') || '';
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const url = `${protocol}://${window.location.host}/ws/battery?token=${encodeURIComponent(token)}`;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      retryCountRef.current = 0;
      // Only fetch ports if a station is already selected
      if (selectedStationRef.current?.id) {
        ws.send(JSON.stringify({ action: 'get_ports', stationId: selectedStationRef.current.id }));
      }
      if (pendingNewSessionRef.current) {
        try {
          ws.send(JSON.stringify({ action: 'clear_session', stationId: selectedStationRef.current?.id }));
          pendingNewSessionRef.current = false;
        } catch {
          // flag remains true; will retry on next connection
        }
      }
    };

    ws.onmessage = handleWsMessage;

    ws.onclose = (evt) => {
      if (!mountedRef.current) return;
      // If closed unexpectedly and we were connected/running, attempt reconnect
      if (evt.code !== 1000 && retryCountRef.current < MAX_RETRIES) {
        retryCountRef.current += 1;
        retryTimerRef.current = setTimeout(connectWs, RETRY_DELAY_MS);
      }
    };

    ws.onerror = () => {
      // error will be followed by close
    };
  }, [handleWsMessage]);

  useEffect(() => {
    mountedRef.current = true;
    connectWs();
    return () => {
      mountedRef.current = false;
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
      if (wsRef.current) {
        wsRef.current.close(1000, 'unmount');
      }
    };
  }, [connectWs]);

  // Refresh port list
  const handleRefreshPorts = () => {
    sendMsg({ action: 'get_ports' });
  };

  // Connect to device
  const handleConnect = () => {
    if (!simMode && !port) {
      notification.warning({ message: t('batterySelectPort') });
      return;
    }
    setConnecting(true);
    sendMsg({
      action: 'connect',
      payload: { port: simMode ? null : port, baud_rate: baudRate, simulation: simMode },
    });
  };

  // Disconnect
  const handleDisconnect = () => {
    sendMsg({ action: 'disconnect' });
  };

  // Start / stop test
  const handleStartStop = () => {
    if (running) {
      sendMsg({ action: 'stop' });
    } else {
      setSessionStarted(true);
      sendMsg({ action: 'start', payload: buildParams() });
    }
  };

  // Retest a specific record — clear old chart data first so only fresh data is shown
  const handleRetest = useCallback((record) => {
    setChartSeriesByBattery(prev => ({ ...prev, [record.id]: { ocv: [], ccv: [] } }));
    setReadingsByBattery(prev => ({ ...prev, [record.id]: [] }));
    setRetestingBatteryId(record.id);
    sendMsg({ action: 'start', payload: { ...buildParams(), retest_id: record.id } });
  }, [sendMsg, buildParams]);

  // When a pending retest was queued (via "Đo lại ngay" while test was running),
  // trigger it as soon as the test stops.
  useEffect(() => {
    if (!running && pendingRetestRecordRef.current) {
      const record = pendingRetestRecordRef.current;
      pendingRetestRecordRef.current = null;
      handleRetest(record);
    }
  }, [running, handleRetest]);

  // After a retest completes, auto-restart the full test from the next battery.
  // Only fires when: test is stopped, auto-restart flag is set, no other retest is pending,
  // and no out-of-spec modal is blocking (if the modal is shown the restart waits for user choice).
  useEffect(() => {
    if (!running && autoRestartAfterRetestRef.current && !pendingRetestRecordRef.current && outOfSpecModal === null) {
      autoRestartAfterRetestRef.current = false;
      setRetestingBatteryId(null);
      sendMsg({ action: 'start', payload: buildParams() });
    }
  }, [running, outOfSpecModal, sendMsg, buildParams]);

  // Store latest chart data in refs so saveCurrentOrderSnapshot can access without stale closure
  const chartSeriesByBatteryRef = useRef(chartSeriesByBattery);
  const readingsByBatteryRef = useRef(readingsByBattery);
  const testDateRef = useRef(testDate);
  // Ref to read retestingBatteryId inside the stale-closure handleWsMessage callback
  const retestingBatteryIdRef = useRef(retestingBatteryId);
  useEffect(() => { chartSeriesByBatteryRef.current = chartSeriesByBattery; }, [chartSeriesByBattery]);
  useEffect(() => { readingsByBatteryRef.current = readingsByBattery; }, [readingsByBattery]);
  useEffect(() => { testDateRef.current = testDate; }, [testDate]);
  useEffect(() => { retestingBatteryIdRef.current = retestingBatteryId; }, [retestingBatteryId]);

  const resetLoadedSnapshotTracking = useCallback(() => {
    loadedSnapshotIdRef.current = null;
    loadedSnapshotSignatureRef.current = null;
    loadedSnapshotOrderIdRef.current = '';
    setLoadedSnapshotId(null);
  }, []);

  // Save order snapshot with latest chart/reading data
  const saveCurrentOrderSnapshot = useCallback(() => {
    const snap_records = recordsRef.current;
    if (!snap_records || snap_records.length === 0) return;
    const normalizedOrderId = normalizeOrderId(orderIdRef.current);
    if (!normalizedOrderId) return;
    const snapshot = {
      orderId: orderIdRef.current?.trim() || '',
      testDate: testDateRef.current ? testDateRef.current.format('YYYY-MM-DD') : null,
      batteryType: batteryTypeRef.current,
      productLine: productLineRef.current,
      records: snap_records,
      chartSeriesByBattery: chartSeriesByBatteryRef.current,
      readingsByBattery: readingsByBatteryRef.current,
    };
    const currentSignature = buildSnapshotSignature(snapshot);
    if (
      loadedSnapshotIdRef.current
      && loadedSnapshotSignatureRef.current === currentSignature
      && normalizeOrderId(loadedSnapshotOrderIdRef.current) === normalizedOrderId
    ) {
      return;
    }

    const payload = {
      _snapshotId: loadedSnapshotIdRef.current || undefined,
      ...snapshot,
    };
    saveOrderHistorySnapshot(payload).then((res) => {
      const savedId = res.data.id;
      const savedAt = res.data.savedAt;
      const isUpdated = res.data.updated === true;
      setOrderHistory(prev => {
        const entry = {
          _snapshotId: savedId,
          _savedAt: savedAt,
          _status: isUpdated ? 'updated' : 'new',
          ...snapshot,
        };
        const next = dedupeOrderHistory([
          entry,
          ...prev.filter((item) => normalizeOrderId(item.orderId) !== normalizedOrderId),
        ]);
        return next;
      });
      if (!loadedSnapshotIdRef.current) {
        loadedSnapshotIdRef.current = savedId;
      }
    }).catch((e) => {
      notification.error({ message: 'Không thể lưu lịch sử đơn hàng', description: e?.message });
    });
  }, []);

  // Load an order snapshot back into current session
  const handleLoadOrder = useCallback((snapshot) => {
    setRecords(snapshot.records || []);
    setChartSeriesByBattery(snapshot.chartSeriesByBattery || {});
    setReadingsByBattery(snapshot.readingsByBattery || {});
    // Rebuild flat chart data from chartSeriesByBattery for legacy chart fallback
    const allOcv = [];
    const allCcv = [];
    Object.values(snapshot.chartSeriesByBattery || {}).forEach(bat => {
      (bat.ocv || []).forEach(p => allOcv.push(p));
      (bat.ccv || []).forEach(p => allCcv.push(p));
    });
    setChartDataOCV(allOcv);
    setChartDataCCV(allCcv);
    setChartData([...allOcv, ...allCcv]);
    setOrderId(snapshot.orderId || '');
    setTestDate(snapshot.testDate ? dayjs(snapshot.testDate) : dayjs());
    setBatteryType(snapshot.batteryType || 'LR6');
    setProductLine(snapshot.productLine || 'UD+');
    loadedSnapshotIdRef.current = snapshot._snapshotId;
    loadedSnapshotOrderIdRef.current = snapshot.orderId || '';
    loadedSnapshotSignatureRef.current = buildSnapshotSignature(snapshot);
    setLoadedSnapshotId(snapshot._snapshotId);
    notification.success({ message: `Đã tải lại đơn hàng: ${snapshot.orderId || '-'}` });
  }, []);

  // Delete a specific order snapshot from history
  const handleDeleteOrderSnapshot = useCallback((snapshotId) => {
    deleteOrderHistorySnapshot(snapshotId).then(() => {
      setOrderHistory(prev => dedupeOrderHistory(prev.filter(s => s._snapshotId !== snapshotId)));
      if (loadedSnapshotIdRef.current === snapshotId) {
        resetLoadedSnapshotTracking();
      }
    }).catch((e) => {
      notification.error({ message: 'Không thể xóa đơn hàng', description: e?.message });
    });
  }, [resetLoadedSnapshotTracking]);

  // Clear session (save snapshot first, then clear)
  const handleClearSession = useCallback(() => {
    saveCurrentOrderSnapshot();
    sendMsg({ action: 'clear_session' });
    localStorage.removeItem('battery_session');
    setReadingsByBattery({});
    setChartSeriesByBattery({});
    setSessionStarted(false);
    resetLoadedSnapshotTracking();
  }, [saveCurrentOrderSnapshot, sendMsg, resetLoadedSnapshotTracking]);

  // Skip current battery (advance without saving dimensions)
  const handleSaveCaliper = useCallback(() => {
    if (caliperSingleMode) {
      // Single mode: just exit
      setCaliperPhase(false);
      setCaliperDia('');
      setCaliperHei('');
      setCaliperBuffer('');
      setCaliperMode('dia');
      setCaliperSingleMode(false);
      return;
    }
    const nextIdx = caliperIndex + 1;
    setCaliperIndex(nextIdx);
    setCaliperDia('');
    setCaliperHei('');
    setCaliperBuffer('');
    setCaliperMode('dia');
    if (nextIdx >= records.length) {
      setCaliperPhase(false);
    }
  }, [caliperSingleMode, caliperIndex, records.length]);

  const handleResetCaliper = useCallback(() => {
    setCaliperPhase(false);
    setCaliperDia('');
    setCaliperHei('');
    setCaliperBuffer('');
  }, []);

  // Jump the caliper to the battery with the given ID (used by both onPressEnter and onBlur)
  const handleCaliperIdJump = useCallback((rawValue) => {
    const inputId = parseInt(rawValue, 10);
    if (!isNaN(inputId) && inputId >= (recordsRef.current[0]?.id ?? 1) && inputId <= (recordsRef.current[recordsRef.current.length - 1]?.id ?? 1)) {
      const idx = recordsRef.current.findIndex(r => r.id === inputId);
      if (idx >= 0) {
        caliperIndexRef.current = idx; // Update ref immediately so caliper hardware events see the new index
        setCaliperIndex(idx);
        setCaliperDia('');
        setCaliperHei('');
        setCaliperMode('dia');
      }
    }
  }, []);

  // Template upload handler
  const handleTemplateUpload = async ({ file, onSuccess, onError }) => {
    const formData = new FormData();
    formData.append('template', file);
    try {
      await uploadTemplate(formData);
      setTemplateName(file.name);
      localStorage.setItem('battery_template_name', file.name);
      notification.success({ message: t('batteryTemplateUploaded') });
      onSuccess();
    } catch (e) {
      notification.error({ message: t('batteryTemplateUploadFailed'), description: e.message });
      onError(e);
    }
  };

  // Archive upload handler
  const handleArchiveUpload = async ({ file, onSuccess, onError }) => {
    const formData = new FormData();
    formData.append('archive', file);
    try {
      await uploadArchive(formData);
      setArchiveName(file.name);
      localStorage.setItem('battery_archive_name', file.name);
      notification.success({ message: t('batteryArchiveUploaded') });
      onSuccess();
    } catch (e) {
      notification.error({ message: t('batteryArchiveUploadFailed'), description: e.message });
      onError(e);
    }
  };

  // Download report from template
  const handleDownloadTemplateReport = async () => {
    setDownloadingTemplate(true);
    try {
      const response = await downloadReportFromTemplate(records);
      const blob = new Blob([response.data], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      const date = testDate ? testDate.format('YYYY-MM-DD') : dayjs().format('YYYY-MM-DD');
      link.download = `battery_report_${orderId}_${date}.xlsx`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      notification.success({ message: t('batteryDownloadSuccess') });
    } catch (e) {
      const status = e.response?.status;
      if (status === 404) {
        notification.warning({ message: t('batteryTemplateNotFound') });
      } else {
        let errMsg = e.message;
        if (e.response?.data instanceof Blob) {
          try {
            const text = await e.response.data.text();
            const parsed = JSON.parse(text);
            errMsg = parsed.error || parsed.detail || errMsg;
          } catch (_parseErr) { /* blob is not JSON, keep original message */ }
        }
        notification.error({ message: t('batteryDownloadFailed'), description: errMsg });
      }
    } finally {
      setDownloadingTemplate(false);
    }
  };

  // Download archive report
  const handleDownloadArchiveReport = async () => {
    setDownloadingArchive(true);
    try {
      const response = await downloadArchiveReport(records);
      const blob = new Blob([response.data], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      const date = testDate ? testDate.format('YYYY-MM-DD') : dayjs().format('YYYY-MM-DD');
      link.download = `battery_archive_${orderId}_${date}.xlsx`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      notification.success({ message: t('batteryDownloadSuccess') });
    } catch (e) {
      const status = e.response?.status;
      if (status === 404) {
        notification.warning({ message: t('batteryArchiveNotFound') });
      } else {
        let errMsg = e.message;
        if (e.response?.data instanceof Blob) {
          try {
            const text = await e.response.data.text();
            const parsed = JSON.parse(text);
            errMsg = parsed.error || parsed.detail || errMsg;
          } catch (_parseErr) { /* blob is not JSON, keep original message */ }
        }
        notification.error({ message: t('batteryDownloadFailed'), description: errMsg });
      }
    } finally {
      setDownloadingArchive(false);
    }
  };

  // ECharts option — show only the latest battery being measured (OCV + CCV connected)
  const allBatteryIds = Object.keys(chartSeriesByBattery).map(Number).sort((a, b) => a - b);
  // During active measurement: show only the battery currently being tested (or retested).
  // When not actively running (loaded snapshot, stopped session): show nothing in the left chart.
  const latestBatteryIds = running
    ? (retestingBatteryId !== null
        ? [retestingBatteryId]
        : (allBatteryIds.length > 0 ? [allBatteryIds[allBatteryIds.length - 1]] : []))
    : [];
  const chartSeries = [];
  latestBatteryIds.forEach((bid, idx) => {
    const { ocv = [], ccv = [] } = chartSeriesByBattery[bid];
    const isFirst = idx === 0;
    chartSeries.push({
      name: 'OCV',
      type: 'line',
      data: ocv,
      symbol: 'none',
      lineStyle: { color: '#ffee58', width: 2 },
      markArea: isFirst && ocv.length > 0 && ocvTime > 0 ? {
        silent: true,
        data: [[
          { name: 'OCV', xAxis: 0, itemStyle: { color: 'rgba(255,238,88,0.08)' } },
          { xAxis: ocvTime },
        ]],
      } : undefined,
    });
    const ccvConnected = ocv.length > 0 && ccv.length > 0
      ? [ocv[ocv.length - 1], ...ccv]
      : ccv;
    chartSeries.push({
      name: 'CCV',
      type: 'line',
      data: ccvConnected,
      symbol: 'none',
      lineStyle: { color: '#0091ea', width: 2 },
      areaStyle: isFirst ? { color: 'rgba(0,145,234,0.08)' } : undefined,
      markArea: isFirst && ocv.length > 0 && ocvTime > 0 ? {
        silent: true,
        data: [[
          { name: 'Load', xAxis: ocvTime, itemStyle: { color: 'rgba(0,229,255,0.06)' } },
          { xAxis: ocvTime + loadTime },
        ]],
      } : undefined,
    });
  });
  // Fallback to legacy flat data when chartSeriesByBattery is empty (e.g. resumed session without per-battery data)
  if (running && allBatteryIds.length === 0 && (chartDataOCV.length > 0 || chartDataCCV.length > 0)) {
    chartSeries.push({
      name: 'OCV',
      type: 'line',
      data: chartDataOCV,
      symbol: 'none',
      lineStyle: { color: '#ffee58', width: 2 },
      markArea: (chartDataOCV.length > 0 || chartDataCCV.length > 0) && ocvTime > 0 ? {
        silent: true,
        data: [[
          { name: 'OCV', xAxis: 0, itemStyle: { color: 'rgba(255,238,88,0.08)' } },
          { xAxis: ocvTime },
        ]],
      } : undefined,
    });
    chartSeries.push({
      name: 'CCV',
      type: 'line',
      data: chartDataOCV.length > 0 && chartDataCCV.length > 0
        ? [chartDataOCV[chartDataOCV.length - 1], ...chartDataCCV]
        : chartDataCCV,
      symbol: 'none',
      lineStyle: { color: '#0091ea', width: 2 },
      areaStyle: { color: 'rgba(0,145,234,0.08)' },
      markArea: (chartDataOCV.length > 0 || chartDataCCV.length > 0) && ocvTime > 0 ? {
        silent: true,
        data: [[
          { name: 'Load', xAxis: ocvTime, itemStyle: { color: 'rgba(0,229,255,0.06)' } },
          { xAxis: ocvTime + loadTime },
        ]],
      } : undefined,
    });
  }
  // Compute explicit Y-axis bounds from only the visible series so that hiding
  // either OCV or CCV always triggers a proper rescale (ECharts' built-in
  // scale:true is unreliable when the first-defined series is toggled off).
  const yAxisScale = React.useMemo(() => {
    const ocvVisible = legendSelected['OCV'] !== false;
    const visibleYValues = chartSeries
      .filter(s => legendSelected[s.name] !== false)
      .flatMap(s => {
        const data = s.data || [];
        // When OCV is hidden, skip the first point of each CCV series — it's the
        // connector point from OCV and would artificially expand the Y range.
        const effectiveData = (!ocvVisible && s.name === 'CCV' && data.length > 1)
          ? data.slice(1)
          : data;
        return effectiveData
          .map(d => (Array.isArray(d) ? d[1] : (typeof d === 'number' ? d : null)))
          .filter(v => v != null && !isNaN(v) && isFinite(v));
      });
    if (visibleYValues.length > 0) {
      const yMin = Math.min(...visibleYValues);
      const yMax = Math.max(...visibleYValues);
      const range = yMax - yMin;
      const pad = range * Y_AXIS_PADDING_RATIO || MIN_Y_AXIS_PADDING;
      return { min: +(yMin - pad).toFixed(4), max: +(yMax + pad).toFixed(4) };
    }
    return { scale: true };
  }, [chartSeries, legendSelected]);

  const chartOption = {
    animation: false,
    backgroundColor: '#141414',
    grid: { top: 36, right: 24, bottom: 40, left: 56 },
    legend: {
      top: 4,
      data: ['OCV', 'CCV'],
      selected: legendSelected,
      textStyle: { color: '#d0d0d0', fontSize: 12 },
    },
    tooltip: { trigger: 'axis', formatter: (params) => params.map(p => `${p.marker}${p.seriesName}: ${p.value[1]?.toFixed(3)} V @ ${p.value[0]}s`).join('<br/>') },
    xAxis: {
      type: 'value',
      name: 's',
      nameLocation: 'end',
      axisLabel: { color: '#aaa' },
      axisLine: { lineStyle: { color: '#444' } },
      splitLine: { lineStyle: { color: '#2a2a2a' } },
    },
    yAxis: {
      type: 'value',
      name: 'V',
      nameLocation: 'end',
      axisLabel: { color: '#aaa' },
      axisLine: { lineStyle: { color: '#444' } },
      splitLine: { lineStyle: { color: '#2a2a2a' } },
      ...yAxisScale,
    },
    dataZoom: autoScroll
      ? [{ type: 'inside', filterMode: 'none' }]
      : [{ type: 'inside' }, { type: 'slider', height: 20, bottom: 4 }],
    series: chartSeries,
  };

  // Results table columns
  const columns = [
    {
      title: t('batteryId'),
      dataIndex: 'id',
      key: 'id',
      width: 60,
      render: (id) => <span>{id}</span>,
    },
    {
      title: t('batteryOcv'), dataIndex: 'ocv', key: 'ocv', width: 90,
      render: (v) => {
        const bad = ocvSpec && v != null && (v < ocvSpec.min || v > ocvSpec.max);
        return <span style={{ color: bad ? '#ff4d4f' : undefined, fontWeight: bad ? 700 : undefined }}>{v != null ? v.toFixed(3) : '-'}</span>;
      },
    },
    {
      title: t('batteryCcv'), dataIndex: 'ccv', key: 'ccv', width: 90,
      render: (v) => {
        const bad = ccvSpec && v != null && (v < ccvSpec.min || v > ccvSpec.max);
        return <span style={{ color: bad ? '#ff4d4f' : undefined, fontWeight: bad ? 700 : undefined }}>{v != null ? v.toFixed(3) : '-'}</span>;
      },
    },
    { title: t('batteryTime'), dataIndex: 'time', key: 'time', width: 80, render: (v) => v != null ? String(v) : '-' },
    {
      title: t('batteryCaliperDia'), dataIndex: 'dia', key: 'dia', width: 80,
      render: (v) => {
        const bad = diaSpec && v != null && (v < diaSpec.min || v > diaSpec.max);
        return <span style={{ color: bad ? '#ff4d4f' : undefined, fontWeight: bad ? 700 : undefined }}>{v != null ? parseFloat(v).toFixed(2) : '-'}</span>;
      },
    },
    {
      title: t('batteryCaliperHei'), dataIndex: 'hei', key: 'hei', width: 80,
      render: (v) => {
        const bad = heiSpec && v != null && (v < heiSpec.min || v > heiSpec.max);
        return <span style={{ color: bad ? '#ff4d4f' : undefined, fontWeight: bad ? 700 : undefined }}>{v != null ? parseFloat(v).toFixed(2) : '-'}</span>;
      },
    },
    {
      title: t('actions'),
      key: 'actions',
      width: 120,
      render: (_, record) => {
        const recordIdx = records.findIndex(r => r.id === record.id);
        return (
          <Space size={4}>
            <Button size="small" onClick={() => handleRetest(record)} disabled={!connected || running}>
              {t('batteryRetest')}
            </Button>
            <Button
              size="small"
              icon={<span>📏</span>}
              onClick={() => {
                const recordIdx = records.findIndex(r => r.id === record.id);
                const idx = recordIdx >= 0 ? recordIdx : 0;
                caliperIndexRef.current = idx; // Update ref immediately
                setCaliperIndex(idx);
                setCaliperSingleMode(true);
                setCaliperPhase(true);
                setCaliperMode('dia');
                setCaliperDia('');
                setCaliperHei('');
              }}
              disabled={running}
              title={t('batteryCaliperSection')}
            />
          </Space>
        );
      },
    },
  ];

  // Preset handlers
  const handleSavePreset = useCallback(async () => {
    if (!setupForm.batteryType || !setupForm.productLine) {
      notification.warning({ message: t('batterySelectTypeAndLine') });
      return;
    }
    if (setupForm.ocvMin == null || setupForm.ocvMax == null || setupForm.ccvMin == null || setupForm.ccvMax == null) {
      notification.warning({ message: t('batteryFillRequiredFields') });
      return;
    }
    try {
      await upsertBatteryPreset({
        batteryType: setupForm.batteryType,
        productLine: setupForm.productLine,
        resistance: setupForm.resistance,
        ocvTime: setupForm.ocvTime,
        loadTime: setupForm.loadTime,
        kCoeff: setupForm.kCoeff,
        ocvMin: setupForm.ocvMin,
        ocvMax: setupForm.ocvMax,
        ccvMin: setupForm.ccvMin,
        ccvMax: setupForm.ccvMax,
        diaMin: setupForm.diaMin,
        diaMax: setupForm.diaMax,
        heiMin: setupForm.heiMin,
        heiMax: setupForm.heiMax,
      });
      const res = await getBatteryPresets();
      setPresets(mapPresetsResponse(res.data.presets));
      notification.success({ message: t('batterySetupSaved') });
    } catch (e) {
      notification.error({ message: e?.response?.data?.error || e.message });
    }
  }, [setupForm, t]);

  const handleDeletePreset = useCallback(async (batteryTypeVal, productLineVal) => {
    try {
      await deleteBatteryPreset(batteryTypeVal, productLineVal);
      const res = await getBatteryPresets();
      setPresets(mapPresetsResponse(res.data.presets));
    } catch (e) {
      notification.error({ message: e?.response?.data?.error || e.message });
    }
  }, []);

  const handleAddBatteryType = useCallback(async () => {
    if (!newTypeName.trim()) return;
    setTypeLineLoading(true);
    try {
      await createBatteryType(newTypeName.trim());
      const res = await getBatteryTypes();
      setBatteryTypes(res.data.types || []);
      setNewTypeName('');
      notification.success({ message: t('batteryTypeAdded') });
    } catch (e) {
      notification.error({ message: e?.response?.status === 409 ? t('batteryTypeExists') : (e?.response?.data?.error || e.message) });
    } finally { setTypeLineLoading(false); }
  }, [newTypeName, t]);

  const handleAddBatteryLine = useCallback(async () => {
    if (!newLineName.trim()) return;
    setTypeLineLoading(true);
    try {
      await createBatteryProductLine(newLineName.trim());
      const res = await getBatteryProductLines();
      setProductLines(res.data.productLines || []);
      setNewLineName('');
      notification.success({ message: t('batteryLineAdded') });
    } catch (e) {
      notification.error({ message: e?.response?.status === 409 ? t('batteryLineExists') : (e?.response?.data?.error || e.message) });
    } finally { setTypeLineLoading(false); }
  }, [newLineName, t]);

  const handleCreateAndSelectType = useCallback(async (name) => {
    if (!name.trim()) return;
    setTypeLineLoading(true);
    try {
      await createBatteryType(name.trim());
      const res = await getBatteryTypes();
      setBatteryTypes(res.data.types || []);
      setBatteryType(name.trim());
      setTypeSearchText('');
    } catch (e) {
      if (e?.response?.status === 409) {
        setBatteryType(name.trim());
        setTypeSearchText('');
      } else {
        notification.error({ message: e?.response?.data?.error || e.message });
      }
    } finally { setTypeLineLoading(false); }
  }, []);

  const handleCreateAndSelectLine = useCallback(async (name) => {
    if (!name.trim()) return;
    setTypeLineLoading(true);
    try {
      await createBatteryProductLine(name.trim());
      const res = await getBatteryProductLines();
      setProductLines(res.data.productLines || []);
      setProductLine(name.trim());
      setLineSearchText('');
    } catch (e) {
      if (e?.response?.status === 409) {
        setProductLine(name.trim());
        setLineSearchText('');
      } else {
        notification.error({ message: e?.response?.data?.error || e.message });
      }
    } finally { setTypeLineLoading(false); }
  }, []);

  const ocvSpec = React.useMemo(() => {
    const min = parseFloat(ocvMin);
    const max = parseFloat(ocvMax);
    return (!isNaN(min) && !isNaN(max)) ? { min, max } : null;
  }, [ocvMin, ocvMax]);
  const ccvSpec = React.useMemo(() => {
    const min = parseFloat(ccvMin);
    const max = parseFloat(ccvMax);
    return (!isNaN(min) && !isNaN(max)) ? { min, max } : null;
  }, [ccvMin, ccvMax]);

  // Refs for ocvSpec/ccvSpec and running — used inside WS callbacks to avoid stale closures
  const runningRef = useRef(running);
  useEffect(() => { runningRef.current = running; }, [running]);
  const ocvSpecRef = useRef(ocvSpec);
  useEffect(() => { ocvSpecRef.current = ocvSpec; }, [ocvSpec]);
  const ccvSpecRef = useRef(ccvSpec);
  useEffect(() => { ccvSpecRef.current = ccvSpec; }, [ccvSpec]);

  const diaSpec = React.useMemo(() => {
    const min = parseFloat(diaMin);
    const max = parseFloat(diaMax);
    return (!isNaN(min) && !isNaN(max)) ? { min, max } : null;
  }, [diaMin, diaMax]);
  const heiSpec = React.useMemo(() => {
    const min = parseFloat(heiMin);
    const max = parseFloat(heiMax);
    return (!isNaN(min) && !isNaN(max)) ? { min, max } : null;
  }, [heiMin, heiMax]);
  const diaSpecRef = useRef(diaSpec);
  useEffect(() => { diaSpecRef.current = diaSpec; }, [diaSpec]);
  const heiSpecRef = useRef(heiSpec);
  useEffect(() => { heiSpecRef.current = heiSpec; }, [heiSpec]);

  const recordsMap = React.useMemo(() => {
    const map = {};
    records.forEach((r) => { map[String(r.id)] = r; });
    return map;
  }, [records]);

  const filteredOrderHistory = React.useMemo(() => {
    const search = historySearchOrder.trim().toLowerCase();
    return orderHistory.filter((snapshot) => {
      if (historyTypeFilter && snapshot.batteryType !== historyTypeFilter) return false;
      if (historyLineFilter && snapshot.productLine !== historyLineFilter) return false;
      if (search) {
        const haystack = [snapshot.orderId, snapshot.batteryType, snapshot.productLine]
          .filter(Boolean)
          .join(' ')
          .toLowerCase();
        if (!haystack.includes(search)) return false;
      }
      if (historyDateRange?.[0] && historyDateRange?.[1]) {
        const savedAt = dayjs(snapshot._savedAt);
        const rangeStart = historyDateRange[0].startOf('day');
        const rangeEnd = historyDateRange[1].endOf('day');
        if (!savedAt.isValid() || savedAt.valueOf() < rangeStart.valueOf() || savedAt.valueOf() > rangeEnd.valueOf()) {
          return false;
        }
      }
      return true;
    });
  }, [
    orderHistory,
    historySearchOrder,
    historyTypeFilter,
    historyLineFilter,
    historyDateRange,
  ]);

  const handleExportOrderSnapshot = useCallback((snapshot) => {
    const headers = ['ID', 'OCV (V)', 'CCV (V)', 'Time (s)', 'Dia (mm)', 'Hei (mm)', 'Status'];
    const rows = (snapshot.records || []).map((record) => [
      record.id ?? '',
      record.ocv != null ? record.ocv.toFixed(3) : '',
      record.ccv != null ? record.ccv.toFixed(3) : '',
      record.time != null ? String(record.time) : '',
      record.dia != null ? record.dia.toFixed(2) : '',
      record.hei != null ? record.hei.toFixed(2) : '',
      record.status || '',
    ]);
    const escape = (value) => String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
    const xmlRows = [headers, ...rows].map((row) =>
      `<Row>${row.map((cell) => `<Cell><Data ss:Type="String">${escape(cell)}</Data></Cell>`).join('')}</Row>`
    ).join('');
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
  <Worksheet ss:Name="Order">
    <Table>${xmlRows}</Table>
  </Worksheet>
</Workbook>`;
    const blob = new Blob([xml], { type: 'application/vnd.ms-excel;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `order_${snapshot.orderId || 'unknown'}_${dayjs(snapshot._savedAt).format('YYYY-MM-DD')}.xls`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }, []);

  const historyDetailColumns = React.useMemo(() => ([
    { title: t('batteryId'), dataIndex: 'id', key: 'id', width: 60 },
    { title: t('batteryOcv'), dataIndex: 'ocv', key: 'ocv', width: 90, render: (value) => value != null ? value.toFixed(3) : '-' },
    { title: t('batteryCcv'), dataIndex: 'ccv', key: 'ccv', width: 90, render: (value) => value != null ? value.toFixed(3) : '-' },
    { title: t('batteryTime'), dataIndex: 'time', key: 'time', width: 80, render: (value) => value != null ? String(value) : '-' },
    { title: t('batteryCaliperDia'), dataIndex: 'dia', key: 'dia', width: 90, render: (value) => value != null ? parseFloat(value).toFixed(2) : '-' },
    { title: t('batteryCaliperHei'), dataIndex: 'hei', key: 'hei', width: 90, render: (value) => value != null ? parseFloat(value).toFixed(2) : '-' },
  ]), [t]);

  const historySummaryColumns = React.useMemo(() => ([
    {
      title: t('batteryHistorySavedAt'),
      dataIndex: '_savedAt',
      key: '_savedAt',
      width: 170,
      defaultSortOrder: 'descend',
      sorter: (left, right) => dayjs(left._savedAt).valueOf() - dayjs(right._savedAt).valueOf(),
      render: (value) => value ? dayjs(value).format('DD/MM/YYYY HH:mm') : '-',
    },
    {
      title: t('batteryOrderId'),
      dataIndex: 'orderId',
      key: 'orderId',
      width: 160,
      render: (value, snapshot) => (
        <Space size={4} direction="vertical">
          <strong>{value || '-'}</strong>
          {snapshot._status === 'updated' && <Tag color="orange" style={{ fontSize: 11 }}>{t('batteryHistoryStatusUpdated')}</Tag>}
        </Space>
      ),
    },
    {
      title: t('batteryTestMonth'),
      dataIndex: 'testDate',
      key: 'testDate',
      width: 120,
      render: (value) => value ? dayjs(value).format('DD/MM/YYYY') : '-',
    },
    {
      title: t('batteryType'),
      dataIndex: 'batteryType',
      key: 'batteryType',
      width: 100,
      render: (value) => value || '-',
    },
    {
      title: t('batteryProductLine'),
      dataIndex: 'productLine',
      key: 'productLine',
      width: 120,
      render: (value) => value || '-',
    },
    {
      title: t('batteryHistoryCount'),
      key: 'count',
      width: 100,
      render: (_, snapshot) => <Tag color="blue">{(snapshot.records || []).length}</Tag>,
    },
    {
      title: t('actions'),
      key: 'actions',
      width: 180,
      render: (_, snapshot) => (
        <Space size={4} wrap>
          <Button
            size="small"
            type={loadedSnapshotId === snapshot._snapshotId ? 'primary' : 'default'}
            disabled={!selectedStation || !connected}
            onClick={() => handleLoadOrder(snapshot)}
          >
            {loadedSnapshotId === snapshot._snapshotId ? t('batteryHistoryViewing') : t('batteryHistoryLoad')}
          </Button>
          <Button
            size="small"
            icon={<ExportOutlined />}
            onClick={() => handleExportOrderSnapshot(snapshot)}
            title={t('batteryHistoryExportExcel')}
          />
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => {
              Modal.confirm({
                title: `${t('delete')} ${snapshot.orderId || ''}?`,
                okText: t('delete'),
                cancelText: t('cancel'),
                okButtonProps: { danger: true },
                onOk: () => handleDeleteOrderSnapshot(snapshot._snapshotId),
              });
            }}
          />
        </Space>
      ),
    },
  ]), [t, loadedSnapshotId, selectedStation, connected, handleLoadOrder, handleDeleteOrderSnapshot, handleExportOrderSnapshot]);

  const buildMiniChartOption = React.useCallback((batteryId) => {
    const readings = readingsByBattery[batteryId] || [];
    let ocvData = [], ccvData = [];
    if (readings.length > 0) {
      ocvData = readings.filter(r => r.phase === 'ocv').map(r => [r.t, r.v]);
      ccvData = readings.filter(r => r.phase === 'ccv').map(r => [r.t, r.v]);
    } else {
      // Fallback to chartSeriesByBattery (e.g. loaded snapshots without readingsByBattery)
      const series = chartSeriesByBattery[batteryId] || {};
      ocvData = series.ocv || [];
      ccvData = series.ccv || [];
    }
    const ccvDataConnected = ocvData.length > 0 && ccvData.length > 0
      ? [ocvData[ocvData.length - 1], ...ccvData]
      : ccvData;
    return {
      backgroundColor: '#141414',
      grid: { top: 20, right: 16, bottom: 24, left: 48 },
      tooltip: { trigger: 'axis', formatter: (params) => params.map(p => `${p.marker}${p.seriesName}: ${p.value[1]?.toFixed(3)}V @ ${p.value[0]}s`).join('<br/>') },
      xAxis: {
        type: 'value',
        name: 's',
        axisLabel: { color: '#aaa', fontSize: 10 },
        axisLine: { lineStyle: { color: '#444' } },
        splitLine: { lineStyle: { color: '#2a2a2a' } },
      },
      yAxis: {
        type: 'value',
        name: 'V',
        scale: true,
        axisLabel: { color: '#aaa', fontSize: 10 },
        axisLine: { lineStyle: { color: '#444' } },
        splitLine: { lineStyle: { color: '#2a2a2a' } },
      },
      series: [
        { name: 'OCV', type: 'line', data: ocvData, symbol: 'none', lineStyle: { color: '#ffee58', width: 1.5 } },
        { name: 'CCV', type: 'line', data: ccvDataConnected, symbol: 'none', lineStyle: { color: '#0091ea', width: 1.5 } },
      ],
    };
  }, [readingsByBattery, chartSeriesByBattery]);

  const prevRecordsForScrollRef = useRef(records);
  const resultsTableRef = useRef(null);

  // Unified auto-scroll: scroll to whichever row was most recently added or modified.
  // Covers: new OCV/CCV record, caliper filling dia/hei, and retest/edit of existing rows.
  useEffect(() => {
    const prev = prevRecordsForScrollRef.current;
    prevRecordsForScrollRef.current = records;
    if (!resultsTableRef.current || records.length === 0) return;

    let targetId = null;
    if (caliperPhase && records[caliperIndex]) {
      // Caliper phase: always track the row currently being measured
      targetId = records[caliperIndex].id;
    } else if (records.length > prev.length) {
      // New record appended (OCV/CCV phase)
      targetId = records[records.length - 1].id;
    } else {
      // Existing record modified (edit, retest, caliper write-back, etc.)
      // Scan from end to find the first changed row by reference
      for (let i = records.length - 1; i >= 0; i--) {
        if (records[i] !== prev[i]) {
          targetId = records[i].id;
          break;
        }
      }
    }

    if (!targetId) return;
    requestAnimationFrame(() => {
      const tableBody = resultsTableRef.current?.querySelector('.ant-table-body');
      if (!tableBody) return;
      const row = tableBody.querySelector(`tr[data-row-key="${targetId}"]`);
      if (row) row.scrollIntoView({ block: 'nearest' });
    });
  }, [records, caliperPhase, caliperIndex]);

  // Auto-scroll voltage chart to show the latest data when autoScroll is enabled
  useEffect(() => {
    if (!autoScroll || !voltChartRef.current) return;
    try {
      const instance = voltChartRef.current.getEchartsInstance();
      if (instance) {
        instance.dispatchAction({ type: 'dataZoom', batch: [{ start: 0, end: 100 }] });
      }
    } catch (_) { /* ignore if chart instance is not yet mounted */ }
  }, [autoScroll, chartSeriesByBattery, chartDataOCV, chartDataCCV]);

  useEffect(() => {
    try {
      const sessionData = {
        records,
        chartData,
        chartDataOCV,
        chartDataCCV,
        orderId,
        testDate: testDate ? testDate.format('YYYY-MM-DD') : null,
        batteryType,
        productLine,
        ocvMin,
        ocvMax,
        ccvMin,
        ccvMax,
        diaMin,
        diaMax,
        heiMin,
        heiMax,
        chartSeriesByBattery,
        readingsByBattery,
        sessionStarted,
      };
      localStorage.setItem('battery_session', JSON.stringify(sessionData));
    } catch { }
  }, [records, chartData, chartDataOCV, chartDataCCV, orderId, testDate, batteryType, productLine, ocvMin, ocvMax, ccvMin, ccvMax, diaMin, diaMax, heiMin, heiMax, chartSeriesByBattery, readingsByBattery, sessionStarted]);

  useEffect(() => {
    try {
      const parsed = JSON.parse(localStorage.getItem('battery_session') || '{}');
      if (parsed?.records?.length > 0) {
        setSavedSessionInfo(parsed);
        setResumeModalVisible(true);
      }
    } catch { }
  }, []);

  // Verify template file still exists on server on mount
  useEffect(() => {
    getTemplateInfo().then(res => {
      if (res.data.exists) {
        const saved = localStorage.getItem('battery_template_name');
        if (saved) setTemplateName(saved);
      } else {
        localStorage.removeItem('battery_template_name');
        setTemplateName(null);
      }
    }).catch(() => { });
  }, []);

  // Verify archive file still exists on server on mount
  useEffect(() => {
    getArchiveInfo().then(res => {
      if (res.data.exists) {
        const saved = localStorage.getItem('battery_archive_name');
        if (saved) setArchiveName(saved);
      } else {
        localStorage.removeItem('battery_archive_name');
        setArchiveName(null);
      }
    }).catch(() => { });
  }, []);

  // Load battery types, product lines, and presets from server on mount
  useEffect(() => {
    const loadServerData = async () => {
      try {
        const [typesRes, linesRes, presetsRes] = await Promise.all([
          getBatteryTypes(),
          getBatteryProductLines(),
          getBatteryPresets(),
        ]);
        setBatteryTypes(typesRes.data.types || []);
        setProductLines(linesRes.data.productLines || []);
        setPresets(mapPresetsResponse(presetsRes.data.presets));
      } catch { }
    };
    loadServerData();
  }, []);
  useEffect(() => {
    const preset = presets[makePresetKey(batteryType, productLine)];
    if (preset) {
      setResistance(preset.resistance);
      setOcvTime(preset.ocvTime);
      setLoadTime(preset.loadTime);
      setKCoeff(preset.kCoeff);
      setOcvMin(preset.ocvMin);
      setOcvMax(preset.ocvMax);
      setCcvMin(preset.ccvMin);
      setCcvMax(preset.ccvMax);
      setDiaMin(preset.diaMin ?? null);
      setDiaMax(preset.diaMax ?? null);
      setHeiMin(preset.heiMin ?? null);
      setHeiMax(preset.heiMax ?? null);
    }
  }, [batteryType, productLine, presets]);

  const inputsDisabled = !connected || sessionStarted;
  const hasPreset = presets[makePresetKey(batteryType, productLine)] != null;
  const paramsDisabled = inputsDisabled || hasPreset;

  // Duplicate order ID: an existing history snapshot has the same orderId but is NOT the currently-loaded snapshot
  const isDuplicateOrderId = React.useMemo(() =>
    orderId.trim() !== '' &&
    orderHistory.some(h =>
      normalizeOrderId(h.orderId) === normalizeOrderId(orderId) &&
      h._snapshotId !== loadedSnapshotId
    ),
    [orderId, orderHistory, loadedSnapshotId]);

  const canStart = connected && !running && orderId.trim() !== '' &&
    !(records.length === 0 && isDuplicateOrderId) &&
    testDate !== null && ocvMin != null && ocvMax != null && ccvMin != null && ccvMax != null;

  return (
    <div>
      <style>{`.battery-row-bad td { background: rgba(255,77,79,0.12) !important; }`}</style>
      {/* Station Selector */}
      <div style={{ marginBottom: 8 }}>
        <Card
          size="small"
          style={{ borderColor: selectedStation ? '#1677ff' : '#d9d9d9', background: '#fff' }}
          styles={{ header: { background: '#fff' } }}
          title={
            <Space>
              <span>🏭</span>
              <span>Chọn trạm kiểm tra</span>
              {selectedStation && (
                <Badge status="success" text={
                  <span style={{ color: '#52c41a', fontSize: 12 }}>{selectedStation.name}</span>
                } />
              )}
            </Space>
          }
        >
          <Row gutter={8} align="middle">
            <Col flex="auto">
              <Select
                style={{ width: '100%' }}
                placeholder="Chọn trạm..."
                value={selectedStation?.id}
                onChange={(id) => {
                  const s = stations.find((st) => st.id === id);
                  setSelectedStation(s || null);
                  setConnected(false);
                  setRunning(false);
                  setPorts([]);
                  setPort('');
                  // Load ports for newly selected station
                  if (s) sendMsg({ action: 'get_ports', stationId: s.id });
                }}
                loading={stationsLoading}
                notFoundContent={
                  stationsLoading ? 'Đang tải...' : 'Chưa có trạm nào đăng ký. Hãy chạy start_hardware.bat trên máy trạm.'
                }
              >
                {stations.map((s) => (
                  <Option key={s.id} value={s.id} disabled={!s.online}>
                    <Badge status={s.online ? 'success' : 'default'} />
                    {' '}{s.name}
                    {!s.online && <span style={{ color: '#666', marginLeft: 8 }}>(Offline)</span>}
                  </Option>
                ))}
              </Select>
            </Col>
            <Col>
              <Tooltip title="Làm mới danh sách trạm">
                <Button
                  icon={<ReloadOutlined />}
                  loading={stationsLoading}
                  onClick={async () => {
                    setStationsLoading(true);
                    try {
                      const res = await getStations();
                      setStations(res.data.stations || []);
                    } catch (_) { }
                    finally { setStationsLoading(false); }
                  }}
                />
              </Tooltip>
            </Col>
          </Row>
          {!selectedStation && stations.length > 0 && (
            <div style={{ marginTop: 8, color: '#faad14', fontSize: 12 }}>
              ⚠ Vui lòng chọn trạm trước khi kết nối thiết bị
            </div>
          )}
        </Card>
      </div>

      {/* Header */}
      <div style={{ marginBottom: 8 }}>
        <Tabs
          activeKey={pageTab}
          onChange={setPageTab}
          size="small"
          items={[
            { key: 'test', label: t('batteryTestTab') },
            { key: 'history', label: t('batteryHistoryTab') },
            { key: 'setup', label: t('batterySetupTab') },
          ]}
          style={{ marginBottom: 0 }}
        />
      </div>

      {pageTab === 'test' ? (
        <>
          {/* Connection + Parameters row */}
          <Row gutter={16} style={{ marginBottom: 16, opacity: selectedStation ? 1 : 0.45, pointerEvents: selectedStation ? 'auto' : 'none' }}>
            {/* Connection Card */}
            <Col xs={24} md={12} lg={10}>
              <Card
                title={
                  <Space>
                    <ApiOutlined />
                    {t('batteryConnection')}
                    <Badge
                      status={connected ? 'success' : 'default'}
                      text={connected ? t('batteryConnected') : t('batteryNotConnected')}
                    />
                  </Space>
                }
                size="small"
              >
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Row gutter={8} align="middle">
                    <Col flex="auto">
                      <Select
                        placeholder={t('batterySelectPort')}
                        value={port || undefined}
                        onChange={setPort}
                        style={{ width: '100%' }}
                        disabled={connected}
                      >
                        {ports.map((p) => (
                          <Option key={p} value={p}>{p}</Option>
                        ))}
                      </Select>
                    </Col>
                    <Col>
                      <Tooltip title={t('batteryRefreshPorts')}>
                        <Button icon={<ReloadOutlined />} onClick={handleRefreshPorts} disabled={connected} />
                      </Tooltip>
                    </Col>
                  </Row>

                  <Row gutter={8}>
                    <Col flex="auto">
                      <Select
                        value={baudRate}
                        onChange={setBaudRate}
                        style={{ width: '100%' }}
                        disabled={connected}
                      >
                        {[9600, 19200, 38400, 57600, 115200].map((b) => (
                          <Option key={b} value={b}>{b}</Option>
                        ))}
                      </Select>
                    </Col>
                    <Col>
                      <Checkbox
                        checked={simMode}
                        onChange={(e) => setSimMode(e.target.checked)}
                        disabled={connected}
                      >
                        {t('batterySimMode')}
                      </Checkbox>
                    </Col>
                  </Row>

                  {!connected ? (
                    <Button
                      type="primary"
                      icon={<ApiOutlined />}
                      onClick={handleConnect}
                      loading={connecting}
                      block
                    >
                      {connecting ? t('batteryConnecting') : t('batteryConnect')}
                    </Button>
                  ) : (
                    <Button
                      danger
                      icon={<DisconnectOutlined />}
                      onClick={handleDisconnect}
                      block
                    >
                      {t('batteryDisconnect')}
                    </Button>
                  )}
                </Space>
              </Card>
            </Col>

            {/* Parameters Card */}
            <Col xs={24} md={12} lg={14}>
              <Card title={t('batteryParameters')} size="small">
                {hasPreset && (
                  <div style={{ color: '#faad14', fontSize: 12, marginBottom: 8 }}>
                    <InfoCircleOutlined style={{ marginRight: 4 }} />
                    {t('batteryParamsLockedHint')}
                  </div>
                )}
                <Row gutter={[8, 8]}>
                  <Col xs={24} sm={12}>
                    <Form.Item
                      label={t('batteryOrderId')}
                      style={{ marginBottom: 0 }}
                      validateStatus={isDuplicateOrderId ? 'error' : ''}
                      help={isDuplicateOrderId ? t('batteryOrderIdDuplicate') : undefined}
                    >
                      <Input
                        value={orderId}
                        onChange={(e) => setOrderId(e.target.value)}
                        onBlur={(e) => {
                          // Only show the duplicate modal when the user finishes typing (on blur)
                          // so that entering "121" doesn't falsely trigger a warning for "12"
                          if (records.length === 0) {
                            const normalized = normalizeOrderId(e.target.value);
                            if (normalized !== '') {
                              const match = orderHistory.find(h =>
                                normalizeOrderId(h.orderId) === normalized &&
                                h._snapshotId !== loadedSnapshotId
                              );
                              if (match) {
                                setDuplicateMatchingSnapshot(match);
                                setDuplicateOrderWarningVisible(true);
                              }
                            }
                          }
                        }}
                        disabled={inputsDisabled}
                        placeholder="e.g. ORD-001"
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={24} sm={12}>
                    <Form.Item label={t('batteryTestMonth')} style={{ marginBottom: 0 }}>
                      <DatePicker
                        value={testDate}
                        onChange={setTestDate}
                        disabled={inputsDisabled}
                        format="DD/MM/YYYY"
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} sm={8}>
                    <Form.Item label={t('batteryResistance')} style={{ marginBottom: 0 }}>
                      <InputNumber
                        value={resistance}
                        onChange={setResistance}
                        disabled={paramsDisabled}
                        min={0.01}
                        step={0.01}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} sm={8}>
                    <Form.Item label={t('batteryOcvTime')} style={{ marginBottom: 0 }}>
                      <InputNumber
                        value={ocvTime}
                        onChange={setOcvTime}
                        disabled={paramsDisabled}
                        min={0.1}
                        step={0.1}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} sm={8}>
                    <Form.Item label={t('batteryLoadTime')} style={{ marginBottom: 0 }}>
                      <InputNumber
                        value={loadTime}
                        onChange={setLoadTime}
                        disabled={paramsDisabled}
                        min={0.1}
                        step={0.1}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} sm={8}>
                    <Form.Item label={t('batteryKCoeff')} style={{ marginBottom: 0 }}>
                      <InputNumber
                        value={kCoeff}
                        onChange={setKCoeff}
                        disabled={paramsDisabled}
                        min={0}
                        step={0.01}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>
                  </Col>
                  <Col xs={12} sm={8}>
                    <Form.Item label={t('batteryType')} style={{ marginBottom: 0 }}>
                      <Select
                        value={batteryType}
                        onChange={(val) => { setBatteryType(val); setTypeSearchText(''); }}
                        disabled={inputsDisabled}
                        showSearch
                        searchValue={typeSearchText}
                        onSearch={setTypeSearchText}
                        filterOption={(input, option) =>
                          option?.value?.toLowerCase().includes(input.toLowerCase())
                        }
                        dropdownRender={(menu) => (
                          <>
                            {menu}
                            {typeSearchText && !batteryTypes.some(bt => bt.name.toLowerCase() === typeSearchText.toLowerCase()) && (
                              <>
                                <Divider style={{ margin: '4px 0' }} />
                                <div
                                  role="button"
                                  tabIndex={0}
                                  style={{ padding: '4px 12px', cursor: typeLineLoading ? 'not-allowed' : 'pointer', color: '#1677ff', display: 'flex', alignItems: 'center', gap: 6 }}
                                  onMouseDown={(e) => { e.preventDefault(); handleCreateAndSelectType(typeSearchText); }}
                                  onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handleCreateAndSelectType(typeSearchText); } }}
                                >
                                  <PlusOutlined /> {t('batteryTypeAddNew')}
                                </div>
                              </>
                            )}
                          </>
                        )}
                        style={{ width: '100%' }}
                      >
                        {batteryTypes.map(bt => <Option key={bt.id} value={bt.name}>{bt.name}</Option>)}
                      </Select>
                    </Form.Item>
                  </Col>
                  <Col xs={12} sm={8}>
                    <Form.Item label={t('batteryProductLine')} style={{ marginBottom: 0 }}>
                      <Select
                        value={productLine}
                        onChange={(val) => { setProductLine(val); setLineSearchText(''); }}
                        disabled={inputsDisabled}
                        showSearch
                        searchValue={lineSearchText}
                        onSearch={setLineSearchText}
                        filterOption={(input, option) =>
                          option?.value?.toLowerCase().includes(input.toLowerCase())
                        }
                        dropdownRender={(menu) => (
                          <>
                            {menu}
                            {lineSearchText && !productLines.some(pl => pl.name.toLowerCase() === lineSearchText.toLowerCase()) && (
                              <>
                                <Divider style={{ margin: '4px 0' }} />
                                <div
                                  role="button"
                                  tabIndex={0}
                                  style={{ padding: '4px 12px', cursor: typeLineLoading ? 'not-allowed' : 'pointer', color: '#1677ff', display: 'flex', alignItems: 'center', gap: 6 }}
                                  onMouseDown={(e) => { e.preventDefault(); handleCreateAndSelectLine(lineSearchText); }}
                                  onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handleCreateAndSelectLine(lineSearchText); } }}
                                >
                                  <PlusOutlined /> {t('batteryLineAddNew')}
                                </div>
                              </>
                            )}
                          </>
                        )}
                        style={{ width: '100%' }}
                      >
                        {productLines.map(pl => <Option key={pl.id} value={pl.name}>{pl.name}</Option>)}
                      </Select>
                    </Form.Item>
                  </Col>
                  <Col xs={24} sm={12}>
                    <Form.Item label={t('batteryOcvStandard')} style={{ marginBottom: 0 }} required>
                      <Space.Compact style={{ width: '100%' }}>
                        <InputNumber
                          value={ocvMin}
                          onChange={setOcvMin}
                          disabled={paramsDisabled}
                          placeholder={t('from')}
                          min={0}
                          step={0.001}
                          style={{ width: '50%' }}
                        />
                        <InputNumber
                          value={ocvMax}
                          onChange={setOcvMax}
                          disabled={paramsDisabled}
                          placeholder={t('to')}
                          min={0}
                          step={0.001}
                          style={{ width: '50%' }}
                        />
                      </Space.Compact>
                    </Form.Item>
                  </Col>
                  <Col xs={24} sm={12}>
                    <Form.Item label={t('batteryCcvStandard')} style={{ marginBottom: 0 }} required>
                      <Space.Compact style={{ width: '100%' }}>
                        <InputNumber
                          value={ccvMin}
                          onChange={setCcvMin}
                          disabled={paramsDisabled}
                          placeholder={t('from')}
                          min={0}
                          step={0.001}
                          style={{ width: '50%' }}
                        />
                        <InputNumber
                          value={ccvMax}
                          onChange={setCcvMax}
                          disabled={paramsDisabled}
                          placeholder={t('to')}
                          min={0}
                          step={0.001}
                          style={{ width: '50%' }}
                        />
                      </Space.Compact>
                    </Form.Item>
                  </Col>
                  <Col xs={24} sm={12}>
                    <Form.Item label={t('batteryDiaStandard')} style={{ marginBottom: 0 }}>
                      <Space.Compact style={{ width: '100%' }}>
                        <InputNumber
                          value={diaMin}
                          onChange={setDiaMin}
                          disabled={paramsDisabled}
                          placeholder={t('from')}
                          min={0}
                          step={0.01}
                          style={{ width: '50%' }}
                        />
                        <InputNumber
                          value={diaMax}
                          onChange={setDiaMax}
                          disabled={paramsDisabled}
                          placeholder={t('to')}
                          min={0}
                          step={0.01}
                          style={{ width: '50%' }}
                        />
                      </Space.Compact>
                    </Form.Item>
                  </Col>
                  <Col xs={24} sm={12}>
                    <Form.Item label={t('batteryHeiStandard')} style={{ marginBottom: 0 }}>
                      <Space.Compact style={{ width: '100%' }}>
                        <InputNumber
                          value={heiMin}
                          onChange={setHeiMin}
                          disabled={paramsDisabled}
                          placeholder={t('from')}
                          min={0}
                          step={0.01}
                          style={{ width: '50%' }}
                        />
                        <InputNumber
                          value={heiMax}
                          onChange={setHeiMax}
                          disabled={paramsDisabled}
                          placeholder={t('to')}
                          min={0}
                          step={0.01}
                          style={{ width: '50%' }}
                        />
                      </Space.Compact>
                    </Form.Item>
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>

          {/* Status Bar */}
          <div
            style={{
              background: '#fff',
              border: '1px solid #f0f0f0',
              borderLeft: `4px solid ${statusColor}`,
              borderRadius: 8,
              padding: '8px 16px',
              marginBottom: 8,
              color: statusColor,
              fontSize: 15,
              fontWeight: 600,
              fontFamily: 'monospace',
              letterSpacing: 1,
              minHeight: 36,
              display: 'flex',
              alignItems: 'center',
              gap: 12,
            }}
          >
            <span>{t('batteryStatus')}:</span>
            <span>{statusText}</span>
          </div>

          {/* Caliper Card — only shown after OCV/CCV phase is done */}
          {caliperPhase && (
            <Card
              size="small"
              title={
                <Space>
                  <span>📏 {t('batteryCaliperSection')}</span>
                  {records.length > 0 && records[caliperIndex] != null && (
                    caliperSingleMode ? (
                      <Tag color="warning" style={{ marginLeft: 4 }}>
                        Re-measure: {t('batteryId')} {records[caliperIndex].id}
                      </Tag>
                    ) : (
                      <span style={{ marginLeft: 4, display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                        <Tag color="processing" style={{ margin: 0 }}>{t('batteryId')}:</Tag>
                        <Tooltip title={t('batteryCaliperJumpTooltip')}>
                          <Input
                            key={caliperIndex}
                            size="small"
                            defaultValue={String(records[caliperIndex].id)}
                            style={{ width: 55, textAlign: 'center' }}
                            onPressEnter={(e) => {
                              handleCaliperIdJump(e.target.value);
                              e.target.blur();
                            }}
                            onBlur={(e) => {
                              handleCaliperIdJump(e.target.value);
                            }}
                          />
                        </Tooltip>
                        <span style={{ color: '#1677ff', fontSize: 13 }}>/ {records.length}</span>
                      </span>
                    )
                  )}
                </Space>
              }
              style={{ marginBottom: 8, background: '#fff' }}
              styles={{ header: { background: '#fff' } }}
            >
              {/* Row 1: mode selector + live buffer + read-only value cells */}
              <Row gutter={[16, 12]} align="middle" wrap>
                {/* Mode toggle + live buffer from caliper */}
                <Col xs={24} sm="auto">
                  <Space direction="vertical" size={4}>
                    <span style={{ fontSize: 11, color: '#888', textTransform: 'uppercase', letterSpacing: 1 }}>
                      {t('batteryCaliperMode')}
                    </span>
                    <Space size={8}>
                      <Radio.Group
                        value={caliperMode}
                        onChange={(e) => {
                          setCaliperMode(e.target.value);
                          // Blur the radio button so the caliper's next Enter keypress
                          // is caught by the global window keydown listener, not the button
                          e.target.blur();
                        }}
                        buttonStyle="solid"
                        size="small"
                      >
                        <Radio.Button value="dia">{t('batteryCaliperModeDia')}</Radio.Button>
                        <Radio.Button value="hei">{t('batteryCaliperModeHei')}</Radio.Button>
                      </Radio.Group>
                      <Input
                        ref={caliperInputRef}
                        size="small"
                        value={caliperBuffer}
                        placeholder="Đang chờ thước kẹp..."
                        style={{
                          width: 170,
                          fontFamily: 'monospace',
                          background: caliperBuffer ? '#f6ffed' : '#fafafa',
                          borderColor: caliperBuffer ? '#52c41a' : '#d9d9d9',
                          color: caliperBuffer ? '#389e0d' : '#8c8c8c',
                        }}
                        readOnly
                        tabIndex={-1}
                      />
                    </Space>
                  </Space>
                </Col>

                {/* Diameter — read-only display */}
                <Col xs="auto">
                  <Space direction="vertical" size={4}>
                    <span style={{ fontSize: 11, color: caliperMode === 'dia' ? '#1677ff' : '#8c8c8c', textTransform: 'uppercase', letterSpacing: 1, fontWeight: caliperMode === 'dia' ? 600 : 400 }}>
                      {t('batteryCaliperDia')} (mm)
                    </span>
                    <div
                      style={{
                        width: 110,
                        height: 24,
                        lineHeight: '22px',
                        textAlign: 'center',
                        fontFamily: 'monospace',
                        fontSize: 14,
                        fontWeight: 600,
                        border: `1px solid ${caliperMode === 'dia' ? '#1677ff' : '#d9d9d9'}`,
                        borderRadius: 6,
                        background: caliperDia ? '#e6f4ff' : '#fafafa',
                        color: caliperDia ? '#1677ff' : '#bfbfbf',
                        userSelect: 'none',
                        cursor: 'default',
                        boxShadow: caliperMode === 'dia' ? '0 0 6px #1677ff33' : 'none',
                        transition: 'all 0.2s',
                      }}
                    >
                      {caliperDia ? parseFloat(caliperDia).toFixed(2) : '—'}
                    </div>
                  </Space>
                </Col>

                {/* Height — read-only display */}
                <Col xs="auto">
                  <Space direction="vertical" size={4}>
                    <span style={{ fontSize: 11, color: caliperMode === 'hei' ? '#52c41a' : '#8c8c8c', textTransform: 'uppercase', letterSpacing: 1, fontWeight: caliperMode === 'hei' ? 600 : 400 }}>
                      {t('batteryCaliperHei')} (mm)
                    </span>
                    <div
                      style={{
                        width: 110,
                        height: 24,
                        lineHeight: '22px',
                        textAlign: 'center',
                        fontFamily: 'monospace',
                        fontSize: 14,
                        fontWeight: 600,
                        border: `1px solid ${caliperMode === 'hei' ? '#52c41a' : '#d9d9d9'}`,
                        borderRadius: 6,
                        background: caliperHei ? '#f6ffed' : '#fafafa',
                        color: caliperHei ? '#389e0d' : '#bfbfbf',
                        userSelect: 'none',
                        cursor: 'default',
                        boxShadow: caliperMode === 'hei' ? '0 0 6px #52c41a33' : 'none',
                        transition: 'all 0.2s',
                      }}
                    >
                      {caliperHei ? parseFloat(caliperHei).toFixed(2) : '—'}
                    </div>
                  </Space>
                </Col>

                {/* Dia standard — same row */}
                <Col xs="auto">
                  <Space direction="vertical" size={2}>
                    <span style={{ fontSize: 11, color: '#888', textTransform: 'uppercase', letterSpacing: 1 }}>
                      {t('batteryDiaStandard')}
                    </span>
                    <div style={{ fontFamily: 'monospace', fontSize: 13, color: '#595959', padding: '2px 8px', background: '#fafafa', border: '1px solid #d9d9d9', borderRadius: 6, userSelect: 'none' }}>
                      {diaMin != null && diaMax != null ? `${diaMin} – ${diaMax} mm` : diaMin != null ? `≥ ${diaMin} mm` : diaMax != null ? `≤ ${diaMax} mm` : '—'}
                    </div>
                  </Space>
                </Col>

                {/* Hei standard — same row */}
                <Col xs="auto">
                  <Space direction="vertical" size={2}>
                    <span style={{ fontSize: 11, color: '#888', textTransform: 'uppercase', letterSpacing: 1 }}>
                      {t('batteryHeiStandard')}
                    </span>
                    <div style={{ fontFamily: 'monospace', fontSize: 13, color: '#595959', padding: '2px 8px', background: '#fafafa', border: '1px solid #d9d9d9', borderRadius: 6, userSelect: 'none' }}>
                      {heiMin != null && heiMax != null ? `${heiMin} – ${heiMax} mm` : heiMin != null ? `≥ ${heiMin} mm` : heiMax != null ? `≤ ${heiMax} mm` : '—'}
                    </div>
                  </Space>
                </Col>

                {/* Help icon */}
                <Col xs="auto">
                  <Tooltip title={t('batteryCaliperHint')}>
                    <QuestionCircleOutlined style={{ color: '#555', fontSize: 16, cursor: 'help', marginTop: 20 }} />
                  </Tooltip>
                </Col>

                {/* Action buttons — same row */}
                <Col xs="auto" style={{ marginTop: 18 }}>
                  <Button size="small" onClick={handleResetCaliper}>
                    {t('cancel')}
                  </Button>
                </Col>
              </Row>
            </Card>
          )}

          {/* Excel Report Card */}
          <Collapse
            style={{ marginBottom: 16 }}
            items={[{
              key: 'excel-report',
              label: t('batteryExcelReport'),
              children: (
                <Row gutter={[16, 8]}>
                  <Col xs={24} md={12}>
                    <Upload.Dragger
                      accept=".xlsx"
                      showUploadList={false}
                      customRequest={handleTemplateUpload}
                      style={{ padding: '8px 16px' }}
                    >
                      <p className="ant-upload-drag-icon">
                        <InboxOutlined />
                      </p>
                      <p className="ant-upload-text">{t('batteryTemplateUpload')}</p>
                      <p className="ant-upload-hint">{t('batteryTemplateUploadHint')}</p>
                      {templateName && (
                        <p style={{ color: '#52c41a', marginTop: 4 }}>
                          {t('batteryCurrentTemplate')}: <strong>{templateName}</strong>
                        </p>
                      )}
                      {!templateName && (
                        <p style={{ color: '#888', marginTop: 4 }}>{t('batteryNoTemplate')}</p>
                      )}
                    </Upload.Dragger>
                  </Col>
                  <Col xs={24} md={12}>
                    <Upload.Dragger
                      accept=".xlsx"
                      showUploadList={false}
                      customRequest={handleArchiveUpload}
                      style={{ padding: '8px 16px' }}
                    >
                      <p className="ant-upload-drag-icon">
                        <InboxOutlined />
                      </p>
                      <p className="ant-upload-text">{t('batteryArchiveUpload')}</p>
                      <p className="ant-upload-hint">{t('batteryArchiveUploadHint')}</p>
                      {archiveName && (
                        <p style={{ color: '#52c41a', marginTop: 4 }}>
                          {t('batteryCurrentArchive')}: <strong>{archiveName}</strong>
                        </p>
                      )}
                      {!archiveName && (
                        <p style={{ color: '#888', marginTop: 4 }}>{t('batteryNoArchive')}</p>
                      )}
                    </Upload.Dragger>
                  </Col>
                  <Col xs={24} style={{ display: 'flex', gap: 8, marginTop: 8 }}>
                    <Button
                      icon={<DownloadOutlined />}
                      onClick={handleDownloadTemplateReport}
                      disabled={records.length === 0}
                      loading={downloadingTemplate}
                      style={{ flex: 1 }}
                    >
                      {t('batteryDownloadTemplateReport')}
                    </Button>
                    <Button
                      icon={<DownloadOutlined />}
                      onClick={handleDownloadArchiveReport}
                      disabled={records.length === 0}
                      loading={downloadingArchive}
                      style={{ flex: 1 }}
                    >
                      {t('batteryDownloadArchiveReport')}
                    </Button>
                  </Col>
                </Row>
              ),
            }]}
          />

          {/* Chart + Results */}
          <Row gutter={16} style={{ marginBottom: 16 }}>
            {/* Chart */}
            <Col xs={24} lg={14}>
              <Card
                title={
                  <Space>
                    {t('batteryChart')}
                    <Checkbox
                      checked={autoScroll}
                      onChange={(e) => setAutoScroll(e.target.checked)}
                    >
                      {t('batteryAutoScroll')}
                    </Checkbox>
                  </Space>
                }
                extra={<Button icon={<FullscreenOutlined />} size="small" onClick={() => setChartZoomVisible(true)} />}
                size="small"
                bodyStyle={{ padding: 8 }}
              >
                <ReactECharts
                  ref={voltChartRef}
                  option={chartOption}
                  style={{ height: 280 }}
                  notMerge={true}
                  lazyUpdate={true}
                  onEvents={{
                    legendselectchanged: (params) => setLegendSelected(params.selected),
                  }}
                />
              </Card>
            </Col>

            {/* Results Table */}
            <Col xs={24} lg={10}>
              <div ref={resultsTableRef} style={{ height: '100%' }}>
              <Card
                size="small"
                style={{ height: '100%' }}
                title={<span style={{ fontWeight: 600 }}>{t('batteryResults')}</span>}
                extra={<Button icon={<FullscreenOutlined />} size="small" onClick={() => setTableZoomVisible(true)} />}
              >
                <Table
                  dataSource={records}
                  columns={columns}
                  rowKey="id"
                  size="small"
                  pagination={false}
                  locale={{ emptyText: t('batteryNoResults') }}
                  scroll={{ x: true, y: 260 }}
                  rowClassName={(record) => {
                    const ocvBad = ocvSpec && record.ocv != null && (record.ocv < ocvSpec.min || record.ocv > ocvSpec.max);
                    const ccvBad = ccvSpec && record.ccv != null && (record.ccv < ccvSpec.min || record.ccv > ccvSpec.max);
                    const diaBad = diaSpec && record.dia != null && (record.dia < diaSpec.min || record.dia > diaSpec.max);
                    const heiBad = heiSpec && record.hei != null && (record.hei < heiSpec.min || record.hei > heiSpec.max);
                    return (ocvBad || ccvBad || diaBad || heiBad) ? 'battery-row-bad' : '';
                  }}
                  components={{
                    body: {
                      row: (rowProps) => {
                        const record = recordsMap[String(rowProps['data-row-key'])];
                        return <RowWithPopover record={record} readingsByBattery={readingsByBattery} chartSeriesByBattery={chartSeriesByBattery} buildMiniChartOption={buildMiniChartOption} {...rowProps} />;
                      },
                    },
                  }}
                />
              </Card>
              </div>
            </Col>
          </Row>

          {/* Action Buttons */}
          <Space wrap>
            <Tooltip title={!canStart && !running ? t('batteryFillRequiredFields') : undefined}>
              <Button
                type="primary"
                size="large"
                icon={running ? <StopOutlined /> : <PlayCircleOutlined />}
                danger={running}
                disabled={running ? false : !canStart}
                onClick={handleStartStop}
              >
                {running ? t('batteryStop') : t('batteryStart')}
              </Button>
            </Tooltip>

            <Divider type="vertical" />

            <Button
              icon={<DeleteOutlined />}
              onClick={() => {
                Modal.confirm({
                  title: t('batteryClearSessionConfirmTitle'),
                  content: t('batteryClearSessionConfirmContent'),
                  okText: t('confirm'),
                  cancelText: t('cancel'),
                  okButtonProps: { danger: true },
                  onOk: handleClearSession,
                });
              }}
              disabled={!connected || records.length === 0}
            >
              {t('batteryClearSession')}
            </Button>

            {!running && records.length > 0 && !caliperPhase && (
              <>
                <Divider type="vertical" />
                <Button
                  type="primary"
                  style={{ background: '#52c41a', borderColor: '#52c41a' }}
                  onClick={() => {
                    setCaliperPhase(true);
                    setCaliperSingleMode(false);
                    setCaliperMode('dia');
                    setCaliperDia('');
                    setCaliperHei('');
                    setCaliperIndex(0);
                  }}
                >
                  ✅ {t('batteryOcvCcvDone')}
                </Button>
              </>
            )}
          </Space>
        </>
      ) : pageTab === 'history' ? (
        <Card
          size="small"
          title={
            <Space>
              <span>{t('batteryHistoryTab')}</span>
              <Tag color="blue">{filteredOrderHistory.length}</Tag>
            </Space>
          }
        >
          <Row gutter={[8, 8]} style={{ marginBottom: 12 }}>
            <Col xs={24} md={10} lg={8}>
              <Input
                allowClear
                placeholder={t('batteryHistorySearch')}
                value={historySearchOrder}
                onChange={(e) => setHistorySearchOrder(e.target.value)}
              />
            </Col>
            <Col xs={12} sm={8} md={4} lg={3}>
              <Select
                style={{ width: '100%' }}
                value={historyTypeFilter || undefined}
                placeholder={t('batteryHistoryAllTypes')}
                allowClear
                onChange={(value) => setHistoryTypeFilter(value || '')}
              >
                {batteryTypes.map(bt => <Option key={bt.id} value={bt.name}>{bt.name}</Option>)}
              </Select>
            </Col>
            <Col xs={12} sm={8} md={4} lg={3}>
              <Select
                style={{ width: '100%' }}
                value={historyLineFilter || undefined}
                placeholder={t('batteryHistoryAllLines')}
                allowClear
                onChange={(value) => setHistoryLineFilter(value || '')}
              >
                {productLines.map(pl => <Option key={pl.id} value={pl.name}>{pl.name}</Option>)}
              </Select>
            </Col>
            <Col xs={24} sm={16} md={8} lg={6}>
              <RangePicker
                style={{ width: '100%' }}
                value={historyDateRange}
                onChange={setHistoryDateRange}
                allowClear
                placeholder={[t('batteryHistoryDateFrom'), t('batteryHistoryDateTo')]}
              />
            </Col>
            <Col xs={24} md="auto">
              <Button
                danger
                icon={<DeleteOutlined />}
                disabled={orderHistory.length === 0}
                onClick={() => {
                  Modal.confirm({
                    title: t('batteryHistoryDeleteAllTitle'),
                    content: t('batteryHistoryDeleteAllContent'),
                    okText: t('delete'),
                    cancelText: t('cancel'),
                    okButtonProps: { danger: true },
                    onOk: () => clearOrderHistory().then(() => {
                      setOrderHistory([]);
                      resetLoadedSnapshotTracking();
                    }).catch((e) => {
                      notification.error({ message: 'Không thể xóa lịch sử', description: e?.message });
                    }),
                  });
                }}
              >
                {t('batteryClearHistory')}
              </Button>
            </Col>
          </Row>

          {orderHistory.length === 0 ? (
            <div style={{ textAlign: 'center', color: '#595959', padding: '24px 0' }}>
              {t('batteryOrderHistoryEmpty')}
            </div>
          ) : (
            <Table
              dataSource={filteredOrderHistory}
              columns={historySummaryColumns}
              rowKey="_snapshotId"
              size="small"
              loading={orderHistoryLoading}
              locale={{ emptyText: t('batteryHistoryNoMatch') }}
              pagination={{
                pageSize: 10,
                showSizeChanger: false,
              }}
              scroll={{ x: true }}
              expandable={{
                expandedRowRender: (snapshot) => (
                  <Table
                    dataSource={snapshot.records || []}
                    columns={historyDetailColumns}
                    rowKey="id"
                    size="small"
                    pagination={false}
                    locale={{ emptyText: t('batteryNoResults') }}
                    scroll={{ x: true, y: 320 }}
                  />
                ),
              }}
            />
          )}
        </Card>
      ) : pageTab === 'setup' ? (
        <Card
          size="small"
          title={t('batterySetupTab')}
        >
          {/* Manage Battery Types and Product Lines */}
          <Row gutter={16} style={{ marginBottom: 16 }}>
            <Col xs={24} sm={12}>
              <Card size="small" title={t('batteryManageTypes')} style={{ marginBottom: 8 }}>
                <Space.Compact style={{ width: '100%', marginBottom: 8 }}>
                  <Input
                    value={newTypeName}
                    onChange={(e) => setNewTypeName(e.target.value)}
                    placeholder={t('batteryTypeNamePlaceholder')}
                    onPressEnter={handleAddBatteryType}
                  />
                  <Button
                    type="primary"
                    loading={typeLineLoading}
                    onClick={handleAddBatteryType}
                  >
                    {t('batteryAddType')}
                  </Button>
                </Space.Compact>
                <Space wrap>
                  {batteryTypes.map(bt => (
                    <Tag
                      key={bt.id}
                      closable
                      onClose={(e) => {
                        e.preventDefault();
                        Modal.confirm({
                          title: t('batteryDeleteTypeConfirm'),
                          content: bt.name,
                          okText: t('delete'),
                          cancelText: t('cancel'),
                          okButtonProps: { danger: true },
                          onOk: async () => {
                            try {
                              await deleteBatteryType(bt.id);
                              const res = await getBatteryTypes();
                              setBatteryTypes(res.data.types || []);
                              notification.success({ message: t('batteryTypeDeleted') });
                            } catch (err) {
                              notification.error({ message: err?.response?.data?.error || err.message });
                            }
                          },
                        });
                      }}
                    >
                      {bt.name}
                    </Tag>
                  ))}
                </Space>
              </Card>
            </Col>
            <Col xs={24} sm={12}>
              <Card size="small" title={t('batteryManageLines')} style={{ marginBottom: 8 }}>
                <Space.Compact style={{ width: '100%', marginBottom: 8 }}>
                  <Input
                    value={newLineName}
                    onChange={(e) => setNewLineName(e.target.value)}
                    placeholder={t('batteryLineNamePlaceholder')}
                    onPressEnter={handleAddBatteryLine}
                  />
                  <Button
                    type="primary"
                    loading={typeLineLoading}
                    onClick={handleAddBatteryLine}
                  >
                    {t('batteryAddLine')}
                  </Button>
                </Space.Compact>
                <Space wrap>
                  {productLines.map(pl => (
                    <Tag
                      key={pl.id}
                      closable
                      onClose={(e) => {
                        e.preventDefault();
                        Modal.confirm({
                          title: t('batteryDeleteLineConfirm'),
                          content: pl.name,
                          okText: t('delete'),
                          cancelText: t('cancel'),
                          okButtonProps: { danger: true },
                          onOk: async () => {
                            try {
                              await deleteBatteryProductLine(pl.id);
                              const res = await getBatteryProductLines();
                              setProductLines(res.data.productLines || []);
                              notification.success({ message: t('batteryLineDeleted') });
                            } catch (err) {
                              notification.error({ message: err?.response?.data?.error || err.message });
                            }
                          },
                        });
                      }}
                    >
                      {pl.name}
                    </Tag>
                  ))}
                </Space>
              </Card>
            </Col>
          </Row>

          {/* Preset Form */}
          <Card size="small" title={t('batterySetupAddEdit')} style={{ marginBottom: 16 }}>
            <Row gutter={[8, 8]}>
              <Col xs={12} sm={6}>
                <Form.Item label={t('batteryType')} style={{ marginBottom: 0 }}>
                  <Select
                    value={setupForm.batteryType || undefined}
                    placeholder={t('batteryType')}
                    onChange={(v) => {
                      const preset = presets[makePresetKey(v, setupForm.productLine)];
                      setSetupForm(f => preset ? { ...f, ...preset } : { ...f, batteryType: v });
                    }}
                    style={{ width: '100%' }}
                  >
                    {batteryTypes.map(bt => <Option key={bt.id} value={bt.name}>{bt.name}</Option>)}
                  </Select>
                </Form.Item>
              </Col>
              <Col xs={12} sm={6}>
                <Form.Item label={t('batteryProductLine')} style={{ marginBottom: 0 }}>
                  <Select
                    value={setupForm.productLine || undefined}
                    placeholder={t('batteryProductLine')}
                    onChange={(v) => {
                      const preset = presets[makePresetKey(setupForm.batteryType, v)];
                      setSetupForm(f => preset ? { ...f, ...preset } : { ...f, productLine: v });
                    }}
                    style={{ width: '100%' }}
                  >
                    {productLines.map(pl => <Option key={pl.id} value={pl.name}>{pl.name}</Option>)}
                  </Select>
                </Form.Item>
              </Col>
              <Col xs={12} sm={4}>
                <Form.Item label={t('batteryResistance')} style={{ marginBottom: 0 }}>
                  <InputNumber
                    value={setupForm.resistance}
                    onChange={(v) => setSetupForm(f => ({ ...f, resistance: v }))}
                    min={0.01} step={0.01} style={{ width: '100%' }}
                  />
                </Form.Item>
              </Col>
              <Col xs={12} sm={4}>
                <Form.Item label={t('batteryOcvTime')} style={{ marginBottom: 0 }}>
                  <InputNumber
                    value={setupForm.ocvTime}
                    onChange={(v) => setSetupForm(f => ({ ...f, ocvTime: v }))}
                    min={0.1} step={0.1} style={{ width: '100%' }}
                  />
                </Form.Item>
              </Col>
              <Col xs={12} sm={4}>
                <Form.Item label={t('batteryLoadTime')} style={{ marginBottom: 0 }}>
                  <InputNumber
                    value={setupForm.loadTime}
                    onChange={(v) => setSetupForm(f => ({ ...f, loadTime: v }))}
                    min={0.1} step={0.1} style={{ width: '100%' }}
                  />
                </Form.Item>
              </Col>
              <Col xs={12} sm={4}>
                <Form.Item label={t('batteryKCoeff')} style={{ marginBottom: 0 }}>
                  <InputNumber
                    value={setupForm.kCoeff}
                    onChange={(v) => setSetupForm(f => ({ ...f, kCoeff: v }))}
                    min={0} step={0.01} style={{ width: '100%' }}
                  />
                </Form.Item>
              </Col>
              <Col xs={24} sm={12}>
                <Form.Item label={t('batteryOcvStandard')} style={{ marginBottom: 0 }} required>
                  <Space.Compact style={{ width: '100%' }}>
                    <InputNumber
                      value={setupForm.ocvMin}
                      onChange={(v) => setSetupForm(f => ({ ...f, ocvMin: v }))}
                      placeholder={t('from')} min={0} step={0.001} style={{ width: '50%' }}
                    />
                    <InputNumber
                      value={setupForm.ocvMax}
                      onChange={(v) => setSetupForm(f => ({ ...f, ocvMax: v }))}
                      placeholder={t('to')} min={0} step={0.001} style={{ width: '50%' }}
                    />
                  </Space.Compact>
                </Form.Item>
              </Col>
              <Col xs={24} sm={12}>
                <Form.Item label={t('batteryCcvStandard')} style={{ marginBottom: 0 }} required>
                  <Space.Compact style={{ width: '100%' }}>
                    <InputNumber
                      value={setupForm.ccvMin}
                      onChange={(v) => setSetupForm(f => ({ ...f, ccvMin: v }))}
                      placeholder={t('from')} min={0} step={0.001} style={{ width: '50%' }}
                    />
                    <InputNumber
                      value={setupForm.ccvMax}
                      onChange={(v) => setSetupForm(f => ({ ...f, ccvMax: v }))}
                      placeholder={t('to')} min={0} step={0.001} style={{ width: '50%' }}
                    />
                  </Space.Compact>
                </Form.Item>
              </Col>
              <Col xs={24} sm={12}>
                <Form.Item label={t('batteryDiaStandard')} style={{ marginBottom: 0 }}>
                  <Space.Compact style={{ width: '100%' }}>
                    <InputNumber
                      value={setupForm.diaMin}
                      onChange={(v) => setSetupForm(f => ({ ...f, diaMin: v }))}
                      placeholder={t('from')} min={0} step={0.01} style={{ width: '50%' }}
                    />
                    <InputNumber
                      value={setupForm.diaMax}
                      onChange={(v) => setSetupForm(f => ({ ...f, diaMax: v }))}
                      placeholder={t('to')} min={0} step={0.01} style={{ width: '50%' }}
                    />
                  </Space.Compact>
                </Form.Item>
              </Col>
              <Col xs={24} sm={12}>
                <Form.Item label={t('batteryHeiStandard')} style={{ marginBottom: 0 }}>
                  <Space.Compact style={{ width: '100%' }}>
                    <InputNumber
                      value={setupForm.heiMin}
                      onChange={(v) => setSetupForm(f => ({ ...f, heiMin: v }))}
                      placeholder={t('from')} min={0} step={0.01} style={{ width: '50%' }}
                    />
                    <InputNumber
                      value={setupForm.heiMax}
                      onChange={(v) => setSetupForm(f => ({ ...f, heiMax: v }))}
                      placeholder={t('to')} min={0} step={0.01} style={{ width: '50%' }}
                    />
                  </Space.Compact>
                </Form.Item>
              </Col>
              <Col xs={24}>
                <Button type="primary" onClick={handleSavePreset}>
                  {t('batterySetupSave')}
                </Button>
              </Col>
            </Row>
          </Card>

          {/* Presets List */}
          {Object.keys(presets).length === 0 ? (
            <div style={{ textAlign: 'center', color: '#595959', padding: '24px 0' }}>
              {t('batterySetupNoPresets')}
            </div>
          ) : (
            <Table
              dataSource={Object.values(presets)}
              rowKey={(r) => `${r.batteryType}_${r.productLine}`}
              size="small"
              pagination={false}
              scroll={{ x: true }}
              columns={[
                { title: t('batteryType'), dataIndex: 'batteryType', key: 'batteryType', width: 80 },
                { title: t('batteryProductLine'), dataIndex: 'productLine', key: 'productLine', width: 100 },
                { title: t('batteryResistance'), dataIndex: 'resistance', key: 'resistance', width: 90 },
                { title: t('batteryOcvTime'), dataIndex: 'ocvTime', key: 'ocvTime', width: 100 },
                { title: t('batteryLoadTime'), dataIndex: 'loadTime', key: 'loadTime', width: 110 },
                { title: t('batteryKCoeff'), dataIndex: 'kCoeff', key: 'kCoeff', width: 80 },
                {
                  title: `OCV (${t('from')}-${t('to')})`, key: 'ocv', width: 140,
                  render: (_, r) => r.ocvMin != null && r.ocvMax != null ? `${r.ocvMin} – ${r.ocvMax}` : '-',
                },
                {
                  title: `CCV (${t('from')}-${t('to')})`, key: 'ccv', width: 140,
                  render: (_, r) => r.ccvMin != null && r.ccvMax != null ? `${r.ccvMin} – ${r.ccvMax}` : '-',
                },
                {
                  title: `${t('batteryCaliperDia')} (${t('from')}-${t('to')})`, key: 'dia', width: 140,
                  render: (_, r) => r.diaMin != null && r.diaMax != null ? `${r.diaMin} – ${r.diaMax}` : '-',
                },
                {
                  title: `${t('batteryCaliperHei')} (${t('from')}-${t('to')})`, key: 'hei', width: 140,
                  render: (_, r) => r.heiMin != null && r.heiMax != null ? `${r.heiMin} – ${r.heiMax}` : '-',
                },
                {
                  title: t('actions'), key: 'actions', width: 140,
                  render: (_, preset) => (
                    <Space size={4}>
                      <Button
                        size="small"
                        onClick={() => setSetupForm({ ...preset })}
                      >
                        {t('batterySetupEdit')}
                      </Button>
                      <Button
                        size="small"
                        danger
                        icon={<DeleteOutlined />}
                        onClick={() => {
                          Modal.confirm({
                            title: `${t('delete')} ${preset.batteryType} - ${preset.productLine}?`,
                            okText: t('delete'),
                            cancelText: t('cancel'),
                            okButtonProps: { danger: true },
                            onOk: () => handleDeletePreset(preset.batteryType, preset.productLine),
                          });
                        }}
                      />
                    </Space>
                  ),
                },
              ]}
            />
          )}
        </Card>
      ) : null}
      <Modal
        open={resumeModalVisible}
        title={t('batteryResumeTitle')}
        closable={false}
        maskClosable={false}
        keyboard={false}
        footer={[
          <Button key="new" danger onClick={() => {
            localStorage.removeItem('battery_session');
            setRecords([]);
            setChartData([]);
            setChartDataOCV([]);
            setChartDataCCV([]);
            setChartSeriesByBattery({});
            setReadingsByBattery({});
            setOrderId('');
            setTestDate(dayjs());
            setSessionStarted(false);
            const newBatteryType = batteryTypes[0]?.name || '';
            const newProductLine = productLines[0]?.name || '';
            setBatteryType(newBatteryType);
            setProductLine(newProductLine);
            const newPreset = presets[makePresetKey(newBatteryType, newProductLine)];
            if (newPreset) {
              setResistance(newPreset.resistance);
              setOcvTime(newPreset.ocvTime);
              setLoadTime(newPreset.loadTime);
              setKCoeff(newPreset.kCoeff);
              setOcvMin(newPreset.ocvMin ?? null);
              setOcvMax(newPreset.ocvMax ?? null);
              setCcvMin(newPreset.ccvMin ?? null);
              setCcvMax(newPreset.ccvMax ?? null);
            } else {
              setOcvMin(null);
              setOcvMax(null);
              setCcvMin(null);
              setCcvMax(null);
            }
            resetLoadedSnapshotTracking();
            setResumeModalVisible(false);
            if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
              wsRef.current.send(JSON.stringify({ action: 'clear_session' }));
            } else {
              pendingNewSessionRef.current = true;
            }
          }}>{t('batteryNewSession')}</Button>,
          <Button key="continue" type="primary" onClick={() => {
            setResumeModalVisible(false);
          }}>{t('batteryContinueSession')}</Button>,
        ]}
      >
        <p>{t('batteryResumeDesc')}</p>
        {savedSessionInfo && (
          <ul>
            <li><strong>{t('batteryOrderId')}:</strong> {savedSessionInfo.orderId || '-'}</li>
            <li><strong>{t('batteryType')}:</strong> {savedSessionInfo.batteryType || '-'}</li>
            <li><strong>{t('batteryProductLine')}:</strong> {savedSessionInfo.productLine || '-'}</li>
            <li><strong>{t('batteryDate')}:</strong> {savedSessionInfo.testDate || '-'}</li>
            <li><strong>{t('batteryResults')}:</strong> {savedSessionInfo.records?.length || 0} {t('batteryId')}</li>
          </ul>
        )}
      </Modal>
      {/* Duplicate Order ID Blocking Modal */}
      <Modal
        open={duplicateOrderWarningVisible}
        title={<Space><span style={{ color: '#ff4d4f', fontSize: 18 }}>⚠️</span><span style={{ color: '#cf1322' }}>{t('batteryDuplicateOrderTitle')}</span></Space>}
        closable={false}
        maskClosable={false}
        keyboard={false}
        footer={[
          <Button
            key="useOther"
            onClick={() => {
              setOrderId('');
              setDuplicateMatchingSnapshot(null);
              setDuplicateOrderWarningVisible(false);
            }}
          >
            {t('batteryDuplicateOrderUseOther')}
          </Button>,
          <Button
            key="loadOld"
            type="primary"
            onClick={() => {
              if (duplicateMatchingSnapshot) {
                handleLoadOrder(duplicateMatchingSnapshot);
              }
              setDuplicateMatchingSnapshot(null);
              setDuplicateOrderWarningVisible(false);
            }}
          >
            {t('batteryDuplicateOrderLoadOld')}
          </Button>,
        ]}
      >
        <div style={{ background: '#fff1f0', border: '1px solid #ffccc7', borderRadius: 6, padding: '12px 16px', marginBottom: 12 }}>
          <p style={{ margin: 0, color: '#cf1322' }}>{t('batteryDuplicateOrderDesc')}</p>
        </div>
        {duplicateMatchingSnapshot && (
          <ul style={{ margin: 0 }}>
            <li><strong>{t('batteryDuplicateOrderExisting')}:</strong> {duplicateMatchingSnapshot.orderId || '-'}</li>
            <li><strong>{t('batteryDuplicateOrderCount')}:</strong> {duplicateMatchingSnapshot.records?.length || 0} {t('batteryId')}</li>
          </ul>
        )}
      </Modal>
      {/* Chart Zoom Modal */}
      <Modal
        open={chartZoomVisible}
        onCancel={() => setChartZoomVisible(false)}
        footer={null}
        width="90vw"
        title={t('batteryChart')}
        destroyOnClose
        bodyStyle={{ padding: 8 }}
      >
        <ReactECharts
          option={{ ...chartOption, dataZoom: ZOOM_CHART_DATA_ZOOM }}
          style={{ height: 'calc(80vh - 60px)' }}
          notMerge={true}
          onEvents={{
            legendselectchanged: (params) => setLegendSelected(params.selected),
          }}
        />
      </Modal>
      {/* Table Zoom Modal */}
      <Modal
        open={tableZoomVisible}
        onCancel={() => setTableZoomVisible(false)}
        footer={null}
        width="90vw"
        title={t('batteryResults')}
        destroyOnClose
      >
        <Table
          dataSource={records}
          columns={columns}
          rowKey="id"
          size="small"
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            pageSizeOptions: ['10', '20', '50', '100'],
            showTotal: (total, range) => `${range[0]}-${range[1]} / ${total}`,
          }}
          locale={{ emptyText: t('batteryNoResults') }}
          scroll={{ x: true, y: ZOOM_MODAL_TABLE_SCROLL_Y }}
          rowClassName={(record) => {
            const ocvBad = ocvSpec && record.ocv != null && (record.ocv < ocvSpec.min || record.ocv > ocvSpec.max);
            const ccvBad = ccvSpec && record.ccv != null && (record.ccv < ccvSpec.min || record.ccv > ccvSpec.max);
            const diaBad = diaSpec && record.dia != null && (record.dia < diaSpec.min || record.dia > diaSpec.max);
            const heiBad = heiSpec && record.hei != null && (record.hei < heiSpec.min || record.hei > heiSpec.max);
            return (ocvBad || ccvBad || diaBad || heiBad) ? 'battery-row-bad' : '';
          }}
        />
      </Modal>
      {/* Out-of-spec battery alert — blocking modal, cannot be dismissed */}
      <Modal
        open={outOfSpecModal !== null}
        title={<Space><span>⚠️</span><span>{`Pin #${outOfSpecModal?.record?.id ?? ''} ${t('batteryOutOfSpec')}`}</span></Space>}
        closable={false}
        maskClosable={false}
        keyboard={false}
        footer={outOfSpecModal?.type === 'dim' ? [
          <Button
            key="retest-now"
            type="primary"
            danger
            onClick={() => {
              const idx = outOfSpecModal.batIdx ?? 0;
              const wasSingle = outOfSpecModal.wasInSingleMode;
              caliperIndexRef.current = idx;
              setCaliperIndex(idx);
              setCaliperSingleMode(wasSingle);
              setCaliperPhase(true);
              setCaliperMode('dia');
              setCaliperDia('');
              setCaliperHei('');
              setOutOfSpecModal(null);
            }}
          >
            {t('batteryRetestNow')}
          </Button>,
          <Button
            key="save-temp"
            onClick={() => {
              const idx = outOfSpecModal.batIdx ?? 0;
              const wasSingle = outOfSpecModal.wasInSingleMode;
              setOutOfSpecModal(null);
              if (!wasSingle) {
                const nextIdx = idx + 1;
                caliperIndexRef.current = nextIdx;
                setCaliperIndex(nextIdx);
                setCaliperDia('');
                setCaliperHei('');
                setCaliperMode('dia');
                if (nextIdx < recordsLengthRef.current) {
                  setCaliperPhase(true);
                }
              }
            }}
          >
            {t('batterySaveTemp')}
          </Button>,
        ] : [
          <Button
            key="retest-now"
            type="primary"
            danger
            onClick={() => {
              const record = outOfSpecModal.record;
              // Increment retry count for this battery
              retestCountMapRef.current[record.id] = (retestCountMapRef.current[record.id] || 0) + 1;
              // After retest completes, auto-restart the full test from the next battery
              autoRestartAfterRetestRef.current = true;
              setOutOfSpecModal(null);
              if (running) {
                // Stop current test then retest once stopped
                pendingRetestRecordRef.current = record;
                sendMsg({ action: 'stop' });
              } else {
                handleRetest(record);
              }
            }}
          >
            {t('batteryRetestNow')}
          </Button>,
          <Button
            key="save-temp"
            onClick={() => {
              setOutOfSpecModal(null);
              // Restart the full test from the next battery (test was stopped when modal appeared)
              setRetestingBatteryId(null);
              sendMsg({ action: 'start', payload: buildParams() });
            }}
          >
            {t('batterySaveTemp')}
          </Button>,
        ]}
      >
        <p>{outOfSpecModal?.parts?.join(', ')}</p>
        {outOfSpecModal?.retryCount > 0 && (
          <p style={{ color: '#ff4d4f', marginTop: 4 }}>
            {t('batteryFailedNTimes', { count: outOfSpecModal.retryCount })}
          </p>
        )}
      </Modal>
    </div>
  );
}
