import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Card, Form, Select, Input, InputNumber, DatePicker, Button, Table, Tabs,
  Badge, notification, Tooltip, Space, Row, Col, Divider, Tag, Checkbox,
  Upload, Collapse, Modal, Popover, Radio,
} from 'antd';
import {
  ReloadOutlined, DownloadOutlined, DeleteOutlined, PlayCircleOutlined,
  StopOutlined, DisconnectOutlined, ApiOutlined, InboxOutlined, QuestionCircleOutlined,
  ExportOutlined, FullscreenOutlined, InfoCircleOutlined,
} from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import dayjs from 'dayjs';
import { useLang } from '../../contexts/LangContext';
import { uploadTemplate, getTemplateInfo, downloadReportFromTemplate, uploadArchive, getArchiveInfo, downloadArchiveReport, getStations, getBatteryTypes, createBatteryType, deleteBatteryType, getBatteryProductLines, createBatteryProductLine, deleteBatteryProductLine, getBatteryPresets, upsertBatteryPreset, deleteBatteryPreset, getOrderHistory, saveOrderHistorySnapshot, deleteOrderHistorySnapshot, clearOrderHistory } from '../../api/battery';

const { Option } = Select;
const { RangePicker } = DatePicker;

const STATUS_COLORS = {
  'Waiting...': '#ffffff',
  'Testing...': '#00e5ff',
  'Done': '#69f0ae',
  'Remove': '#69f0ae',
  'Saving...': '#ffee58',
  'Stopped': '#9e9e9e',
  'Error': '#ef5350',
};

function getStatusColor(text) {
  if (!text) return '#ffffff';
  for (const [key, color] of Object.entries(STATUS_COLORS)) {
    if (text.includes(key)) return color;
  }
  return '#ffffff';
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

function RowWithPopover({ record, readingsByBattery, buildMiniChartOption, ...rowProps }) {
  const hasReadings = record && readingsByBattery && (readingsByBattery[record.id] || []).length > 0;
  if (!hasReadings) {
    return <tr {...rowProps} />;
  }
  const popoverContent = (
    <div style={{ width: 900, background: '#1a1a1a', borderRadius: 6, padding: 4 }}>
      <ReactECharts
        option={buildMiniChartOption(record.id)}
        style={{ height: 450, width: 900 }}
        notMerge
        theme="dark"
      />
    </div>
  );
  return (
    <Popover
      content={popoverContent}
      overlayInnerStyle={{ background: '#1a1a1a', padding: 0 }}
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
  const [presets, setPresets] = useState({});
  const [batteryTypes, setBatteryTypes] = useState([]);
  const [productLines, setProductLines] = useState([]);
  const [setupForm, setSetupForm] = useState({
    batteryType: '', productLine: '',
    resistance: 3.9, ocvTime: 1.0, loadTime: 0.3, kCoeff: 1.0,
    ocvMin: null, ocvMax: null, ccvMin: null, ccvMax: null,
  });
  const [newTypeName, setNewTypeName] = useState('');
  const [newLineName, setNewLineName] = useState('');
  const [typeLineLoading, setTypeLineLoading] = useState(false);

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

  // Results
  const [records, setRecords] = useState(() => getInitialSession().records || []);

  // Out-of-spec blocking modal
  const [outOfSpecModal, setOutOfSpecModal] = useState(null);

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
      setOrderHistory(res.data.items || []);
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

  // Order ID change warning modal
  const [orderIdChangeModalVisible, setOrderIdChangeModalVisible] = useState(false);
  const [pendingOrderId, setPendingOrderId] = useState('');

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
            setRecords(prev => {
              if (idx >= prev.length) return prev;
              const updated = [...prev];
              const rec = { ...updated[idx] };
              if (diaVal !== '') rec.dia = parseFloat(diaVal);
              rec.hei = parseFloat(heiVal);
              updated[idx] = rec;
              return updated;
            });
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
    date: testDate ? testDate.format('YYYY-MM') : dayjs().format('YYYY-MM'),
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
          // When a retest result comes in, check if it's still out of spec
          if (msg.record.is_retest) {
            const rec = enrichedRecord;
            const spec_ocv = ocvSpecRef.current;
            const spec_ccv = ccvSpecRef.current;
            const ocvBad = spec_ocv && rec.ocv !== null && rec.ocv !== undefined && (rec.ocv < spec_ocv.min || rec.ocv > spec_ocv.max);
            const ccvBad = spec_ccv && rec.ccv !== null && rec.ccv !== undefined && (rec.ccv < spec_ccv.min || rec.ccv > spec_ccv.max);
            if (ocvBad || ccvBad) {
              const retryCount = retestCountMapRef.current[rec.id] || 0;
              const parts = [];
              if (ocvBad) parts.push(`OCV ${rec.ocv.toFixed(3)}V (spec: ${spec_ocv.min} - ${spec_ocv.max})`);
              if (ccvBad) parts.push(`CCV ${rec.ccv.toFixed(3)}V (spec: ${spec_ccv.min} - ${spec_ccv.max})`);
              setOutOfSpecModal({ record: rec, parts, retryCount });
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
  useEffect(() => { chartSeriesByBatteryRef.current = chartSeriesByBattery; }, [chartSeriesByBattery]);
  useEffect(() => { readingsByBatteryRef.current = readingsByBattery; }, [readingsByBattery]);
  useEffect(() => { testDateRef.current = testDate; }, [testDate]);

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
      testDate: testDateRef.current ? testDateRef.current.format('YYYY-MM') : null,
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
      setOrderHistory(prev => {
        const entry = {
          _snapshotId: savedId,
          _savedAt: savedAt,
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
    resetLoadedSnapshotTracking();
  }, [saveCurrentOrderSnapshot, sendMsg, resetLoadedSnapshotTracking]);

  // Order ID change: end current session and start fresh with new ID
  const handleEndSessionForOrderIdChange = () => {
    handleClearSession();
    setOrderId(pendingOrderId);
    setPendingOrderId('');
    setOrderIdChangeModalVisible(false);
  };

  // Order ID change: keep all results and re-tag every record with the new ID
  const handleRenameAllOrderId = () => {
    const newId = pendingOrderId;
    setOrderId(newId);
    setPendingOrderId('');
    setOrderIdChangeModalVisible(false);
  };

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
      const date = testDate ? testDate.format('YYYY-MM') : dayjs().format('YYYY-MM');
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
      const date = testDate ? testDate.format('YYYY-MM') : dayjs().format('YYYY-MM');
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
  // During a retest, show only that battery's (fresh) chart data
  const latestBatteryIds = retestingBatteryId !== null
    ? [retestingBatteryId]
    : (allBatteryIds.length > 0 ? [allBatteryIds[allBatteryIds.length - 1]] : []);
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
  if (allBatteryIds.length === 0 && (chartDataOCV.length > 0 || chartDataCCV.length > 0)) {
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
    backgroundColor: 'transparent',
    grid: { top: 36, right: 24, bottom: 40, left: 56 },
    legend: {
      top: 4,
      data: ['OCV', 'CCV'],
      selected: legendSelected,
      textStyle: { color: '#aaa', fontSize: 12 },
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
    { title: t('batteryCaliperDia'), dataIndex: 'dia', key: 'dia', width: 80, render: (v) => v != null ? parseFloat(v).toFixed(2) : '-' },
    { title: t('batteryCaliperHei'), dataIndex: 'hei', key: 'hei', width: 80, render: (v) => v != null ? parseFloat(v).toFixed(2) : '-' },
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
                setCaliperIndex(recordIdx >= 0 ? recordIdx : 0);
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
    { title: t('status'), dataIndex: 'status', key: 'status', width: 100, render: (value) => value ? <Tag color="blue">{value}</Tag> : '-' },
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
      width: 140,
      render: (value) => <strong>{value || '-'}</strong>,
    },
    {
      title: t('batteryTestMonth'),
      dataIndex: 'testDate',
      key: 'testDate',
      width: 110,
      render: (value) => value || '-',
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
  ]), [t, loadedSnapshotId, handleLoadOrder, handleDeleteOrderSnapshot, handleExportOrderSnapshot]);

  const buildMiniChartOption = React.useCallback((batteryId) => {
    const readings = readingsByBattery[batteryId] || [];
    const ocvData = readings.filter(r => r.phase === 'ocv').map(r => [r.t, r.v]);
    const ccvData = readings.filter(r => r.phase === 'ccv').map(r => [r.t, r.v]);
    const ccvDataConnected = ocvData.length > 0 && ccvData.length > 0
      ? [ocvData[ocvData.length - 1], ...ccvData]
      : ccvData;
    return {
      backgroundColor: 'transparent',
      grid: { top: 20, right: 16, bottom: 24, left: 48 },
      tooltip: { trigger: 'axis', formatter: (params) => params.map(p => `${p.marker}${p.seriesName}: ${p.value[1]?.toFixed(3)}V @ ${p.value[0]}s`).join('<br/>') },
      xAxis: {
        type: 'value',
        name: 's',
        axisLabel: { color: '#aaa', fontSize: 10 },
        splitLine: { lineStyle: { color: '#2a2a2a' } },
      },
      yAxis: {
        type: 'value',
        name: 'V',
        scale: true,
        axisLabel: { color: '#aaa', fontSize: 10 },
        splitLine: { lineStyle: { color: '#2a2a2a' } },
      },
      series: [
        { name: 'OCV', type: 'line', data: ocvData, symbol: 'none', lineStyle: { color: '#ffee58', width: 1.5 } },
        { name: 'CCV', type: 'line', data: ccvDataConnected, symbol: 'none', lineStyle: { color: '#0091ea', width: 1.5 } },
      ],
    };
  }, [readingsByBattery]);

  const prevRecordsLenRef = useRef(records.length);
  const resultsTableRef = useRef(null);
  useEffect(() => {
    if (records.length <= prevRecordsLenRef.current) return;
    prevRecordsLenRef.current = records.length;
    const latest = records[records.length - 1];
    if (!latest) return;
    const ocvBad = ocvSpec && latest.ocv != null && (latest.ocv < ocvSpec.min || latest.ocv > ocvSpec.max);
    const ccvBad = ccvSpec && latest.ccv != null && (latest.ccv < ccvSpec.min || latest.ccv > ccvSpec.max);
    if (ocvBad || ccvBad) {
      const parts = [];
      if (ocvBad) parts.push(`OCV ${latest.ocv.toFixed(3)}V (spec: ${ocvSpec.min} - ${ocvSpec.max})`);
      if (ccvBad) parts.push(`CCV ${latest.ccv.toFixed(3)}V (spec: ${ccvSpec.min} - ${ccvSpec.max})`);
      // Show blocking modal instead of dismissible notification
      setOutOfSpecModal({ record: latest, parts, retryCount: 0 });
      // Bug fix: stop the test immediately so it doesn't advance to the next battery
      if (runningRef.current) {
        sendMsg({ action: 'stop' });
      }
    }
    // Auto-scroll the results table to the latest record
    if (resultsTableRef.current) {
      const tableBody = resultsTableRef.current.querySelector('.ant-table-body');
      if (tableBody) tableBody.scrollTop = tableBody.scrollHeight;
    }
  }, [records, ocvSpec, ccvSpec, t, sendMsg]);

  useEffect(() => {
    try {
      const sessionData = {
        records,
        chartData,
        chartDataOCV,
        chartDataCCV,
        orderId,
        testDate: testDate ? testDate.format('YYYY-MM') : null,
        batteryType,
        productLine,
        ocvMin,
        ocvMax,
        ccvMin,
        ccvMax,
        chartSeriesByBattery,
        readingsByBattery,
      };
      localStorage.setItem('battery_session', JSON.stringify(sessionData));
    } catch { }
  }, [records, chartData, chartDataOCV, chartDataCCV, orderId, testDate, batteryType, productLine, ocvMin, ocvMax, ccvMin, ccvMax, chartSeriesByBattery, readingsByBattery]);

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
    }
  }, [batteryType, productLine, presets]);

  const inputsDisabled = !connected;
  const hasPreset = presets[makePresetKey(batteryType, productLine)] != null;
  const paramsDisabled = inputsDisabled || hasPreset;
  const canStart = connected && !running && orderId.trim() !== '' && testDate !== null && ocvMin != null && ocvMax != null && ccvMin != null && ccvMax != null;

  return (
    <div>
      <style>{`.battery-row-bad td { background: rgba(255,77,79,0.12) !important; }`}</style>
      {/* Station Selector */}
      <div style={{ marginBottom: 16 }}>
        <Card
          size="small"
          style={{ borderColor: selectedStation ? '#177ddc' : '#434343', background: '#141414' }}
          title={
            <Space>
              <span>🏭</span>
              <span style={{ color: '#fff' }}>Chọn trạm kiểm tra</span>
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
      <div style={{ marginBottom: 16 }}>
        <Tabs
          activeKey={pageTab}
          onChange={setPageTab}
          size="large"
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
                    <Form.Item label={t('batteryOrderId')} style={{ marginBottom: 0 }}>
                      <Input
                        value={orderId}
                        onChange={(e) => {
                          const newVal = e.target.value;
                          if (records.length > 0 && newVal !== orderId) {
                            setPendingOrderId(newVal);
                            setOrderIdChangeModalVisible(true);
                          } else {
                            setOrderId(newVal);
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
                        picker="month"
                        value={testDate}
                        onChange={setTestDate}
                        disabled={inputsDisabled}
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
                        onChange={setBatteryType}
                        disabled={inputsDisabled}
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
                        onChange={setProductLine}
                        disabled={inputsDisabled}
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
                </Row>
              </Card>
            </Col>
          </Row>

          {/* Status Bar */}
          <div
            style={{
              background: '#000',
              borderRadius: 8,
              padding: '12px 20px',
              marginBottom: 16,
              color: statusColor,
              fontSize: 18,
              fontWeight: 600,
              fontFamily: 'monospace',
              letterSpacing: 1,
              minHeight: 48,
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
                <span>
                  📏 {t('batteryCaliperSection')}
                  {records.length > 0 && records[caliperIndex] != null && (
                    <span style={{ fontSize: 12, color: caliperSingleMode ? '#faad14' : '#69b1ff', marginLeft: 8 }}>
                      {caliperSingleMode
                        ? `(Re-measure: ${t('batteryId')} ${records[caliperIndex].id})`
                        : `(${t('batteryId')}: ${records[caliperIndex].id} / ${records.length})`
                      }
                    </span>
                  )}
                </span>
              }
              style={{ marginBottom: 16 }}
            >
              <Space direction="horizontal" wrap>
                <Space direction="vertical" size={2}>
                  <span style={{ fontSize: 12, color: '#aaa' }}>{t('batteryCaliperMode')}</span>
                  <Radio.Group
                    value={caliperMode}
                    onChange={(e) => setCaliperMode(e.target.value)}
                    buttonStyle="solid"
                    size="small"
                  >
                    <Radio.Button value="dia">{t('batteryCaliperModeDia')}</Radio.Button>
                    <Radio.Button value="hei">{t('batteryCaliperModeHei')}</Radio.Button>
                  </Radio.Group>
                </Space>
                <Space direction="vertical" size={2}>
                  <span style={{ fontSize: 12, color: '#aaa' }}>{t('batteryCaliperBuffer')}</span>
                  <Input
                    ref={caliperInputRef}
                    size="small"
                    value={caliperBuffer}
                    placeholder={t('batteryCaliperBuffer')}
                    style={{ width: 160, fontFamily: 'monospace', background: caliperBuffer ? '#1a3a1a' : undefined }}
                    readOnly
                  />
                </Space>
                <Space direction="vertical" size={2}>
                  <span style={{ fontSize: 12, color: '#aaa' }}>{t('batteryCaliperDia')} (mm)</span>
                  <InputNumber
                    size="small"
                    value={caliperDia ? parseFloat(caliperDia) : null}
                    onChange={(v) => setCaliperDia(v != null ? String(v) : '')}
                    step={0.01}
                    style={{ width: 100 }}
                    placeholder="—"
                  />
                </Space>
                <Space direction="vertical" size={2}>
                  <span style={{ fontSize: 12, color: '#aaa' }}>{t('batteryCaliperHei')} (mm)</span>
                  <InputNumber
                    size="small"
                    value={caliperHei ? parseFloat(caliperHei) : null}
                    onChange={(v) => setCaliperHei(v != null ? String(v) : '')}
                    step={0.01}
                    style={{ width: 100 }}
                    placeholder="—"
                  />
                </Space>
                <Tooltip title={t('batteryCaliperHint')}>
                  <QuestionCircleOutlined style={{ color: '#888', marginTop: 20 }} />
                </Tooltip>
              </Space>
              <div style={{ marginTop: 6, fontSize: 11, color: '#666' }}>
                💡 {t('batteryCaliperHint')}
              </div>
              <div style={{ marginTop: 8 }}>
                <Button
                  onClick={handleSaveCaliper}
                >
                  {t('batteryCaliperSkip')}
                </Button>
                <Button
                  style={{ marginLeft: 8 }}
                  onClick={handleResetCaliper}
                >
                  {t('cancel')}
                </Button>
              </div>
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
                bodyStyle={{ padding: 8, background: '#111', borderRadius: '0 0 8px 8px' }}
              >
                <ReactECharts
                  option={chartOption}
                  style={{ height: 280 }}
                  notMerge={true}
                  lazyUpdate={true}
                  theme="dark"
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
                    return (ocvBad || ccvBad) ? 'battery-row-bad' : '';
                  }}
                  components={{
                    body: {
                      row: (rowProps) => {
                        const record = recordsMap[String(rowProps['data-row-key'])];
                        return <RowWithPopover record={record} readingsByBattery={readingsByBattery} buildMiniChartOption={buildMiniChartOption} {...rowProps} />;
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
                    onChange={(v) => setSetupForm(f => ({ ...f, batteryType: v }))}
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
                    onChange={(v) => setSetupForm(f => ({ ...f, productLine: v }))}
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
            saveCurrentOrderSnapshot();
            localStorage.removeItem('battery_session');
            setRecords([]);
            setChartData([]);
            setChartDataOCV([]);
            setChartDataCCV([]);
            setChartSeriesByBattery({});
            setReadingsByBattery({});
            setOrderId('');
            setTestDate(dayjs());
            setBatteryType(batteryTypes[0]?.name || '');
            setProductLine(productLines[0]?.name || '');
            setOcvMin(null);
            setOcvMax(null);
            setCcvMin(null);
            setCcvMax(null);
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
      {/* Order ID Change Warning Modal */}
      <Modal
        open={orderIdChangeModalVisible}
        title={<Space><span style={{ color: '#faad14' }}>⚠️</span><span>{t('batteryOrderIdChangeTitle')}</span></Space>}
        closable={false}
        maskClosable={false}
        keyboard={false}
        footer={[
          <Button key="cancel" onClick={() => { setPendingOrderId(''); setOrderIdChangeModalVisible(false); }}>{t('cancel')}</Button>,
          <Button key="endSession" danger onClick={handleEndSessionForOrderIdChange}>{t('batteryOrderIdChangeEndSession')}</Button>,
          <Button key="renameAll" type="primary" onClick={handleRenameAllOrderId}>{t('batteryOrderIdChangeRenameAll')}</Button>,
        ]}
      >
        <p>{t('batteryOrderIdChangeDesc')}</p>
        <ul>
          <li><strong>{t('batteryOrderIdChangeCurrent')}:</strong> {orderId || '-'}</li>
          <li><strong>{t('batteryOrderIdChangeNew')}:</strong> {pendingOrderId || '-'}</li>
          <li><strong>{t('batteryResults')}:</strong> {records.length} {t('batteryId')}</li>
        </ul>
        <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
          <div style={{ background: '#fff1f0', border: '1px solid #ffccc7', borderRadius: 6, padding: '8px 12px' }}>
            <strong style={{ color: '#cf1322' }}>{t('batteryOrderIdChangeEndSession')}:</strong>
            <span style={{ marginLeft: 6 }}>{t('batteryOrderIdChangeEndSessionDesc')}</span>
          </div>
          <div style={{ background: '#f6ffed', border: '1px solid #b7eb8f', borderRadius: 6, padding: '8px 12px' }}>
            <strong style={{ color: '#389e0d' }}>{t('batteryOrderIdChangeRenameAll')}:</strong>
            <span style={{ marginLeft: 6 }}>{t('batteryOrderIdChangeRenameAllDesc')}</span>
          </div>
        </div>
        <p style={{ color: '#ff4d4f', marginTop: 8 }}>{t('batteryOrderIdChangeWarning')}</p>
      </Modal>
      {/* Chart Zoom Modal */}
      <Modal
        open={chartZoomVisible}
        onCancel={() => setChartZoomVisible(false)}
        footer={null}
        width="90vw"
        title={t('batteryChart')}
        destroyOnClose
        bodyStyle={{ background: '#111', padding: 8 }}
      >
        <ReactECharts
          option={{ ...chartOption, dataZoom: ZOOM_CHART_DATA_ZOOM }}
          style={{ height: 'calc(80vh - 60px)' }}
          notMerge={true}
          theme="dark"
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
            return (ocvBad || ccvBad) ? 'battery-row-bad' : '';
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
        footer={[
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
