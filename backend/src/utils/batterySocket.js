/**
 * batterySocket.js
 *
 * WebSocket hub — relays SSE events from Python battery_service to all
 * connected browser clients, and proxies control commands from the browser
 * to the correct Python service.
 *
 * Multi-station support: each WS message carries a `stationId` field.
 * Commands are routed to the matching station URL from stationRegistry.
 * Each active station gets its own independent SSE relay stream.
 */
const { WebSocketServer, WebSocket } = require('ws');
const axios = require('axios');
const logger = require('./logger');
const { resolveUrl, getStations } = require('./stationRegistry');

const PYTHON_BASE = process.env.BATTERY_SERVICE_URL || 'http://127.0.0.1:8765';

let wss = null;

// Map<ws, { token, stationId }>
const clients = new Map();

// Map<stationId, { abortController, connected }>
const stationSseMap = new Map();

// Map<stationId, ws> — tracks which WebSocket client is the active operator for each station.
// Only the operator may send connect/disconnect/start/stop/clear_session.
const stationOperatorMap = new Map();

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** Resolve base URL: stationRegistry first, env fallback. */
function _getBase(stationId) {
  if (stationId) {
    const url = resolveUrl(stationId);
    if (url) return url;
  }
  return PYTHON_BASE;
}

/** Returns true if any connected WS client is using this stationId. */
function _hasClientsForStation(stationId) {
  for (const [, state] of clients) {
    if (state.stationId === stationId) return true;
  }
  return false;
}

/** Returns true if the station currently has a live operator connection. */
function _isOperatorAlive(stationId) {
  const opWs = stationOperatorMap.get(stationId);
  return opWs != null && opWs.readyState === WebSocket.OPEN;
}

/** Returns true if the given WS connection is the active operator for the station. */
function _isOperator(ws, stationId) {
  return stationOperatorMap.get(stationId) === ws;
}

/**
 * Broadcast station occupancy state to all clients of a station.
 * Each client receives its own isOperator flag.
 */
function _broadcastStationState(stationId) {
  const occupied = _isOperatorAlive(stationId);
  const strFalse = JSON.stringify({ type: 'station_state', occupied, isOperator: false, stationId });
  const strTrue = JSON.stringify({ type: 'station_state', occupied, isOperator: true, stationId });
  for (const [ws, state] of clients) {
    if (state.stationId === stationId && ws.readyState === WebSocket.OPEN) {
      const isOp = stationOperatorMap.get(stationId) === ws;
      try { ws.send(isOp ? strTrue : strFalse); } catch (_) {}
    }
  }
}

// ---------------------------------------------------------------------------
// Initialise WebSocket server
// ---------------------------------------------------------------------------

function initBatteryWebSocket(server) {
  wss = new WebSocketServer({ server, path: '/ws/battery' });

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://localhost');
    const token = url.searchParams.get('token') || null;
    clients.set(ws, { token, stationId: null });
    logger.info('Battery WS client connected', { total: clients.size });

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        await handleClientMessage(ws, msg);
      } catch (e) {
        sendToClient(ws, { type: 'error', message: e.message });
      }
    });

    ws.on('close', () => {
      const { stationId } = clients.get(ws) || {};
      clients.delete(ws);
      logger.info('Battery WS client disconnected', { total: clients.size });
      if (stationId) {
        // If the closing client was the operator, release the station
        if (stationOperatorMap.get(stationId) === ws) {
          stationOperatorMap.delete(stationId);
          _broadcastStationState(stationId);
        }
        // Stop SSE relay for this station if no clients remain
        if (!_hasClientsForStation(stationId)) {
          stopSseRelay(stationId);
        }
      }
    });

    ws.on('error', (err) => {
      logger.error('Battery WS error', { error: err.message });
      clients.delete(ws);
    });
  });

  logger.info('Battery WebSocket server initialised at /ws/battery');
}

// ---------------------------------------------------------------------------
// Message dispatcher
// ---------------------------------------------------------------------------

async function handleClientMessage(ws, msg) {
  const { action, stationId, payload } = msg;
  const base = _getBase(stationId);

  // Bind this WS connection to the selected station
  if (stationId) {
    const state = clients.get(ws);
    if (state) state.stationId = stationId;
  }

  // ── get_stations ──────────────────────────────────────────────────────────
  if (action === 'get_stations') {
    sendToClient(ws, { type: 'stations', stations: getStations() });
    return;
  }

  // ── connect ───────────────────────────────────────────────────────────────
  if (action === 'connect') {
    // If another client is already operating this station, reject with view-only notice
    if (stationId && _isOperatorAlive(stationId) && !_isOperator(ws, stationId)) {
      sendToClient(ws, {
        type: 'connect_result',
        ok: false,
        occupied: true,
        message: 'Trạm đang được sử dụng bởi máy khác. Bạn đang ở chế độ xem.',
        stationId,
      });
      // Ensure SSE relay is running so this viewer receives live data
      startSseRelay(stationId, base);
      // Tell the viewer they are in view-only mode
      sendToClient(ws, { type: 'station_state', occupied: true, isOperator: false, stationId });
      return;
    }
    try {
      const res = await axios.post(`${base}/connect`, payload || {});
      // Mark this WS connection as the station operator
      stationOperatorMap.set(stationId, ws);
      broadcastToStation(stationId, {
        type: 'connect_result',
        ok: true,
        message: res.data.message,
        stationId,
      });
      // Inform all clients of the new operator status
      _broadcastStationState(stationId);
      startSseRelay(stationId, base);
    } catch (e) {
      const detail = e.response?.data?.detail || e.message;
      broadcastToStation(stationId, {
        type: 'connect_result',
        ok: false,
        message: detail,
        stationId,
      });
    }
    return;
  }

  // ── disconnect ────────────────────────────────────────────────────────────
  if (action === 'disconnect') {
    if (stationId && !_isOperator(ws, stationId)) {
      sendToClient(ws, { type: 'error', message: 'Bạn không phải người đang vận hành trạm này.', stationId });
      return;
    }
    try { await axios.post(`${base}/disconnect`); } catch (_) {}
    stationOperatorMap.delete(stationId);
    stopSseRelay(stationId);
    broadcastToStation(stationId, { type: 'disconnected', stationId });
    _broadcastStationState(stationId);
    return;
  }

  // ── start ─────────────────────────────────────────────────────────────────
  if (action === 'start') {
    if (stationId && !_isOperator(ws, stationId)) {
      sendToClient(ws, { type: 'error', message: 'Bạn không phải người đang vận hành trạm này. Chỉ máy kết nối thiết bị mới được phép bắt đầu kiểm tra.', stationId });
      return;
    }
    // Ensure SSE relay is running (may have stopped after page reload)
    startSseRelay(stationId, base);
    try {
      await axios.post(`${base}/start`, payload || {});
      broadcastToStation(stationId, { type: 'test_started', stationId });
      // Sync current Python session state (records) to the frontend so that
      // any records accumulated while the browser was disconnected appear immediately.
      try {
        const statusRes = await axios.get(`${base}/status`);
        if (statusRes.data && Array.isArray(statusRes.data.records) && statusRes.data.records.length > 0) {
          broadcastToStation(stationId, { type: 'status', data: statusRes.data, stationId });
        }
      } catch (statusErr) {
        logger.debug('Battery status sync after start failed', { stationId, error: statusErr.message });
      }
    } catch (e) {
      const detail = e.response?.data?.detail || e.message;
      const isAlreadyRunning = e.response?.status === 400 && detail && detail.includes('running');
      if (isAlreadyRunning) {
        // Test is already running (e.g. after page reload) — treat as a reconnect:
        // tell the frontend the test is running and sync existing records.
        broadcastToStation(stationId, { type: 'test_started', stationId });
        try {
          const statusRes = await axios.get(`${base}/status`);
          if (statusRes.data) {
            broadcastToStation(stationId, { type: 'status', data: statusRes.data, stationId });
          }
        } catch (statusErr) {
          logger.debug('Battery status sync on reconnect failed', { stationId, error: statusErr.message });
        }
      } else {
        broadcastToStation(stationId, { type: 'error', message: detail, stationId });
      }
    }
    return;
  }

  // ── stop ──────────────────────────────────────────────────────────────────
  if (action === 'stop') {
    if (stationId && !_isOperator(ws, stationId)) {
      sendToClient(ws, { type: 'error', message: 'Bạn không phải người đang vận hành trạm này.', stationId });
      return;
    }
    try {
      await axios.post(`${base}/stop`);
      broadcastToStation(stationId, { type: 'test_stopped', stationId });
    } catch (e) {
      const detail = e.response?.data?.detail || e.message;
      broadcastToStation(stationId, { type: 'error', message: detail, stationId });
    }
    return;
  }

  // ── get_ports ─────────────────────────────────────────────────────────────
  if (action === 'get_ports') {
    try {
      const res = await axios.get(`${base}/ports`);
      sendToClient(ws, { type: 'ports', ports: res.data.ports, stationId });
    } catch (e) {
      sendToClient(ws, { type: 'ports', ports: [], error: e.message, stationId });
    }
    return;
  }

  // ── get_status ────────────────────────────────────────────────────────────
  if (action === 'get_status') {
    try {
      const res = await axios.get(`${base}/status`);
      sendToClient(ws, { type: 'status', data: res.data, stationId });
      // If a test is running, make sure the SSE relay is active for this station
      if (res.data && res.data.running) {
        startSseRelay(stationId, base);
      }
    } catch (e) {
      sendToClient(ws, { type: 'error', message: e.message, stationId });
    }
    // Always send current station occupancy state to this specific client
    if (stationId) {
      const occupied = _isOperatorAlive(stationId);
      const isOp = _isOperator(ws, stationId);
      sendToClient(ws, { type: 'station_state', occupied, isOperator: isOp, stationId });
    }
    return;
  }

  // ── clear_session ─────────────────────────────────────────────────────────
  if (action === 'clear_session') {
    if (stationId && !_isOperator(ws, stationId)) {
      sendToClient(ws, { type: 'error', message: 'Bạn không phải người đang vận hành trạm này. Chỉ máy kết nối thiết bị mới được phép xóa phiên.', stationId });
      return;
    }
    try {
      await axios.delete(`${base}/session`);
      broadcastToStation(stationId, { type: 'session_cleared', stationId });
    } catch (e) {
      sendToClient(ws, { type: 'error', message: e.message, stationId });
    }
    return;
  }
}

// ---------------------------------------------------------------------------
// Per-station SSE relay
// ---------------------------------------------------------------------------

function startSseRelay(stationId, base) {
  const existing = stationSseMap.get(stationId);
  if (existing && existing.connected) return; // already running

  const abortController = new AbortController();
  stationSseMap.set(stationId, { abortController, connected: true });

  (async () => {
    try {
      const response = await axios.get(`${base}/stream`, {
        responseType: 'stream',
        signal: abortController.signal,
        timeout: 0, // no timeout on streaming
      });

      let buffer = '';

      response.data.on('data', (chunk) => {
        buffer += chunk.toString();
        const parts = buffer.split('\n\n');
        buffer = parts.pop(); // keep incomplete last chunk
        for (const part of parts) {
          if (!part.trim()) continue;
          const dataLine = part.split('\n').find((l) => l.startsWith('data:'));
          if (!dataLine) continue;
          try {
            const json = JSON.parse(dataLine.slice(5).trim());
            // Attach stationId so the browser can filter messages
            broadcastToStation(stationId, { ...json, stationId });
          } catch (_) {}
        }
      });

      response.data.on('end', () => {
        const s = stationSseMap.get(stationId);
        if (s) s.connected = false;
        logger.info('Battery SSE stream ended', { stationId });
      });

      response.data.on('error', (err) => {
        const s = stationSseMap.get(stationId);
        if (s) s.connected = false;
        logger.warn('Battery SSE stream error', { stationId, error: err.message });
      });
    } catch (e) {
      const s = stationSseMap.get(stationId);
      if (s) s.connected = false;
      if (e.code !== 'ERR_CANCELED') {
        logger.warn('Battery SSE relay failed', { stationId, error: e.message });
      }
    }
  })();
}

function stopSseRelay(stationId) {
  const s = stationSseMap.get(stationId);
  if (s && s.abortController) {
    s.abortController.abort();
  }
  stationSseMap.delete(stationId);
}

// ---------------------------------------------------------------------------
// Broadcast helpers
// ---------------------------------------------------------------------------

/** Send to all WS clients that are currently working with this stationId. */
function broadcastToStation(stationId, data) {
  const str = JSON.stringify(data);
  for (const [ws, state] of clients) {
    if (state.stationId === stationId) {
      try {
        if (ws.readyState === WebSocket.OPEN) ws.send(str);
      } catch (_) {}
    }
  }
}

/** Send to a specific WS client only. */
function sendToClient(ws, data) {
  try {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
  } catch (_) {}
}

module.exports = { initBatteryWebSocket };
