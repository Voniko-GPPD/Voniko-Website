const express = require('express');
const axios = require('axios');
const { authenticateToken } = require('../middleware/auth');
const { upsertStation, getStations, resolveUrl } = require('../utils/stationRegistry');
const logger = require('../utils/logger');

const router = express.Router();

// POST /api/dmp/register — called by dmp_service.py heartbeat, no auth
router.post('/register', (req, res) => {
  const { name, url } = req.body || {};
  if (!name || !url) return res.status(400).json({ error: 'name and url are required' });
  try {
    const parsed = new URL(url);
    if (!['http:', 'https:'].includes(parsed.protocol)) {
      return res.status(400).json({ error: 'url must be http(s)' });
    }
  } catch (_) {
    return res.status(400).json({ error: 'Invalid url' });
  }
  const id = upsertStation(name, url);
  logger.info('DMP station registered', { id, name });
  res.json({ ok: true, id });
});

// GET /api/dmp/stations — list all registered DMP stations
router.get('/stations', authenticateToken, (req, res) => {
  res.json({ stations: getStations() });
});

// Helper: resolve station URL or return 404
function getStationUrl(stationId, res) {
  if (!stationId) {
    res.status(400).json({ error: 'stationId is required' });
    return null;
  }
  const url = resolveUrl(stationId);
  if (!url) {
    res.status(404).json({ error: 'Station not found or offline' });
    return null;
  }
  return url;
}

/**
 * Unified proxy error handler for DMP station routes.
 * - Forwards HTTP error responses from the station as-is.
 * - Returns 503 when the station is unreachable (network/timeout error).
 * - Delegates unexpected errors to Express's next() handler.
 * @param {Error} err - Axios error
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 */
function handleProxyError(err, res, next) {
  if (err.response) return res.status(err.response.status).json(err.response.data);
  if (err.request) return res.status(503).json({ error: 'DMP station unreachable' });
  next(err);
}

// GET /api/dmp/batches?stationId=
router.get('/batches', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/batches`, { timeout: 30000 });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// GET /api/dmp/batches/:batchId/channels?stationId=
router.get('/batches/:batchId/channels', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/batches/${encodeURIComponent(req.params.batchId)}/channels`, { timeout: 30000 });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// GET /api/dmp/telemetry?stationId=&cdmc=&channel=
router.get('/telemetry', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/telemetry`, {
      params: { cdmc: req.query.cdmc, channel: req.query.channel },
      timeout: 120000,
    });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// GET /api/dmp/changes?stationId=&since=
router.get('/changes', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/changes`, {
      params: { since: req.query.since },
      timeout: 15000,
    });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// GET /api/dmp/stats?stationId=&cdmc=&channel=
router.get('/stats', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/stats`, {
      params: { cdmc: req.query.cdmc, channel: req.query.channel },
      timeout: 120000,
    });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// GET /api/dmp/templates?stationId=
router.get('/templates', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/templates`, { timeout: 15000 });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// POST /api/dmp/report — proxy xlsx binary download
router.post('/report', authenticateToken, async (req, res, next) => {
  const { stationId, ...reportBody } = req.body || {};
  const stationUrl = getStationUrl(stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.post(`${stationUrl}/report`, reportBody, {
      responseType: 'arraybuffer',
      timeout: 60000,
    });
    const disposition = r.headers['content-disposition'] || 'attachment; filename="dmp_report.xlsx"';
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', disposition);
    res.send(Buffer.from(r.data));
  } catch (err) {
    if (err.response) {
      const msg = Buffer.from(err.response.data).toString('utf8');
      try {
        return res.status(err.response.status).json(JSON.parse(msg));
      } catch {
        return res.status(err.response.status).send(msg);
      }
    }
    if (err.request) return res.status(503).json({ error: 'DMP station unreachable' });
    next(err);
  }
});

// ─── DM2000 Historic Database Proxy Routes ───────────────────────────────

router.get('/dm2000/archives', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives`, {
      params: {
        date_from: req.query.date_from,
        date_to: req.query.date_to,
        type_filter: req.query.type_filter,
        name_filter: req.query.name_filter,
        mfr_filter: req.query.mfr_filter,
        serial_filter: req.query.serial_filter,
        limit: req.query.limit,
      },
      timeout: 90000,
    });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/archives/:archname/batteries', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/batteries`, { timeout: 15000 });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/archives/:archname/curve', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/curve`, {
      params: { baty: req.query.baty },
      timeout: 120000,
    });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/archives/:archname/average-curve', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/average-curve`, { timeout: 180000 });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/archives/:archname/stats', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/stats`, {
      params: { baty: req.query.baty },
      timeout: 120000,
    });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/archives/:archname/daily-voltage', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/daily-voltage`, {
      params: { baty: req.query.baty },
      timeout: 60000,
    });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/archives/:archname/time-at-voltage', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/time-at-voltage`, {
      params: { baty: req.query.baty },
      timeout: 15000,
    });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.get('/dm2000/templates', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/templates`, { timeout: 10000 });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

router.post('/dm2000/report', authenticateToken, async (req, res, next) => {
  const { stationId, ...reportBody } = req.body || {};
  const stationUrl = getStationUrl(stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.post(`${stationUrl}/dm2000/report`, reportBody, {
      responseType: 'arraybuffer',
      timeout: 60000,
    });
    const disposition = r.headers['content-disposition'] || 'attachment; filename="dm2000_report.xlsx"';
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', disposition);
    res.send(Buffer.from(r.data));
  } catch (err) {
    if (err.response) {
      const msg = Buffer.from(err.response.data).toString('utf8');
      try { return res.status(err.response.status).json(JSON.parse(msg)); }
      catch { return res.status(err.response.status).send(msg); }
    }
    if (err.request) return res.status(503).json({ error: 'DMP station unreachable' });
    next(err);
  }
});

module.exports = router;
