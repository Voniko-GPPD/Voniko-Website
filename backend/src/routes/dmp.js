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

// GET /api/dmp/batches?stationId=
router.get('/batches', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/batches`, { timeout: 30000 });
    res.json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    next(err);
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
    if (err.response) return res.status(err.response.status).json(err.response.data);
    next(err);
  }
});

// GET /api/dmp/telemetry?stationId=&cdmc=&channel=
router.get('/telemetry', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/telemetry`, {
      params: { cdmc: req.query.cdmc, channel: req.query.channel },
      timeout: 30000,
    });
    res.json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    next(err);
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
    if (err.response) return res.status(err.response.status).json(err.response.data);
    next(err);
  }
});

// GET /api/dmp/stats?stationId=&cdmc=&channel=
router.get('/stats', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/stats`, {
      params: { cdmc: req.query.cdmc, channel: req.query.channel },
      timeout: 30000,
    });
    res.json(r.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    next(err);
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
    if (err.response) return res.status(err.response.status).json(err.response.data);
    next(err);
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
    next(err);
  }
});

module.exports = router;
