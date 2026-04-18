const express = require('express');
const axios = require('axios');
const { authenticateToken } = require('../middleware/auth');
const { upsertStation, getStations, resolveUrl } = require('../utils/stationRegistry');

const router = express.Router();

router.post('/register', (req, res) => {
  const { name, url } = req.body || {};
  if (!name || !url) {
    return res.status(400).json({ error: 'name and url required' });
  }
  const id = upsertStation(name, url);
  return res.json({ id });
});

router.get('/stations', authenticateToken, (req, res) => {
  res.json({ stations: getStations() });
});

function resolveOnlineStationUrl(stationId) {
  if (!stationId) {
    throw Object.assign(new Error('stationId is required'), { status: 400 });
  }

  const station = getStations().find((entry) => entry.id === stationId);
  if (!station || !station.online) {
    throw Object.assign(new Error('Station not found or offline'), { status: 404 });
  }

  const stationUrl = resolveUrl(stationId);
  if (!stationUrl) {
    throw Object.assign(new Error('Station not found or offline'), { status: 404 });
  }

  return stationUrl;
}

async function proxyGet(stationUrl, path, queryParams, res, next) {
  try {
    const url = `${stationUrl}${path}`;
    const response = await axios.get(url, { params: queryParams, timeout: 30000 });
    res.json(response.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    return next(err);
  }
}

async function proxyPost(stationUrl, path, body, res, next) {
  try {
    const url = `${stationUrl}${path}`;
    const response = await axios.post(url, body, { timeout: 60000 });
    res.json(response.data);
  } catch (err) {
    if (err.response) return res.status(err.response.status).json(err.response.data);
    return next(err);
  }
}

router.get('/batches', authenticateToken, async (req, res, next) => {
  try {
    const stationUrl = resolveOnlineStationUrl(req.query.stationId);
    await proxyGet(stationUrl, '/batches', {}, res, next);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

router.get('/batches/:batchId/channels', authenticateToken, async (req, res, next) => {
  try {
    const stationUrl = resolveOnlineStationUrl(req.query.stationId);
    await proxyGet(stationUrl, `/batches/${encodeURIComponent(req.params.batchId)}/channels`, {}, res, next);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

router.get('/telemetry', authenticateToken, async (req, res, next) => {
  try {
    const { stationId, cdmc, channel } = req.query;
    const stationUrl = resolveOnlineStationUrl(stationId);
    await proxyGet(stationUrl, '/telemetry', { cdmc, channel }, res, next);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

router.get('/stats', authenticateToken, async (req, res, next) => {
  try {
    const { stationId, cdmc, channel } = req.query;
    const stationUrl = resolveOnlineStationUrl(stationId);
    await proxyGet(stationUrl, '/stats', { cdmc, channel }, res, next);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

router.get('/templates', authenticateToken, async (req, res, next) => {
  try {
    const stationUrl = resolveOnlineStationUrl(req.query.stationId);
    await proxyGet(stationUrl, '/templates', {}, res, next);
  } catch (err) {
    res.status(err.status || 500).json({ error: err.message });
  }
});

router.post('/report', authenticateToken, async (req, res, next) => {
  try {
    const {
      stationId,
      batchId,
      batch_id,
      cdmc,
      channel,
      templateName,
      template_name,
    } = req.body || {};

    const stationUrl = resolveOnlineStationUrl(stationId);
    const payload = {
      batch_id: batch_id || batchId,
      cdmc,
      channel,
      template_name: template_name || templateName,
    };

    const response = await axios.post(`${stationUrl}/report`, payload, {
      timeout: 60000,
      responseType: 'arraybuffer',
    });

    if (response.headers['content-type']) {
      res.setHeader('Content-Type', response.headers['content-type']);
    }
    if (response.headers['content-disposition']) {
      res.setHeader('Content-Disposition', response.headers['content-disposition']);
    }

    res.send(Buffer.from(response.data));
  } catch (err) {
    if (err.response) {
      return res.status(err.response.status).json(err.response.data);
    }
    return next(err);
  }
});

module.exports = router;
