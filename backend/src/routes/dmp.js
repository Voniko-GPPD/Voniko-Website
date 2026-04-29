const express = require('express');
const axios = require('axios');
const multer = require('multer');
const FormData = require('form-data');
const { authenticateToken, requireAdmin } = require('../middleware/auth');
const { upsertStation, getStations, resolveUrl } = require('../utils/stationRegistry');
const logger = require('../utils/logger');

const router = express.Router();

// In-memory multer for proxying uploaded files to the DMP station
const upload = multer({ storage: multer.memoryStorage() });

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
  const id = upsertStation(name, url, 'dmp');
  logger.info('DMP station registered', { id, name });
  res.json({ ok: true, id });
});

// GET /api/dmp/stations — list all registered DMP stations
router.get('/stations', authenticateToken, (req, res) => {
  res.json({ stations: getStations('dmp') });
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

// GET /api/dmp/batches/years?stationId=
router.get('/batches/years', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/batches/years`, { timeout: 30000 });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// GET /api/dmp/batches?stationId=
router.get('/batches', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/batches`, {
      params: { year: req.query.year },
      timeout: 30000,
    });
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

// POST /api/dmp/report-simple — proxy basic xlsx download (no template required)
router.post('/report-simple', authenticateToken, async (req, res, next) => {
  const { stationId, ...reportBody } = req.body || {};
  const stationUrl = getStationUrl(stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.post(`${stationUrl}/report-simple`, reportBody, {
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
        keyword: req.query.keyword,
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

router.get('/dm2000/config', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/config`, { timeout: 10000 });
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

// GET /api/dmp/dm2000/archives/:archname/schema?stationId= — diagnostic: return raw column names from ls_jb_cs
router.get('/dm2000/archives/:archname/schema', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/archives/${encodeURIComponent(req.params.archname)}/schema`, { timeout: 15000 });
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

router.post('/dm2000/report-simple', authenticateToken, async (req, res, next) => {
  const { stationId, ...reportBody } = req.body || {};
  const stationUrl = getStationUrl(stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.post(`${stationUrl}/dm2000/report-simple`, reportBody, {
      responseType: 'arraybuffer',
      timeout: 120000,
    });
    const disposition = r.headers['content-disposition'] || 'attachment; filename="dm2000_preview.xlsx"';
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

router.post('/dm2000/perf-report', authenticateToken, async (req, res, next) => {
  const { stationId, ...reportBody } = req.body || {};
  const stationUrl = getStationUrl(stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.post(`${stationUrl}/dm2000/perf-report`, reportBody, {
      responseType: 'arraybuffer',
      timeout: 120000,
    });
    const disposition = r.headers['content-disposition'] || 'attachment; filename="perf_report.xlsx"';
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

// GET /api/dmp/dm2000/perf-templates?stationId=
router.get('/dm2000/perf-templates', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dm2000/perf-templates`, { timeout: 10000 });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

// POST /api/dmp/dm2000/perf-template/upload?stationId=
router.post('/dm2000/perf-template/upload', authenticateToken, upload.single('file'), async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  try {
    const form = new FormData();
    form.append('file', req.file.buffer, {
      filename: req.file.originalname,
      contentType: req.file.mimetype || 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    });
    const r = await axios.post(`${stationUrl}/dm2000/perf-template/upload`, form, {
      headers: form.getHeaders(),
      timeout: 30000,
    });
    res.json(r.data);
  } catch (err) { handleProxyError(err, res, next); }
});

// ─── DM2000 Dropdown Options (Type / Manufacturer) ───────────────────────────

// GET /api/dmp/dm2000/options?field=type|manufacturer
router.get('/dm2000/options', authenticateToken, (req, res) => {
  const { field } = req.query;
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const rows = field
      ? db.prepare('SELECT id, field, value FROM dm2000_options WHERE field = ? ORDER BY value ASC').all(field)
      : db.prepare('SELECT id, field, value FROM dm2000_options ORDER BY field, value ASC').all();
    res.json({ options: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/dmp/dm2000/options — admin only
router.post('/dm2000/options', authenticateToken, requireAdmin, (req, res) => {
  const { field, value } = req.body || {};
  if (!field || !['type', 'manufacturer'].includes(field)) {
    return res.status(400).json({ error: 'field must be "type" or "manufacturer"' });
  }
  if (!value || typeof value !== 'string' || !value.trim()) {
    return res.status(400).json({ error: 'value is required' });
  }
  const { getDb } = require('../models/database');
  const { v4: uuidv4 } = require('uuid');
  const db = getDb();
  try {
    const id = uuidv4();
    db.prepare('INSERT OR IGNORE INTO dm2000_options (id, field, value, created_by) VALUES (?, ?, ?, ?)')
      .run(id, field, value.trim(), req.user.id);
    const row = db.prepare('SELECT id, field, value FROM dm2000_options WHERE field = ? AND value = ?').get(field, value.trim());
    res.json({ option: row });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/dmp/dm2000/options/:id — admin only
router.delete('/dm2000/options/:id', authenticateToken, requireAdmin, (req, res) => {
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const result = db.prepare('DELETE FROM dm2000_options WHERE id = ?').run(req.params.id);
    if (result.changes === 0) return res.status(404).json({ error: 'Option not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Discharge Condition Presets (admin-editable) ────────────────────────────

function _readDischargePresets(db) {
  return db.prepare(
    'SELECT id, family, condition_text, suffix, sort_order '
    + 'FROM discharge_condition_presets ORDER BY family ASC, sort_order ASC, condition_text ASC',
  ).all();
}

// GET /api/dmp/discharge-presets — list all (any authenticated user)
router.get('/discharge-presets', authenticateToken, (req, res) => {
  const { getDb } = require('../models/database');
  try {
    res.json({ presets: _readDischargePresets(getDb()) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/dmp/discharge-presets — admin only, create one preset
router.post('/discharge-presets', authenticateToken, requireAdmin, (req, res) => {
  const { family, condition_text: conditionText, suffix, sort_order: sortOrder } = req.body || {};
  if (!family || typeof family !== 'string' || !family.trim()) {
    return res.status(400).json({ error: 'family is required' });
  }
  if (!conditionText || typeof conditionText !== 'string' || !conditionText.trim()) {
    return res.status(400).json({ error: 'condition_text is required' });
  }
  const sfx = (suffix || '').toString().trim().toLowerCase();
  if (sfx && !['h', 'm', 't'].includes(sfx)) {
    return res.status(400).json({ error: 'suffix must be one of "", "h", "m", "t"' });
  }
  const { getDb } = require('../models/database');
  const { v4: uuidv4 } = require('uuid');
  const db = getDb();
  try {
    const id = uuidv4();
    // Default sort_order to "after the last entry of this family"
    const order = Number.isFinite(Number(sortOrder))
      ? Number(sortOrder)
      : (db.prepare('SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM discharge_condition_presets WHERE family = ?')
        .get(family.trim()).n);
    db.prepare(
      'INSERT INTO discharge_condition_presets '
      + '(id, family, condition_text, suffix, sort_order, created_by) '
      + 'VALUES (?, ?, ?, ?, ?, ?)',
    ).run(id, family.trim(), conditionText.trim(), sfx, order, req.user.id);
    const row = db.prepare(
      'SELECT id, family, condition_text, suffix, sort_order FROM discharge_condition_presets WHERE id = ?',
    ).get(id);
    res.json({ preset: row });
  } catch (err) {
    if (String(err.message).includes('UNIQUE')) {
      return res.status(409).json({ error: 'This condition already exists for the family' });
    }
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/dmp/discharge-presets/:id — admin only, update existing preset
router.put('/discharge-presets/:id', authenticateToken, requireAdmin, (req, res) => {
  const { family, condition_text: conditionText, suffix, sort_order: sortOrder } = req.body || {};
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const existing = db.prepare(
      'SELECT id, family, condition_text, suffix, sort_order FROM discharge_condition_presets WHERE id = ?',
    ).get(req.params.id);
    if (!existing) return res.status(404).json({ error: 'Preset not found' });
    const newFamily = family != null ? String(family).trim() : existing.family;
    const newText = conditionText != null ? String(conditionText).trim() : existing.condition_text;
    const newSuffix = suffix != null ? String(suffix).trim().toLowerCase() : existing.suffix;
    if (newSuffix && !['h', 'm', 't'].includes(newSuffix)) {
      return res.status(400).json({ error: 'suffix must be one of "", "h", "m", "t"' });
    }
    if (!newFamily || !newText) {
      return res.status(400).json({ error: 'family and condition_text must be non-empty' });
    }
    const newOrder = Number.isFinite(Number(sortOrder)) ? Number(sortOrder) : existing.sort_order;
    db.prepare(
      "UPDATE discharge_condition_presets "
      + "SET family = ?, condition_text = ?, suffix = ?, sort_order = ?, "
      + "updated_at = (datetime('now') || 'Z') WHERE id = ?",
    ).run(newFamily, newText, newSuffix, newOrder, req.params.id);
    const row = db.prepare(
      'SELECT id, family, condition_text, suffix, sort_order FROM discharge_condition_presets WHERE id = ?',
    ).get(req.params.id);
    res.json({ preset: row });
  } catch (err) {
    if (String(err.message).includes('UNIQUE')) {
      return res.status(409).json({ error: 'This condition already exists for the family' });
    }
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/dmp/discharge-presets/:id — admin only
router.delete('/discharge-presets/:id', authenticateToken, requireAdmin, (req, res) => {
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const result = db.prepare('DELETE FROM discharge_condition_presets WHERE id = ?').run(req.params.id);
    if (result.changes === 0) return res.status(404).json({ error: 'Preset not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Battery Family Detection Keywords (admin-editable) ──────────────────────

function _readFamilyKeywords(db) {
  return db.prepare(
    'SELECT id, keyword, family, sort_order FROM battery_family_keywords '
    + 'ORDER BY sort_order ASC, keyword ASC',
  ).all();
}

// GET /api/dmp/family-keywords
router.get('/family-keywords', authenticateToken, (req, res) => {
  const { getDb } = require('../models/database');
  try {
    res.json({ keywords: _readFamilyKeywords(getDb()) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/dmp/family-keywords — admin only
router.post('/family-keywords', authenticateToken, requireAdmin, (req, res) => {
  const { keyword, family, sort_order: sortOrder } = req.body || {};
  if (!keyword || typeof keyword !== 'string' || !keyword.trim()) {
    return res.status(400).json({ error: 'keyword is required' });
  }
  if (!family || typeof family !== 'string' || !family.trim()) {
    return res.status(400).json({ error: 'family is required' });
  }
  const { getDb } = require('../models/database');
  const { v4: uuidv4 } = require('uuid');
  const db = getDb();
  try {
    const id = uuidv4();
    const order = Number.isFinite(Number(sortOrder))
      ? Number(sortOrder)
      : (db.prepare('SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM battery_family_keywords').get().n);
    db.prepare(
      'INSERT INTO battery_family_keywords (id, keyword, family, sort_order, created_by) '
      + 'VALUES (?, ?, ?, ?, ?)',
    ).run(id, keyword.trim(), family.trim(), order, req.user.id);
    const row = db.prepare(
      'SELECT id, keyword, family, sort_order FROM battery_family_keywords WHERE id = ?',
    ).get(id);
    res.json({ keyword: row });
  } catch (err) {
    if (String(err.message).includes('UNIQUE')) {
      return res.status(409).json({ error: 'This keyword already exists' });
    }
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/dmp/family-keywords/:id — admin only
router.put('/family-keywords/:id', authenticateToken, requireAdmin, (req, res) => {
  const { keyword, family, sort_order: sortOrder } = req.body || {};
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const existing = db.prepare(
      'SELECT id, keyword, family, sort_order FROM battery_family_keywords WHERE id = ?',
    ).get(req.params.id);
    if (!existing) return res.status(404).json({ error: 'Keyword not found' });
    const newKw = keyword != null ? String(keyword).trim() : existing.keyword;
    const newFamily = family != null ? String(family).trim() : existing.family;
    const newOrder = Number.isFinite(Number(sortOrder)) ? Number(sortOrder) : existing.sort_order;
    if (!newKw || !newFamily) {
      return res.status(400).json({ error: 'keyword and family must be non-empty' });
    }
    db.prepare(
      'UPDATE battery_family_keywords SET keyword = ?, family = ?, sort_order = ? WHERE id = ?',
    ).run(newKw, newFamily, newOrder, req.params.id);
    const row = db.prepare(
      'SELECT id, keyword, family, sort_order FROM battery_family_keywords WHERE id = ?',
    ).get(req.params.id);
    res.json({ keyword: row });
  } catch (err) {
    if (String(err.message).includes('UNIQUE')) {
      return res.status(409).json({ error: 'This keyword already exists' });
    }
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/dmp/family-keywords/:id — admin only
router.delete('/family-keywords/:id', authenticateToken, requireAdmin, (req, res) => {
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const result = db.prepare('DELETE FROM battery_family_keywords WHERE id = ?').run(req.params.id);
    if (result.changes === 0) return res.status(404).json({ error: 'Keyword not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
