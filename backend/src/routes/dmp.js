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
    const data = r.data;
    // Merge user-provided overrides (serialno, remarks) stored in SQLite.
    // SQLite override always takes precedence when non-null/non-empty so that
    // web edits are immediately reflected.
    if (data && Array.isArray(data.batches) && data.batches.length > 0 && req.query.stationId) {
      try {
        const { getDb } = require('../models/database');
        const db = getDb();
        const stationId = req.query.stationId;
        const overrides = db
          .prepare('SELECT batch_id, serialno, remarks FROM dmp_batch_overrides WHERE station_id = ?')
          .all(stationId);
        if (overrides.length > 0) {
          const overrideMap = {};
          for (const ov of overrides) {
            overrideMap[ov.batch_id] = ov;
          }
          data.batches = data.batches.map((batch) => {
            const ov = overrideMap[String(batch.id)];
            if (!ov) return batch;
            return {
              ...batch,
              serialno: (ov.serialno != null && ov.serialno !== '') ? ov.serialno : (batch.serialno ?? null),
              remarks: (ov.remarks != null && ov.remarks !== '') ? ov.remarks : (batch.remarks ?? null),
            };
          });
        }
      } catch (overrideErr) {
        // Non-fatal: if SQLite read fails, return unmerged data
        void overrideErr;
      }
    }
    res.json(data);
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

// ─── DM Historic Database Proxy Routes (shared DM2000 + DM3000) ──────────
// Both modules expose an identical FastAPI surface (same Access schema,
// only differing in the unit on the discharge-condition field — Ohm for
// DM2000, mA for DM3000).  This factory mounts the same set of routes
// twice, once under /dm2000 and once under /dm3000, so the proxy stays
// in lockstep with the FastAPI service without copy-pasting bodies.
function mountDmHistoricRoutes(targetRouter, prefix, sqlite) {
  const { optionsTable, overridesTable, allowedOptionFields } = sqlite;

  // GET /api/dmp/<prefix>/archives — list archives, merged with SQLite
  // overrides for serialno/remarks.
  targetRouter.get(`/${prefix}/archives`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives`, {
        params: {
          date_from: req.query.date_from,
          date_to: req.query.date_to,
          type_filter: req.query.type_filter,
          name_filter: req.query.name_filter,
          mfr_filter: req.query.mfr_filter,
          serial_filter: req.query.serial_filter,
          dis_condition_filter: req.query.dis_condition_filter,
          keyword: req.query.keyword,
          limit: req.query.limit,
        },
        timeout: 90000,
      });
      const data = r.data;
      // Merge user-provided overrides (serialno, remarks) stored in SQLite.
      // SQLite override always takes precedence when non-null/non-empty so that
      // web edits are immediately reflected.  The Access DB value is only used
      // as a fallback when the user has not yet set a web override.
      if (data && Array.isArray(data.archives) && data.archives.length > 0 && req.query.stationId) {
        try {
          const { getDb } = require('../models/database');
          const db = getDb();
          const stationId = req.query.stationId;
          const overrides = db
            .prepare(`SELECT archname, serialno, remarks FROM ${overridesTable} WHERE station_id = ?`)
            .all(stationId);
          if (overrides.length > 0) {
            const overrideMap = {};
            for (const ov of overrides) {
              overrideMap[ov.archname] = ov;
            }
            data.archives = data.archives.map((archive) => {
              const ov = overrideMap[archive.archname];
              if (!ov) return archive;
              return {
                ...archive,
                // SQLite override wins when non-null/non-empty; Access DB value is
                // used as a fallback when the user has not set a web override.
                serialno: (ov.serialno != null && ov.serialno !== '') ? ov.serialno : (archive.serialno ?? null),
                remarks: (ov.remarks != null && ov.remarks !== '') ? ov.remarks : (archive.remarks ?? null),
                _has_override: true,
              };
            });
          }
        } catch (overrideErr) {
          // Non-fatal: if SQLite read fails, return unmerged data
          void overrideErr;
        }
      }
      res.json(data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/dis-condition-options`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/dis-condition-options`, { timeout: 30000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/archives/:archname/batteries`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/batteries`, { timeout: 15000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/archives/:archname/curve`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/curve`, {
        params: { baty: req.query.baty },
        timeout: 120000,
      });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/archives/:archname/average-curve`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/average-curve`, { timeout: 180000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/archives/:archname/stats`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/stats`, {
        params: { baty: req.query.baty },
        timeout: 120000,
      });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/archives/:archname/daily-voltage`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/daily-voltage`, {
        params: { baty: req.query.baty },
        timeout: 60000,
      });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/archives/:archname/time-at-voltage`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/time-at-voltage`, {
        params: { baty: req.query.baty },
        timeout: 15000,
      });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/config`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/config`, { timeout: 10000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.get(`/${prefix}/templates`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/templates`, { timeout: 10000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  // GET /api/dmp/<prefix>/archives/:archname/schema?stationId= — diagnostic: return raw column names from ls_jb_cs
  targetRouter.get(`/${prefix}/archives/:archname/schema`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/archives/${encodeURIComponent(req.params.archname)}/schema`, { timeout: 15000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  // POST /api/dmp/<prefix>/refresh-archives — force-refresh archives cache after manual Access edits
  targetRouter.post(`/${prefix}/refresh-archives`, authenticateToken, async (req, res, next) => {
    const { stationId, ...body } = req.body || {};
    const stationUrl = getStationUrl(stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.post(`${stationUrl}/${prefix}/refresh-archives`, body, { timeout: 30000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  targetRouter.post(`/${prefix}/report`, authenticateToken, async (req, res, next) => {
    const { stationId, ...reportBody } = req.body || {};
    const stationUrl = getStationUrl(stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.post(`${stationUrl}/${prefix}/report`, reportBody, {
        responseType: 'arraybuffer',
        timeout: 60000,
      });
      const disposition = r.headers['content-disposition'] || `attachment; filename="${prefix}_report.xlsx"`;
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

  targetRouter.post(`/${prefix}/report-simple`, authenticateToken, async (req, res, next) => {
    const { stationId, ...reportBody } = req.body || {};
    const stationUrl = getStationUrl(stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.post(`${stationUrl}/${prefix}/report-simple`, reportBody, {
        responseType: 'arraybuffer',
        timeout: 120000,
      });
      const disposition = r.headers['content-disposition'] || `attachment; filename="${prefix}_preview.xlsx"`;
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

  targetRouter.post(`/${prefix}/perf-report`, authenticateToken, async (req, res, next) => {
    const { stationId, ...reportBody } = req.body || {};
    const stationUrl = getStationUrl(stationId, res);
    if (!stationUrl) return;
    try {
      // Inject SQLite overrides (serialno, remarks) into each entry so the
      // perf-report generator uses web-edited values for sheet-name derivation.
      if (stationId && Array.isArray(reportBody.entries) && reportBody.entries.length > 0) {
        try {
          const { getDb } = require('../models/database');
          const db = getDb();
          const overrides = db
            .prepare(`SELECT archname, serialno, remarks FROM ${overridesTable} WHERE station_id = ?`)
            .all(stationId);
          if (overrides.length > 0) {
            const overrideMap = {};
            for (const ov of overrides) { overrideMap[ov.archname] = ov; }
            reportBody.entries = reportBody.entries.map((entry) => {
              const ov = overrideMap[entry.archname];
              if (!ov) return entry;
              return {
                ...entry,
                override_serial_no: (ov.serialno != null && ov.serialno !== '') ? ov.serialno : (entry.override_serial_no ?? null),
                override_remarks: (ov.remarks != null && ov.remarks !== '') ? ov.remarks : (entry.override_remarks ?? null),
              };
            });
          }
        } catch (overrideErr) {
          // Non-fatal: proceed without overrides
          void overrideErr;
        }
      }
      const r = await axios.post(`${stationUrl}/${prefix}/perf-report`, reportBody, {
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

  // GET /api/dmp/<prefix>/perf-templates?stationId=
  targetRouter.get(`/${prefix}/perf-templates`, authenticateToken, async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.get(`${stationUrl}/${prefix}/perf-templates`, { timeout: 10000 });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  // POST /api/dmp/<prefix>/perf-template/upload?stationId=
  targetRouter.post(`/${prefix}/perf-template/upload`, authenticateToken, upload.single('file'), async (req, res, next) => {
    const stationUrl = getStationUrl(req.query.stationId, res);
    if (!stationUrl) return;
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    try {
      const form = new FormData();
      form.append('file', req.file.buffer, {
        filename: req.file.originalname,
        contentType: req.file.mimetype || 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      const r = await axios.post(`${stationUrl}/${prefix}/perf-template/upload`, form, {
        headers: form.getHeaders(),
        timeout: 30000,
      });
      res.json(r.data);
    } catch (err) { handleProxyError(err, res, next); }
  });

  // ─── Dropdown Options (Type / Manufacturer) — SQLite-backed, per module ──

  // GET /api/dmp/<prefix>/options?field=type|manufacturer
  targetRouter.get(`/${prefix}/options`, authenticateToken, (req, res) => {
    const { field } = req.query;
    const { getDb } = require('../models/database');
    const db = getDb();
    try {
      const rows = field
        ? db.prepare(`SELECT id, field, value FROM ${optionsTable} WHERE field = ? ORDER BY value ASC`).all(field)
        : db.prepare(`SELECT id, field, value FROM ${optionsTable} ORDER BY field, value ASC`).all();
      res.json({ options: rows });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/dmp/<prefix>/options — admin only
  targetRouter.post(`/${prefix}/options`, authenticateToken, requireAdmin, (req, res) => {
    const { field, value } = req.body || {};
    if (!field || !allowedOptionFields.includes(field)) {
      return res.status(400).json({ error: `field must be one of: ${allowedOptionFields.join(', ')}` });
    }
    if (!value || typeof value !== 'string' || !value.trim()) {
      return res.status(400).json({ error: 'value is required' });
    }
    const { getDb } = require('../models/database');
    const { v4: uuidv4 } = require('uuid');
    const db = getDb();
    try {
      const id = uuidv4();
      db.prepare(`INSERT OR IGNORE INTO ${optionsTable} (id, field, value, created_by) VALUES (?, ?, ?, ?)`)
        .run(id, field, value.trim(), req.user.id);
      const row = db.prepare(`SELECT id, field, value FROM ${optionsTable} WHERE field = ? AND value = ?`).get(field, value.trim());
      res.json({ option: row });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // DELETE /api/dmp/<prefix>/options/:id — admin only
  targetRouter.delete(`/${prefix}/options/:id`, authenticateToken, requireAdmin, (req, res) => {
    const { getDb } = require('../models/database');
    const db = getDb();
    try {
      const result = db.prepare(`DELETE FROM ${optionsTable} WHERE id = ?`).run(req.params.id);
      if (result.changes === 0) return res.status(404).json({ error: 'Option not found' });
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ─── Archive Overrides (serialno + remarks stored in local SQLite) ────

  // GET /api/dmp/<prefix>/archive-overrides?stationId=&archname=
  targetRouter.get(`/${prefix}/archive-overrides`, authenticateToken, (req, res) => {
    const { stationId, archname } = req.query;
    if (!stationId) return res.status(400).json({ error: 'stationId is required' });
    const { getDb } = require('../models/database');
    const db = getDb();
    try {
      if (archname) {
        const row = db.prepare(
          `SELECT archname, serialno, remarks, updated_at FROM ${overridesTable} WHERE station_id = ? AND archname = ?`
        ).get(stationId, archname);
        return res.json({ override: row || null });
      }
      const rows = db.prepare(
        `SELECT archname, serialno, remarks, updated_at FROM ${overridesTable} WHERE station_id = ?`
      ).all(stationId);
      res.json({ overrides: rows });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // PUT /api/dmp/<prefix>/archive-overrides — upsert serialno/remarks for one archive
  targetRouter.put(`/${prefix}/archive-overrides`, authenticateToken, (req, res) => {
    const { stationId, archname, serialno, remarks } = req.body || {};
    if (!stationId || !archname) return res.status(400).json({ error: 'stationId and archname are required' });
    const { getDb } = require('../models/database');
    const db = getDb();
    try {
      db.prepare(`
        INSERT INTO ${overridesTable} (station_id, archname, serialno, remarks, updated_by, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now') || 'Z')
        ON CONFLICT(station_id, archname) DO UPDATE SET
          serialno = excluded.serialno,
          remarks = excluded.remarks,
          updated_by = excluded.updated_by,
          updated_at = datetime('now', 'utc') || 'Z'
      `).run(stationId, archname, serialno ?? null, remarks ?? null, req.user.id);
      const row = db.prepare(
        `SELECT archname, serialno, remarks, updated_at FROM ${overridesTable} WHERE station_id = ? AND archname = ?`
      ).get(stationId, archname);

      // Fire-and-forget: write remark/serialno back to the live Access database so
      // that performance-report queries (which read directly from Access) see the
      // updated values.  Failure is non-fatal — the SQLite override above still
      // serves as a display-layer fallback.
      const stationUrl = resolveUrl(stationId);
      if (stationUrl) {
        axios.post(`${stationUrl}/${prefix}/update-archive-meta`, {
          archname,
          remarks: remarks ?? null,
          serialno: serialno ?? null,
        }, { timeout: 10000 }).catch((err) => {
          logger.warn(`${prefix.toUpperCase()} archive-meta write-back to Access failed`, {
            archname,
            stationId,
            error: err.message,
          });
        });
      }

      res.json({ override: row });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/dmp/<prefix>/update-archive-meta — write bz/dcph directly to live Access DB
  targetRouter.post(`/${prefix}/update-archive-meta`, authenticateToken, async (req, res, next) => {
    const { stationId, ...body } = req.body || {};
    const stationUrl = getStationUrl(stationId, res);
    if (!stationUrl) return;
    try {
      const r = await axios.post(`${stationUrl}/${prefix}/update-archive-meta`, body, { timeout: 15000 });
      res.json(r.data);
    } catch (err) {
      next(err);
    }
  });
}

mountDmHistoricRoutes(router, 'dm2000', {
  optionsTable: 'dm2000_options',
  overridesTable: 'dm2000_archive_overrides',
  allowedOptionFields: ['type', 'manufacturer'],
});
mountDmHistoricRoutes(router, 'dm3000', {
  optionsTable: 'dm3000_options',
  overridesTable: 'dm3000_archive_overrides',
  allowedOptionFields: ['type', 'manufacturer'],
});

// ─── DMP Batch Overrides (serialno + remarks stored in local SQLite) ──────────

// GET /api/dmp/batch-overrides?stationId=&batchId=
router.get('/batch-overrides', authenticateToken, (req, res) => {
  const { stationId, batchId } = req.query;
  if (!stationId) return res.status(400).json({ error: 'stationId is required' });
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    if (batchId) {
      const row = db.prepare(
        'SELECT batch_id, serialno, remarks, updated_at FROM dmp_batch_overrides WHERE station_id = ? AND batch_id = ?'
      ).get(stationId, batchId);
      return res.json({ override: row || null });
    }
    const rows = db.prepare(
      'SELECT batch_id, serialno, remarks, updated_at FROM dmp_batch_overrides WHERE station_id = ?'
    ).all(stationId);
    res.json({ overrides: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/dmp/batch-overrides — upsert serialno/remarks for one DMP batch
router.put('/batch-overrides', authenticateToken, (req, res) => {
  const { stationId, batchId, serialno, remarks } = req.body || {};
  if (!stationId || !batchId) return res.status(400).json({ error: 'stationId and batchId are required' });
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    db.prepare(`
      INSERT INTO dmp_batch_overrides (station_id, batch_id, serialno, remarks, updated_by, updated_at)
      VALUES (?, ?, ?, ?, ?, datetime('now', 'utc') || 'Z')
      ON CONFLICT(station_id, batch_id) DO UPDATE SET
        serialno = excluded.serialno,
        remarks = excluded.remarks,
        updated_by = excluded.updated_by,
        updated_at = datetime('now', 'utc') || 'Z'
    `).run(stationId, batchId, serialno ?? null, remarks ?? null, req.user.id);
    const row = db.prepare(
      'SELECT batch_id, serialno, remarks, updated_at FROM dmp_batch_overrides WHERE station_id = ? AND batch_id = ?'
    ).get(stationId, batchId);

    // Fire-and-forget: write remark/serialno back to the live Access database so
    // that performance-report queries (which read directly from Access) see the
    // updated values.  Failure is non-fatal — the SQLite override above still
    // serves as a display-layer fallback.
    const stationUrl = resolveUrl(stationId);
    if (stationUrl) {
      axios.post(`${stationUrl}/update-batch-meta`, {
        batch_id: batchId,
        remarks: remarks ?? null,
        serialno: serialno ?? null,
      }, { timeout: 10000 }).catch((err) => {
        logger.warn('DMP batch-meta write-back to Access failed', {
          batchId,
          stationId,
          error: err.message,
        });
      });
    }

    res.json({ override: row });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── DMP Performance Report Entries (stored in local SQLite) ─────────────────

// GET /api/dmp/perf-entries?stationId=&dateFrom=&dateTo=
router.get('/perf-entries', authenticateToken, (req, res) => {
  const { getDb } = require('../models/database');
  const db = getDb();
  const { stationId, dateFrom, dateTo } = req.query;
  try {
    let sql = 'SELECT * FROM dmp_perf_entries WHERE 1=1';
    const params = [];
    if (stationId) { sql += ' AND station_id = ?'; params.push(stationId); }
    if (dateFrom) { sql += ' AND report_date >= ?'; params.push(dateFrom); }
    if (dateTo) { sql += ' AND report_date <= ?'; params.push(dateTo); }
    sql += ' ORDER BY report_date DESC, created_at DESC';
    const rows = db.prepare(sql).all(...params);
    const entries = rows.map((r) => ({
      ...r,
      groups: JSON.parse(r.groups_json || '[]'),
    }));
    res.json({ entries });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/dmp/perf-entries — create a new entry
router.post('/perf-entries', authenticateToken, (req, res) => {
  const { getDb } = require('../models/database');
  const { v4: uuidv4 } = require('uuid');
  const db = getDb();
  const { station_id, batch_id, report_date, model, groups, special_type, raw_remark, notes, dm2000_archname } = req.body || {};
  if (!station_id || !model) {
    return res.status(400).json({ error: 'station_id and model are required' });
  }
  // batch_id and report_date are no longer derived from a DDMMYY remark prefix.
  // The Python perf-report service identifies the matching para_pub batch via
  // raw_remark (bz LIKE) and reads the made date directly from para_singl.scrq,
  // so these columns are just placeholders to satisfy the NOT NULL constraint.
  const effectiveBatchId = batch_id || '';
  const effectiveDate = report_date || new Date().toISOString().slice(0, 10);
  try {
    const id = uuidv4();
    db.prepare(`
      INSERT INTO dmp_perf_entries
        (id, station_id, batch_id, report_date, model, groups_json, special_type, raw_remark, notes, dm2000_archname, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      station_id,
      effectiveBatchId,
      effectiveDate,
      model,
      JSON.stringify(groups || []),
      special_type || 'normal',
      raw_remark || null,
      notes || null,
      dm2000_archname || null,
      req.user.id,
    );
    res.json({ ok: true, id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/dmp/perf-entries/export?stationId= — export all entries as Excel
router.get('/perf-entries/export', authenticateToken, async (req, res) => {
  const { getDb } = require('../models/database');
  const ExcelJS = require('exceljs');
  const db = getDb();
  const { stationId } = req.query;
  try {
    let sql = 'SELECT * FROM dmp_perf_entries WHERE 1=1';
    const params = [];
    if (stationId) { sql += ' AND station_id = ?'; params.push(stationId); }
    sql += ' ORDER BY report_date ASC, created_at ASC';
    const rows = db.prepare(sql).all(...params);

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Remark Data');
    sheet.columns = [
      { header: 'report_date', key: 'report_date', width: 14 },
      { header: 'model', key: 'model', width: 10 },
      { header: 'groups', key: 'groups', width: 30 },
      { header: 'special_type', key: 'special_type', width: 14 },
      { header: 'raw_remark', key: 'raw_remark', width: 30 },
      { header: 'notes', key: 'notes', width: 30 },
    ];
    for (const row of rows) {
      const groups = JSON.parse(row.groups_json || '[]');
      const groupsText = groups.map((g) => `${g.loai} ${g.chuyen}`.trim()).join('; ');
      sheet.addRow({
        report_date: row.report_date || '',
        model: row.model || '',
        groups: groupsText,
        special_type: row.special_type || 'normal',
        raw_remark: row.raw_remark || '',
        notes: row.notes || '',
      });
    }

    const buf = await workbook.xlsx.writeBuffer();
    const filename = `remark_data_${stationId || 'all'}_${new Date().toISOString().slice(0, 10)}.xlsx`;
    res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.set('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(buf);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/dmp/perf-entries/import?stationId= — import entries from Excel
router.post('/perf-entries/import', authenticateToken, upload.single('file'), async (req, res) => {
  const { getDb } = require('../models/database');
  const { v4: uuidv4 } = require('uuid');
  const ExcelJS = require('exceljs');
  const db = getDb();
  const { stationId } = req.query;
  if (!stationId) return res.status(400).json({ error: 'stationId is required' });
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  try {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(req.file.buffer);
    const sheet = workbook.worksheets[0];
    if (!sheet) return res.status(400).json({ error: 'No worksheet found' });

    // Read headers from the first row
    const headers = [];
    sheet.getRow(1).eachCell((cell) => { headers.push(String(cell.value || '').trim().toLowerCase()); });
    const col = (name) => headers.indexOf(name);

    const inserted = [];
    let skippedCount = 0;
    const errors = [];
    sheet.eachRow((row, rowNum) => {
      if (rowNum === 1) return;
      const getValue = (name) => {
        const idx = col(name);
        if (idx < 0) return '';
        const cell = row.getCell(idx + 1);
        if (cell.value == null) return '';
        if (cell.value instanceof Date) return cell.value.toISOString().slice(0, 10);
        return String(cell.value).trim();
      };

      const report_date = getValue('report_date');
      const model = getValue('model');
      const groupsText = getValue('groups');
      const special_type = getValue('special_type') || 'normal';
      const raw_remark = getValue('raw_remark');
      const notes = getValue('notes');

      if (!model) { errors.push({ row: rowNum, error: 'model is required' }); return; }

      // Parse groups: "UD 501; UD+ 502; HP 503"
      const groups = groupsText ? groupsText.split(';').map((s) => {
        const part = s.trim();
        if (!part) return null;
        let loai; let chuyen;
        if (part.startsWith('UD+ ')) { loai = 'UD+'; chuyen = part.slice(4).trim(); }
        else if (part.startsWith('UD ')) { loai = 'UD'; chuyen = part.slice(3).trim(); }
        else if (part.startsWith('HP ')) { loai = 'HP'; chuyen = part.slice(3).trim(); }
        else {
          const sp = part.indexOf(' ');
          loai = sp > 0 ? part.slice(0, sp) : part;
          chuyen = sp > 0 ? part.slice(sp + 1) : '';
        }
        return { loai, chuyen, trays: [] };
      }).filter(Boolean) : [];

      // batch_id and report_date are placeholders only; the perf-report
      // service identifies the para_pub batch via raw_remark (bz LIKE) and
      // reads the made date directly from para_singl.scrq.
      const effectiveBatchId = '';
      const effectiveDate = report_date || new Date().toISOString().slice(0, 10);

      // Duplicate check: skip if an identical entry already exists
      const groupsJson = JSON.stringify(groups);
      let existing = null;
      if (raw_remark) {
        existing = db.prepare(
          'SELECT id FROM dmp_perf_entries WHERE station_id = ? AND raw_remark = ? LIMIT 1'
        ).get(stationId, raw_remark);
      } else {
        existing = db.prepare(
          'SELECT id FROM dmp_perf_entries WHERE station_id = ? AND batch_id = ? AND model = ? AND groups_json = ? LIMIT 1'
        ).get(stationId, effectiveBatchId, model, groupsJson);
      }
      if (existing) { skippedCount += 1; return; }

      const id = uuidv4();
      db.prepare(`
        INSERT INTO dmp_perf_entries
          (id, station_id, batch_id, report_date, model, groups_json, special_type, raw_remark, notes, dm2000_archname, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(id, stationId, effectiveBatchId, effectiveDate, model, groupsJson,
        special_type, raw_remark || null, notes || null, null, req.user.id);
      inserted.push(id);
    });

    res.json({ ok: true, imported: inserted.length, skipped: skippedCount, errors });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/dmp/perf-entries/:id — update an entry
router.put('/perf-entries/:id', authenticateToken, (req, res) => {
  const { getDb } = require('../models/database');
  const db = getDb();
  const { batch_id, report_date, model, groups, special_type, raw_remark, notes, dm2000_archname } = req.body || {};
  try {
    const result = db.prepare(`
      UPDATE dmp_perf_entries SET
        batch_id = COALESCE(?, batch_id),
        report_date = COALESCE(?, report_date),
        model = COALESCE(?, model),
        groups_json = COALESCE(?, groups_json),
        special_type = COALESCE(?, special_type),
        raw_remark = ?,
        notes = ?,
        dm2000_archname = ?,
        updated_at = datetime('now') || 'Z'
      WHERE id = ?
    `).run(
      batch_id || null,
      report_date || null,
      model || null,
      groups !== undefined ? JSON.stringify(groups) : null,
      special_type || null,
      raw_remark || null,
      notes || null,
      dm2000_archname || null,
      req.params.id,
    );
    if (result.changes === 0) return res.status(404).json({ error: 'Entry not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/dmp/perf-entries — delete ALL entries for a station (admin only, requires password)
router.delete('/perf-entries', authenticateToken, requireAdmin, async (req, res) => {
  const { getDb } = require('../models/database');
  const bcrypt = require('bcryptjs');
  const db = getDb();
  const { stationId, password } = req.body || {};
  if (!stationId) return res.status(400).json({ error: 'stationId is required' });
  if (!password) return res.status(400).json({ error: 'password is required' });
  try {
    const user = db.prepare('SELECT password_hash FROM users WHERE id = ?').get(req.user.id);
    if (!user) return res.status(401).json({ error: 'User not found' });
    const valid = await bcrypt.compare(password, user.password_hash);
    if (!valid) return res.status(403).json({ error: 'Incorrect password' });
    const result = db.prepare('DELETE FROM dmp_perf_entries WHERE station_id = ?').run(stationId);
    res.json({ ok: true, deleted: result.changes });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/dmp/perf-entries/:id — delete an entry
router.delete('/perf-entries/:id', authenticateToken, (req, res) => {
  const { getDb } = require('../models/database');
  const db = getDb();
  try {
    const result = db.prepare('DELETE FROM dmp_perf_entries WHERE id = ?').run(req.params.id);
    if (result.changes === 0) return res.status(404).json({ error: 'Entry not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/dmp/dmp-perf-templates?stationId=
router.get('/dmp-perf-templates', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.get(`${stationUrl}/dmp-perf-templates`, { timeout: 10000 });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// POST /api/dmp/dmp-perf-template/upload?stationId=
router.post('/dmp-perf-template/upload', authenticateToken, upload.single('file'), async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  try {
    const form = new FormData();
    form.append('file', req.file.buffer, {
      filename: req.file.originalname,
      contentType: req.file.mimetype,
    });
    const r = await axios.post(`${stationUrl}/dmp-perf-template/upload`, form, {
      headers: form.getHeaders(),
      timeout: 30000,
    });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

// POST /api/dmp/dmp-perf-report/generate?stationId= — generate DMP perf report
router.post('/dmp-perf-report/generate', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    const r = await axios.post(`${stationUrl}/dmp-perf-report/generate`, req.body, {
      responseType: 'arraybuffer',
      timeout: 120000,
    });
    const disposition = r.headers['content-disposition'] || 'attachment; filename="dmp_perf_report.xlsx"';
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

// POST /api/dmp/dmp-perf-data?stationId= — get DMP perf data as JSON for web preview
router.post('/dmp-perf-data', authenticateToken, async (req, res, next) => {
  const stationUrl = getStationUrl(req.query.stationId, res);
  if (!stationUrl) return;
  try {
    // Use a generous timeout: each batch (≤30 entries) requires multiple ODBC
    // queries to the DM2000/DMP Access database.  300 s gives comfortable headroom
    // for slow Access installations.  The frontend sends batches in parallel so
    // several requests may arrive concurrently; the backend semaphore serialises
    // up to 3 concurrent ODBC connections at a time.
    const r = await axios.post(`${stationUrl}/dmp-perf-data`, req.body, { timeout: 300000 });
    res.json(r.data);
  } catch (err) {
    handleProxyError(err, res, next);
  }
});

module.exports = router;
