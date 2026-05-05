const express = require('express');
const router = express.Router();
const axios = require('axios');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');
const { v4: uuidv4 } = require('uuid');
const { authenticateToken } = require('../middleware/auth');
const logger = require('../utils/logger');
const { upsertStation, getStations, resolveUrl } = require('../utils/stationRegistry');
const { getDb } = require('../models/database');

const PYTHON_BASE = process.env.BATTERY_SERVICE_URL || 'http://127.0.0.1:8765';

/** Helper: resolve Python service URL by stationId, fallback to PYTHON_BASE */
function resolveStationUrl(stationId) {
  if (stationId) {
    const url = resolveUrl(stationId);
    if (url) return url;
  }
  return PYTHON_BASE;
}

const TEMPLATES_DIR = path.join(__dirname, '../../templates');
if (!fs.existsSync(TEMPLATES_DIR)) {
  fs.mkdirSync(TEMPLATES_DIR, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, TEMPLATES_DIR),
  filename: (_req, _file, cb) => cb(null, 'battery_template.xlsx'),
});

const upload = multer({
  storage,
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (ext === '.xlsx') {
      cb(null, true);
    } else {
      cb(new Error('Only .xlsx files are accepted'));
    }
  },
});

const archiveStorage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, TEMPLATES_DIR),
  filename: (_req, _file, cb) => cb(null, 'battery_archive.xlsx'),
});

const uploadArchive = multer({
  storage: archiveStorage,
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (ext === '.xlsx') {
      cb(null, true);
    } else {
      cb(new Error('Only .xlsx files are accepted'));
    }
  },
});

// ---------------------------------------------------------------------------
// Station self-registration (NO auth — called by station machines on LAN)
// ---------------------------------------------------------------------------

// POST /api/battery/register — station machine registers / sends heartbeat
router.post('/register', (req, res) => {
  const { name, url } = req.body || {};
  if (!name || !url) {
    return res.status(400).json({ error: 'name and url are required' });
  }
  const id = upsertStation(name, url, 'battery');
  res.json({ ok: true, id });
});

// GET /api/battery/stations — return list of registered battery stations with online flag
router.get('/stations', authenticateToken, (req, res) => {
  res.json({ stations: getStations('battery') });
});

// All remaining battery routes require authentication
router.use(authenticateToken);

// GET /api/battery/ports — list COM ports on the selected station
router.get('/ports', async (req, res) => {
  const base = resolveStationUrl(req.query.stationId);
  try {
    const result = await axios.get(`${base}/ports`);
    res.json(result.data);
  } catch (e) {
    logger.error('Battery /ports proxy error', { error: e.message });
    res.status(503).json({ error: 'Battery service unavailable', detail: e.message });
  }
});

// GET /api/battery/status — session status
router.get('/status', async (req, res) => {
  const base = resolveStationUrl(req.query.stationId);
  try {
    const result = await axios.get(`${base}/status`);
    res.json(result.data);
  } catch (e) {
    res.status(503).json({ error: 'Battery service unavailable', detail: e.message });
  }
});

// GET /api/battery/report/download — stream Excel file to client
router.get('/report/download', async (req, res) => {
  const base = resolveStationUrl(req.query.stationId);
  try {
    const result = await axios.get(`${base}/report/download`, {
      responseType: 'stream',
    });
    const contentDisposition = result.headers['content-disposition'] || 'attachment; filename="report.xlsx"';
    const contentType = result.headers['content-type'] || 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    res.setHeader('Content-Disposition', contentDisposition);
    res.setHeader('Content-Type', contentType);
    result.data.pipe(res);
  } catch (e) {
    if (e.response?.status === 404) {
      return res.status(404).json({ error: 'No report available for current session' });
    }
    logger.error('Battery report download proxy error', { error: e.message });
    res.status(503).json({ error: 'Battery service unavailable', detail: e.message });
  }
});

// GET /api/battery/health — check if a station's Python service is reachable
router.get('/health', async (req, res) => {
  const base = resolveStationUrl(req.query.stationId);
  try {
    await axios.get(`${base}/ports`, { timeout: 3000 });
    res.json({ ok: true, service: 'battery', url: base });
  } catch (e) {
    res.status(503).json({ ok: false, service: 'battery', error: e.message });
  }
});

// POST /api/battery/upload-template — save uploaded .xlsx template
router.post('/upload-template', (req, res) => {
  upload.single('template')(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message });
    }
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded or invalid file type' });
    }
    logger.info('Battery template uploaded', { filename: req.file.filename });
    res.json({ ok: true, message: 'Template saved' });
  });
});

// GET /api/battery/template-info — check if template file exists
router.get('/template-info', (req, res) => {
  const templatePath = path.join(TEMPLATES_DIR, 'battery_template.xlsx');
  const exists = fs.existsSync(templatePath);
  res.json({ exists, name: exists ? 'battery_template.xlsx' : null });
});

// POST /api/battery/upload-archive — save uploaded .xlsx archive
router.post('/upload-archive', (req, res) => {
  uploadArchive.single('archive')(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message });
    }
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded or invalid file type' });
    }
    logger.info('Battery archive uploaded', { filename: req.file.filename });
    res.json({ ok: true, message: 'Archive saved' });
  });
});

// GET /api/battery/archive-info — check if archive file exists
router.get('/archive-info', (req, res) => {
  const archivePath = path.join(TEMPLATES_DIR, 'battery_archive.xlsx');
  const exists = fs.existsSync(archivePath);
  res.json({ exists, name: exists ? 'battery_archive.xlsx' : null });
});

// POST /api/battery/download-report — inject test data into template and return xlsx
router.post('/download-report', async (req, res) => {
  const templatePath = path.join(TEMPLATES_DIR, 'battery_template.xlsx');

  if (!fs.existsSync(templatePath)) {
    return res.status(404).json({ error: 'Template not found. Please upload battery_template.xlsx first.' });
  }

  const { records } = req.body;
  if (!Array.isArray(records)) {
    return res.status(400).json({ error: 'records must be an array' });
  }

  try {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(templatePath);

    // Build lookup map by id
    const dataMap = {};
    for (const rec of records) {
      dataMap[rec.id] = rec;
    }

    const TAG_PATTERN = '(OCV|CCV|Time|Dia|Hei)_(\\d+)';
    const fullTagRegex = new RegExp(`^\\{\\{${TAG_PATTERN}\\}\\}$`, 'i');
    const inlineTagRegex = new RegExp(`\\{\\{${TAG_PATTERN}\\}\\}`, 'gi');

    const getTagValue = (field, rec) => {
      const f = field.toLowerCase();
      if (f === 'ocv') return parseFloat(rec.ocv);
      if (f === 'ccv') return parseFloat(rec.ccv);
      if (f === 'time') return String(rec.time);
      if (f === 'dia') return rec.dia != null ? parseFloat(rec.dia) : '';
      if (f === 'hei') return rec.hei != null ? parseFloat(rec.hei) : '';
      return '';
    };

    workbook.eachSheet((sheet) => {
      sheet.eachRow((row) => {
        row.eachCell({ includeEmpty: false }, (cell) => {
          const val = cell.value;
          if (typeof val !== 'string') return;

          const fullMatch = val.match(fullTagRegex);
          if (fullMatch) {
            const field = fullMatch[1];
            const id = parseInt(fullMatch[2], 10);
            const rec = dataMap[id];
            cell.value = rec ? getTagValue(field, rec) : '';
            return;
          }

          // Inline tags embedded in a string
          const replaced = val.replace(inlineTagRegex, (_match, field, idStr) => {
            const id = parseInt(idStr, 10);
            const rec = dataMap[id];
            if (!rec) return '';
            const v = getTagValue(field, rec);
            return v !== '' ? v : '';
          });

          if (replaced !== val) cell.value = replaced;
        });
      });
    });

    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="battery_report.xlsx"');
    res.send(buffer);
  } catch (e) {
    logger.error('Battery download-report error', { error: e.message });
    res.status(500).json({ error: 'Failed to generate report', detail: e.message });
  }
});

// POST /api/battery/download-archive-report — inject test data into archive and return xlsx
router.post('/download-archive-report', async (req, res) => {
  const archivePath = path.join(TEMPLATES_DIR, 'battery_archive.xlsx');

  if (!fs.existsSync(archivePath)) {
    return res.status(404).json({ error: 'Archive not found. Please upload battery_archive.xlsx first.' });
  }

  const { records } = req.body;
  if (!Array.isArray(records)) {
    return res.status(400).json({ error: 'records must be an array' });
  }

  try {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(archivePath);

    const dataMap = {};
    for (const rec of records) {
      dataMap[rec.id] = rec;
    }

    const TAG_PATTERN = '(OCV|CCV|Time|Dia|Hei)_(\\d+)';
    const fullTagRegex = new RegExp(`^\\{\\{${TAG_PATTERN}\\}\\}$`, 'i');
    const inlineTagRegex = new RegExp(`\\{\\{${TAG_PATTERN}\\}\\}`, 'gi');

    const getTagValue = (field, rec) => {
      const f = field.toLowerCase();
      if (f === 'ocv') return parseFloat(rec.ocv);
      if (f === 'ccv') return parseFloat(rec.ccv);
      if (f === 'time') return String(rec.time);
      if (f === 'dia') return rec.dia != null ? parseFloat(rec.dia) : '';
      if (f === 'hei') return rec.hei != null ? parseFloat(rec.hei) : '';
      return '';
    };

    workbook.eachSheet((sheet) => {
      sheet.eachRow((row) => {
        row.eachCell({ includeEmpty: false }, (cell) => {
          const val = cell.value;
          if (typeof val !== 'string') return;

          const fullMatch = val.match(fullTagRegex);
          if (fullMatch) {
            const field = fullMatch[1];
            const id = parseInt(fullMatch[2], 10);
            const rec = dataMap[id];
            cell.value = rec ? getTagValue(field, rec) : '';
            return;
          }

          const replaced = val.replace(inlineTagRegex, (_match, field, idStr) => {
            const id = parseInt(idStr, 10);
            const rec = dataMap[id];
            if (!rec) return '';
            const v = getTagValue(field, rec);
            return v !== '' ? v : '';
          });

          if (replaced !== val) cell.value = replaced;
        });
      });
    });

    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="battery_archive_report.xlsx"');
    res.send(buffer);
  } catch (e) {
    logger.error('Battery download-archive-report error', { error: e.message });
    res.status(500).json({ error: 'Failed to generate archive report', detail: e.message });
  }
});

// ---------------------------------------------------------------------------
// Battery Types (loai pin)
// ---------------------------------------------------------------------------

// GET /api/battery/types
router.get('/types', (req, res) => {
  try {
    const rows = getDb().prepare('SELECT id, name, created_at FROM battery_types ORDER BY created_at ASC').all();
    res.json({ types: rows });
  } catch (e) {
    logger.error('GET /battery/types error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// POST /api/battery/types
router.post('/types', (req, res) => {
  const { name } = req.body || {};
  if (!name || !String(name).trim()) {
    return res.status(400).json({ error: 'name is required' });
  }
  const trimmed = String(name).trim();
  try {
    const id = uuidv4();
    getDb().prepare('INSERT INTO battery_types (id, name, created_by) VALUES (?, ?, ?)').run(id, trimmed, req.user.id);
    res.json({ ok: true, id, name: trimmed });
  } catch (e) {
    if (e.message && e.message.includes('UNIQUE')) {
      return res.status(409).json({ error: 'Battery type already exists' });
    }
    logger.error('POST /battery/types error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/battery/types/:id
router.delete('/types/:id', (req, res) => {
  try {
    const info = getDb().prepare('DELETE FROM battery_types WHERE id = ?').run(req.params.id);
    if (info.changes === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (e) {
    logger.error('DELETE /battery/types error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// Battery Product Lines (dong san pham)
// ---------------------------------------------------------------------------

// GET /api/battery/product-lines
router.get('/product-lines', (req, res) => {
  try {
    const rows = getDb().prepare('SELECT id, name, created_at FROM battery_product_lines ORDER BY created_at ASC').all();
    res.json({ productLines: rows });
  } catch (e) {
    logger.error('GET /battery/product-lines error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// POST /api/battery/product-lines
router.post('/product-lines', (req, res) => {
  const { name } = req.body || {};
  if (!name || !String(name).trim()) {
    return res.status(400).json({ error: 'name is required' });
  }
  const trimmed = String(name).trim();
  try {
    const id = uuidv4();
    getDb().prepare('INSERT INTO battery_product_lines (id, name, created_by) VALUES (?, ?, ?)').run(id, trimmed, req.user.id);
    res.json({ ok: true, id, name: trimmed });
  } catch (e) {
    if (e.message && e.message.includes('UNIQUE')) {
      return res.status(409).json({ error: 'Product line already exists' });
    }
    logger.error('POST /battery/product-lines error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/battery/product-lines/:id
router.delete('/product-lines/:id', (req, res) => {
  try {
    const info = getDb().prepare('DELETE FROM battery_product_lines WHERE id = ?').run(req.params.id);
    if (info.changes === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (e) {
    logger.error('DELETE /battery/product-lines error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// Battery Presets (thong so)
// ---------------------------------------------------------------------------

// GET /api/battery/presets
router.get('/presets', (req, res) => {
  try {
    const rows = getDb().prepare('SELECT * FROM battery_presets ORDER BY battery_type, product_line').all();
    res.json({ presets: rows });
  } catch (e) {
    logger.error('GET /battery/presets error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/battery/presets — upsert a preset
router.put('/presets', (req, res) => {
  const { batteryType, productLine, resistance, ocvTime, loadTime, kCoeff, ocvMin, ocvMax, ccvMin, ccvMax, diaMin, diaMax, heiMin, heiMax } = req.body || {};
  if (!batteryType || !productLine) {
    return res.status(400).json({ error: 'batteryType and productLine are required' });
  }
  if (ocvMin == null || ocvMax == null || ccvMin == null || ccvMax == null) {
    return res.status(400).json({ error: 'ocvMin, ocvMax, ccvMin, ccvMax are required' });
  }
  try {
    const db = getDb();
    const existing = db.prepare('SELECT id FROM battery_presets WHERE battery_type = ? AND product_line = ?').get(batteryType, productLine);
    if (existing) {
      db.prepare(`
        UPDATE battery_presets SET resistance=?, ocv_time=?, load_time=?, k_coeff=?, ocv_min=?, ocv_max=?, ccv_min=?, ccv_max=?,
          dia_min=?, dia_max=?, hei_min=?, hei_max=?, updated_at=datetime('now')||'Z'
        WHERE battery_type=? AND product_line=?
      `).run(resistance, ocvTime, loadTime, kCoeff, ocvMin, ocvMax, ccvMin, ccvMax, diaMin ?? null, diaMax ?? null, heiMin ?? null, heiMax ?? null, batteryType, productLine);
      res.json({ ok: true, id: existing.id });
    } else {
      const id = uuidv4();
      db.prepare(`
        INSERT INTO battery_presets (id, battery_type, product_line, resistance, ocv_time, load_time, k_coeff, ocv_min, ocv_max, ccv_min, ccv_max, dia_min, dia_max, hei_min, hei_max, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(id, batteryType, productLine, resistance, ocvTime, loadTime, kCoeff, ocvMin, ocvMax, ccvMin, ccvMax, diaMin ?? null, diaMax ?? null, heiMin ?? null, heiMax ?? null, req.user.id);
      res.json({ ok: true, id });
    }
  } catch (e) {
    logger.error('PUT /battery/presets error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/battery/presets/:batteryType/:productLine
router.delete('/presets/:batteryType/:productLine', (req, res) => {
  try {
    const info = getDb().prepare('DELETE FROM battery_presets WHERE battery_type = ? AND product_line = ?').run(req.params.batteryType, req.params.productLine);
    if (info.changes === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (e) {
    logger.error('DELETE /battery/presets error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// Battery Order History (lịch sử đơn hàng)
// ---------------------------------------------------------------------------

// GET /api/battery/order-history — list all history (visible to all authenticated users)
router.get('/order-history', (req, res) => {
  try {
    const rows = getDb().prepare(
      'SELECT id, order_id, test_date, battery_type, product_line, records_json, chart_series_json, readings_json, saved_at, status FROM battery_order_history ORDER BY saved_at DESC LIMIT 200'
    ).all();
    const items = rows.map((row) => ({
      _snapshotId: row.id,
      _savedAt: row.saved_at,
      _status: row.status || 'new',
      orderId: row.order_id,
      testDate: row.test_date,
      batteryType: row.battery_type,
      productLine: row.product_line,
      records: JSON.parse(row.records_json || '[]'),
      chartSeriesByBattery: JSON.parse(row.chart_series_json || '{}'),
      readingsByBattery: JSON.parse(row.readings_json || '{}'),
    }));
    res.json({ items });
  } catch (e) {
    logger.error('GET /battery/order-history error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// POST /api/battery/order-history — upsert a snapshot (keyed by order_id per user)
router.post('/order-history', (req, res) => {
  const { _snapshotId, orderId, testDate, batteryType, productLine, records, chartSeriesByBattery, readingsByBattery } = req.body || {};
  if (!orderId || !String(orderId).trim()) {
    return res.status(400).json({ error: 'orderId is required' });
  }
  try {
    const db = getDb();
    const normalizedOrderId = String(orderId).trim().toLowerCase();
    // Check for existing snapshot: first by _snapshotId, then fall back to orderId to prevent duplicates
    let existing = _snapshotId
      ? db.prepare('SELECT id FROM battery_order_history WHERE id = ? AND created_by = ?').get(_snapshotId, req.user.id)
      : db.prepare('SELECT id FROM battery_order_history WHERE lower(order_id) = ? AND created_by = ?').get(normalizedOrderId, req.user.id);

    // If _snapshotId was provided but not found (e.g. stale/cross-user ID), fall back to orderId lookup
    // so we UPDATE instead of INSERT and avoid duplicate order_id entries.
    if (!existing && _snapshotId) {
      existing = db.prepare('SELECT id FROM battery_order_history WHERE lower(order_id) = ? AND created_by = ?').get(normalizedOrderId, req.user.id);
    }

    const savedAt = new Date().toISOString();
    if (existing) {
      db.prepare(
        "UPDATE battery_order_history SET order_id=?, test_date=?, battery_type=?, product_line=?, records_json=?, chart_series_json=?, readings_json=?, saved_at=?, status='updated' WHERE id=?"
      ).run(
        String(orderId).trim(),
        testDate || null,
        batteryType || null,
        productLine || null,
        JSON.stringify(records || []),
        JSON.stringify(chartSeriesByBattery || {}),
        JSON.stringify(readingsByBattery || {}),
        savedAt,
        existing.id,
      );
      res.json({ ok: true, id: existing.id, savedAt, updated: true });
    } else {
      const id = _snapshotId || uuidv4();
      db.prepare(
        "INSERT INTO battery_order_history (id, order_id, test_date, battery_type, product_line, records_json, chart_series_json, readings_json, saved_at, created_by, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')"
      ).run(
        id,
        String(orderId).trim(),
        testDate || null,
        batteryType || null,
        productLine || null,
        JSON.stringify(records || []),
        JSON.stringify(chartSeriesByBattery || {}),
        JSON.stringify(readingsByBattery || {}),
        savedAt,
        req.user.id,
      );
      res.json({ ok: true, id, savedAt, updated: false });
    }
  } catch (e) {
    logger.error('POST /battery/order-history error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/battery/order-history — clear all history for current user
router.delete('/order-history', (req, res) => {
  try {
    getDb().prepare('DELETE FROM battery_order_history WHERE created_by = ?').run(req.user.id);
    res.json({ ok: true });
  } catch (e) {
    logger.error('DELETE /battery/order-history error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/battery/order-history/:id — delete one snapshot
router.delete('/order-history/:id', (req, res) => {
  try {
    const info = getDb().prepare('DELETE FROM battery_order_history WHERE id = ? AND created_by = ?').run(req.params.id, req.user.id);
    if (info.changes === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (e) {
    logger.error('DELETE /battery/order-history/:id error', { error: e.message });
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
