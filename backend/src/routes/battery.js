const express = require('express');
const router = express.Router();
const axios = require('axios');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const ExcelJS = require('exceljs');
const { authenticateToken } = require('../middleware/auth');
const logger = require('../utils/logger');
const { upsertStation, getStations, resolveUrl } = require('../utils/stationRegistry');

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

module.exports = router;
