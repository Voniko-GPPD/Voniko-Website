/**
 * Count Batteries proxy route.
 *
 * All requests are authenticated via the existing JWT middleware.
 * User identity (id, username, role) is forwarded as custom headers to the
 * Python FastAPI service so it can apply its own role-based logic without
 * maintaining a separate user database.
 *
 * Python service URL: process.env.COUNT_BATTERIES_URL (default http://127.0.0.1:8001)
 */

const express = require('express');
const router = express.Router();
const axios = require('axios');
const multer = require('multer');
const FormData = require('form-data');
const logger = require('../utils/logger');
const { authenticateToken } = require('../middleware/auth');

const PYTHON_BASE = process.env.COUNT_BATTERIES_URL || 'http://127.0.0.1:8001';
const PREDICT_TIMEOUT_MS = parseInt(process.env.COUNT_BATTERIES_TIMEOUT_MS || '120000', 10);

/** Build proxy headers that carry user identity to the Python service */
function userHeaders(req) {
  return {
    'x-user-id': req.user.id,
    'x-username': req.user.username,
    'x-user-role': req.user.role,
    'x-display-name': req.user.display_name || req.user.username,
  };
}

// All routes require a valid JWT token
router.use(authenticateToken);

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------
router.get('/health', async (req, res) => {
  try {
    const result = await axios.get(`${PYTHON_BASE}/health`, { timeout: 3000 });
    res.json(result.data);
  } catch (e) {
    res.status(503).json({ ok: false, error: e.message });
  }
});

// ---------------------------------------------------------------------------
// POST /predict  – multipart image upload
// ---------------------------------------------------------------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 30 * 1024 * 1024 }, // 30 MB
});

router.post('/predict', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'Image file required' });
  }

  const form = new FormData();
  form.append('file', req.file.buffer, {
    filename: req.file.originalname || 'image.jpg',
    contentType: req.file.mimetype || 'image/jpeg',
  });
  form.append('confidence', req.body.confidence ?? '0.5');
  form.append('save_result', req.body.save_result !== 'false' ? 'true' : 'false');
  if (req.body.po_number) form.append('po_number', req.body.po_number);

  try {
    const result = await axios.post(`${PYTHON_BASE}/predict`, form, {
      headers: {
        ...form.getHeaders(),
        ...userHeaders(req),
      },
      timeout: PREDICT_TIMEOUT_MS, // AI inference can take a while
    });
    res.json(result.data);
  } catch (e) {
    logger.error('Count batteries /predict error', { error: e.message });
    if (e.response) return res.status(e.response.status).json(e.response.data);
    res.status(503).json({ error: 'Count batteries service unavailable', detail: e.message });
  }
});

// ---------------------------------------------------------------------------
// GET /history
// ---------------------------------------------------------------------------
router.get('/history', async (req, res) => {
  try {
    const result = await axios.get(`${PYTHON_BASE}/history`, {
      params: req.query,
      headers: userHeaders(req),
    });
    if (result.headers['x-total-count']) {
      res.setHeader('X-Total-Count', result.headers['x-total-count']);
      res.setHeader('Access-Control-Expose-Headers', 'X-Total-Count');
    }
    res.json(result.data);
  } catch (e) {
    logger.error('Count batteries /history error', { error: e.message });
    if (e.response) return res.status(e.response.status).json(e.response.data);
    res.status(503).json({ error: 'Count batteries service unavailable' });
  }
});

// ---------------------------------------------------------------------------
// GET /history/stats
// ---------------------------------------------------------------------------
router.get('/history/stats', async (req, res) => {
  try {
    const result = await axios.get(`${PYTHON_BASE}/history/stats`, {
      headers: userHeaders(req),
    });
    res.json(result.data);
  } catch (e) {
    logger.error('Count batteries /history/stats error', { error: e.message });
    if (e.response) return res.status(e.response.status).json(e.response.data);
    res.status(503).json({ error: 'Count batteries service unavailable' });
  }
});

// ---------------------------------------------------------------------------
// GET /history/export/excel  – stream binary response
// ---------------------------------------------------------------------------
router.get('/history/export/excel', async (req, res) => {
  try {
    const result = await axios.get(`${PYTHON_BASE}/history/export/excel`, {
      params: req.query,
      headers: userHeaders(req),
      responseType: 'stream',
    });
    const ct = result.headers['content-type'] || 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    const cd = result.headers['content-disposition'] || 'attachment; filename="battery_count_report.xlsx"';
    res.setHeader('Content-Type', ct);
    res.setHeader('Content-Disposition', cd);
    result.data.pipe(res);
  } catch (e) {
    logger.error('Count batteries /export error', { error: e.message });
    if (e.response) return res.status(e.response.status).json(e.response.data);
    res.status(503).json({ error: 'Count batteries service unavailable' });
  }
});

// ---------------------------------------------------------------------------
// DELETE /history/batch
// ---------------------------------------------------------------------------
router.delete('/history/batch', async (req, res) => {
  try {
    const result = await axios.delete(`${PYTHON_BASE}/history/batch`, {
      data: req.body,
      headers: { 'Content-Type': 'application/json', ...userHeaders(req) },
    });
    res.json(result.data);
  } catch (e) {
    logger.error('Count batteries /history/batch delete error', { error: e.message });
    if (e.response) return res.status(e.response.status).json(e.response.data);
    res.status(503).json({ error: 'Count batteries service unavailable' });
  }
});

// ---------------------------------------------------------------------------
// DELETE /history/:id
// ---------------------------------------------------------------------------
router.delete('/history/:id', async (req, res) => {
  // Validate id is a positive integer to prevent path traversal
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: 'Invalid record ID' });
  }
  try {
    const result = await axios.delete(`${PYTHON_BASE}/history/${id}`, {
      headers: userHeaders(req),
    });
    res.json(result.data);
  } catch (e) {
    logger.error('Count batteries /history/:id delete error', { error: e.message });
    if (e.response) return res.status(e.response.status).json(e.response.data);
    res.status(503).json({ error: 'Count batteries service unavailable' });
  }
});

module.exports = router;
