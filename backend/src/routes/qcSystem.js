const express = require('express');
const { createProxyMiddleware, fixRequestBody } = require('http-proxy-middleware');

const router = express.Router();

const QC_SERVICE_URL = process.env.QC_SERVICE_URL || 'http://127.0.0.1:8002';

function isQcRole(role) {
  return role === 'qc';
}

function enforceQcPermissions(req, res, next) {
  const role = req.user?.role;
  if (!isQcRole(role)) {
    return next();
  }

  const method = req.method.toUpperCase();
  const path = req.path || '/';

  const isDictionaryPath = path.startsWith('/dictionaries/');
  const isDictionaryImport = method === 'POST' && /^\/dictionaries\/[^/]+\/import\/?$/.test(path);
  const isDictionaryExport = method === 'GET' && /^\/dictionaries\/[^/]+\/export\/?$/.test(path);
  const isDictionaryRead = method === 'GET' && isDictionaryPath && !isDictionaryExport;
  const isHistoryDelete = method === 'DELETE' && /^\/quality-records\/\d+\/?$/.test(path);
  const isProductionDelete = method === 'DELETE' && /^\/production-outputs\/\d+\/?$/.test(path);

  if (isHistoryDelete || isProductionDelete) {
    return res.status(403).json({ message: 'QC role cannot delete QC dashboard data' });
  }

  if (isDictionaryPath) {
    if (isDictionaryExport || isDictionaryRead) {
      return next();
    }
    if (isDictionaryImport) {
      return res.status(403).json({ message: 'QC role cannot import QC dictionaries' });
    }
    return res.status(403).json({ message: 'QC role has export-only access to QC dictionaries' });
  }

  return next();
}

function fixStructuredRequestBody(proxyReq, req, res) {
  const contentType = String(proxyReq.getHeader('Content-Type') || req.headers['content-type'] || '').toLowerCase();
  if (
    contentType.includes('application/json') ||
    contentType.includes('application/x-www-form-urlencoded')
  ) {
    fixRequestBody(proxyReq, req, res);
  }
}

router.use(
  '/',
  enforceQcPermissions,
  createProxyMiddleware({
    target: QC_SERVICE_URL,
    changeOrigin: true,
    pathRewrite: (path) => `/api/v1${path}`,
    proxyTimeout: 30000,
    timeout: 30000,
    on: {
      proxyReq: fixStructuredRequestBody,
      error: (err, req, res) => {
        console.error('[QC Proxy Error]', err.message);
        if (!res.headersSent) {
          res.status(503).json({ error: 'QC System service unavailable' });
        }
      },
    },
  }),
);

module.exports = router;
