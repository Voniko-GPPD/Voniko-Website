'use strict';

const express = require('express');
const router = express.Router();
const multer = require('multer');
const os = require('os');
const { authenticateToken } = require('../middleware/auth');
const { generateBarcode } = require('../controllers/barcodeController');

const upload = multer({
  dest: os.tmpdir(),
  limits: { fileSize: 50 * 1024 * 1024 }, // 50 MB
  fileFilter: (_req, file, cb) => {
    const allowed = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/octet-stream',
    ];
    const ext = (file.originalname || '').toLowerCase().split('.').pop();
    if (allowed.includes(file.mimetype) || ['csv', 'xls', 'xlsx'].includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Only CSV, XLS, XLSX accepted.'));
    }
  },
});

router.post('/generate', authenticateToken, upload.single('file'), generateBarcode);

module.exports = router;
