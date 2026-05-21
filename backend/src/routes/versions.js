const express = require('express');
const router = express.Router();
const { authenticateToken } = require('../middleware/auth');
const { getVersion, downloadVersion, diffVersions, restoreVersion, previewVersion } = require('../controllers/versionController');

router.get('/diff', authenticateToken, diffVersions);
router.get('/:id', authenticateToken, getVersion);
router.get('/:id/download', authenticateToken, downloadVersion);
router.get('/:id/preview', authenticateToken, previewVersion);
router.post('/:id/restore', authenticateToken, restoreVersion);

module.exports = router;
