const express = require('express');

const { authenticateToken } = require('../middleware/auth');
const {
  getBatches,
  getChannels,
  getTelemetry,
  getStats,
  getTemplates,
  generateReport,
} = require('../controllers/dmpController');

const router = express.Router();

router.use(authenticateToken);

router.get('/batches', getBatches);
router.get('/batches/:batchId/channels', getChannels);
router.get('/telemetry', getTelemetry);
router.get('/stats', getStats);
router.get('/templates', getTemplates);
router.post('/report', generateReport);

module.exports = router;
