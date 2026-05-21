const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const os = require('os');
const { authenticateToken, requireEngineerOrAbove } = require('../middleware/auth');
const {
  listFiles, uploadFile, getFile, deleteFile,
  getActivityLog, getDashboardStats, lockFile, unlockFile, exportActivityLog, renameFile,
} = require('../controllers/fileController');
const { addFileTags, removeFileTag } = require('../controllers/tagController');
const { subscribeFile, unsubscribeFile, getSubscribeStatus } = require('../controllers/subscriptionController');

// Use OS temp dir for initial upload
const upload = multer({
  dest: os.tmpdir(),
  limits: { fileSize: 5 * 1024 * 1024 * 1024 }, // 5 GB max
});

router.get('/stats', authenticateToken, requireEngineerOrAbove, getDashboardStats);
router.get('/activity', authenticateToken, requireEngineerOrAbove, getActivityLog);
router.get('/activity/export', authenticateToken, requireEngineerOrAbove, exportActivityLog);
router.get('/', authenticateToken, requireEngineerOrAbove, listFiles);
router.post('/', authenticateToken, requireEngineerOrAbove, upload.single('file'), uploadFile);
router.get('/:id', authenticateToken, requireEngineerOrAbove, getFile);
router.patch('/:id', authenticateToken, requireEngineerOrAbove, renameFile);
router.delete('/:id', authenticateToken, requireEngineerOrAbove, deleteFile);
router.post('/:id/lock', authenticateToken, requireEngineerOrAbove, lockFile);
router.post('/:id/unlock', authenticateToken, requireEngineerOrAbove, unlockFile);

// Tags
router.post('/:id/tags', authenticateToken, requireEngineerOrAbove, addFileTags);
router.delete('/:id/tags/:tagId', authenticateToken, requireEngineerOrAbove, removeFileTag);

// Subscriptions
router.get('/:id/subscribe', authenticateToken, requireEngineerOrAbove, getSubscribeStatus);
router.post('/:id/subscribe', authenticateToken, requireEngineerOrAbove, subscribeFile);
router.delete('/:id/subscribe', authenticateToken, requireEngineerOrAbove, unsubscribeFile);

module.exports = router;
