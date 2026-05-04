const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const os = require('os');
const { authenticateToken } = require('../middleware/auth');
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

router.get('/stats', authenticateToken, getDashboardStats);
router.get('/activity', authenticateToken, getActivityLog);
router.get('/activity/export', authenticateToken, exportActivityLog);
router.get('/', authenticateToken, listFiles);
router.post('/', authenticateToken, upload.single('file'), uploadFile);
router.get('/:id', authenticateToken, getFile);
router.patch('/:id', authenticateToken, renameFile);
router.delete('/:id', authenticateToken, deleteFile);
router.post('/:id/lock', authenticateToken, lockFile);
router.post('/:id/unlock', authenticateToken, unlockFile);

// Tags
router.post('/:id/tags', authenticateToken, addFileTags);
router.delete('/:id/tags/:tagId', authenticateToken, removeFileTag);

// Subscriptions
router.get('/:id/subscribe', authenticateToken, getSubscribeStatus);
router.post('/:id/subscribe', authenticateToken, subscribeFile);
router.delete('/:id/subscribe', authenticateToken, unsubscribeFile);

module.exports = router;
