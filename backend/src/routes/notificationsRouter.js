const express = require('express');
const router = express.Router();
const { authenticateToken } = require('../middleware/auth');
const { getNotifications, markNotificationRead, markAllNotificationsRead } = require('../controllers/subscriptionController');

// Notifications
router.get('/', authenticateToken, getNotifications);
router.put('/read-all', authenticateToken, markAllNotificationsRead);
router.put('/:id/read', authenticateToken, markNotificationRead);

module.exports = router;
