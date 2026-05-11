const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../models/database');
const logger = require('../utils/logger');

async function subscribeFile(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT id, name FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  const existing = db.prepare('SELECT id FROM file_subscriptions WHERE file_id = ? AND user_id = ?').get(file.id, req.user.id);
  if (existing) return res.status(409).json({ message: 'Already subscribed' });

  const id = uuidv4();
  db.prepare('INSERT INTO file_subscriptions (id, file_id, user_id) VALUES (?, ?, ?)').run(id, file.id, req.user.id);

  db.prepare(`INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name) VALUES (?, ?, 'subscribe_file', 'file', ?, ?)`).run(uuidv4(), req.user.id, file.id, file.name);

  logger.info('File subscribed', { fileId: file.id, userId: req.user.id });
  res.status(201).json({ message: 'Subscribed successfully', subscribed: true });
}

async function unsubscribeFile(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT id, name FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  db.prepare('DELETE FROM file_subscriptions WHERE file_id = ? AND user_id = ?').run(file.id, req.user.id);

  db.prepare(`INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name) VALUES (?, ?, 'unsubscribe_file', 'file', ?, ?)`).run(uuidv4(), req.user.id, file.id, file.name);

  logger.info('File unsubscribed', { fileId: file.id, userId: req.user.id });
  res.json({ message: 'Unsubscribed successfully', subscribed: false });
}

async function getSubscribeStatus(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT id FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  const sub = db.prepare('SELECT id FROM file_subscriptions WHERE file_id = ? AND user_id = ?').get(file.id, req.user.id);
  res.json({ subscribed: !!sub });
}

async function getNotifications(req, res) {
  const db = getDb();
  const { page = 1, limit = 20 } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);

  const total = db.prepare('SELECT COUNT(*) as cnt FROM notifications WHERE user_id = ?').get(req.user.id).cnt;
  const unreadCount = db.prepare('SELECT COUNT(*) as cnt FROM notifications WHERE user_id = ? AND is_read = 0').get(req.user.id).cnt;

  const notifications = db.prepare(`
    SELECT * FROM notifications WHERE user_id = ?
    ORDER BY created_at DESC LIMIT ? OFFSET ?
  `).all(req.user.id, parseInt(limit), offset);

  res.json({
    data: notifications.map(n => ({
      id: n.id,
      type: n.type,
      title: n.title,
      message: n.message,
      entityId: n.entity_id,
      entityType: n.entity_type,
      isRead: !!n.is_read,
      createdAt: n.created_at,
    })),
    total,
    unreadCount,
    page: parseInt(page),
    limit: parseInt(limit),
  });
}

async function markNotificationRead(req, res) {
  const db = getDb();
  const notif = db.prepare('SELECT id FROM notifications WHERE id = ? AND user_id = ?').get(req.params.id, req.user.id);
  if (!notif) return res.status(404).json({ message: 'Notification not found' });

  db.prepare('UPDATE notifications SET is_read = 1 WHERE id = ?').run(req.params.id);
  res.json({ message: 'Marked as read' });
}

async function markAllNotificationsRead(req, res) {
  const db = getDb();
  db.prepare('UPDATE notifications SET is_read = 1 WHERE user_id = ?').run(req.user.id);
  res.json({ message: 'All marked as read' });
}

/**
 * Notify all subscribers of a file about an update (excluding the uploader)
 */
function notifyFileSubscribers(fileId, fileName, uploaderId, title, message) {
  try {
    const db = getDb();
    const subscribers = db.prepare('SELECT user_id FROM file_subscriptions WHERE file_id = ? AND user_id != ?').all(fileId, uploaderId);
    const insert = db.prepare('INSERT INTO notifications (id, user_id, type, title, message, entity_id, entity_type) VALUES (?, ?, ?, ?, ?, ?, ?)');
    const insertMany = db.transaction((subs) => {
      for (const sub of subs) {
        insert.run(uuidv4(), sub.user_id, 'file_update', title, message, fileId, 'file');
      }
    });
    insertMany(subscribers);
  } catch (err) {
    logger.warn('Failed to send file notifications', { fileId, error: err.message });
  }
}

module.exports = { subscribeFile, unsubscribeFile, getSubscribeStatus, getNotifications, markNotificationRead, markAllNotificationsRead, notifyFileSubscribers };
