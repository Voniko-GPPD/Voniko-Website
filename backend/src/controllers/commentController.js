const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../models/database');
const logger = require('../utils/logger');

async function getVersionComments(req, res) {
  const db = getDb();
  const version = db.prepare('SELECT id FROM versions WHERE id = ?').get(req.params.id);
  if (!version) return res.status(404).json({ message: 'Version not found' });

  const comments = db.prepare(`
    SELECT vc.id, vc.version_id, vc.user_id, vc.content, vc.created_at, vc.updated_at,
           u.display_name as user_name, u.username, u.avatar_url
    FROM version_comments vc
    LEFT JOIN users u ON vc.user_id = u.id
    WHERE vc.version_id = ? AND vc.is_deleted = 0
    ORDER BY vc.created_at ASC
  `).all(req.params.id);

  res.json(comments.map(c => ({
    id: c.id,
    versionId: c.version_id,
    userId: c.user_id,
    userName: c.user_name,
    username: c.username,
    avatarUrl: c.avatar_url,
    content: c.content,
    createdAt: c.created_at,
    updatedAt: c.updated_at,
  })));
}

async function addVersionComment(req, res) {
  const db = getDb();
  const { content } = req.body;
  if (!content || !content.trim()) return res.status(400).json({ message: 'Content is required' });

  const version = db.prepare(`
    SELECT v.id, v.file_id, f.name as file_name
    FROM versions v
    LEFT JOIN files f ON v.file_id = f.id
    WHERE v.id = ?
  `).get(req.params.id);
  if (!version) return res.status(404).json({ message: 'Version not found' });

  const id = uuidv4();
  db.prepare('INSERT INTO version_comments (id, version_id, user_id, content) VALUES (?, ?, ?, ?)').run(id, version.id, req.user.id, content.trim());

  db.prepare(`INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details) VALUES (?, ?, 'add_comment', 'version', ?, ?, ?)`).run(uuidv4(), req.user.id, version.id, version.file_name, JSON.stringify({ commentId: id }));

  logger.info('Comment added', { commentId: id, versionId: version.id, userId: req.user.id });

  const user = db.prepare('SELECT display_name, username, avatar_url FROM users WHERE id = ?').get(req.user.id);
  res.status(201).json({
    id,
    versionId: version.id,
    userId: req.user.id,
    userName: user.display_name,
    username: user.username,
    avatarUrl: user.avatar_url,
    content: content.trim(),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });
}

async function updateComment(req, res) {
  const db = getDb();
  const comment = db.prepare('SELECT * FROM version_comments WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!comment) return res.status(404).json({ message: 'Comment not found' });

  // Only author or admin
  if (comment.user_id !== req.user.id && req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Not authorized to edit this comment' });
  }

  const { content } = req.body;
  if (!content || !content.trim()) return res.status(400).json({ message: 'Content is required' });

  db.prepare("UPDATE version_comments SET content = ?, updated_at = datetime('now') || 'Z' WHERE id = ?").run(content.trim(), comment.id);

  res.json({ message: 'Comment updated', content: content.trim() });
}

async function deleteComment(req, res) {
  const db = getDb();
  const comment = db.prepare('SELECT * FROM version_comments WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!comment) return res.status(404).json({ message: 'Comment not found' });

  // Only author or admin
  if (comment.user_id !== req.user.id && req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Not authorized to delete this comment' });
  }

  db.prepare("UPDATE version_comments SET is_deleted = 1, updated_at = datetime('now') || 'Z' WHERE id = ?").run(comment.id);

  res.json({ message: 'Comment deleted' });
}

module.exports = { getVersionComments, addVersionComment, updateComment, deleteComment };
