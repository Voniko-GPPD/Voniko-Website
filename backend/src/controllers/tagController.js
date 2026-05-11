const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../models/database');
const logger = require('../utils/logger');

async function listTags(req, res) {
  const db = getDb();
  const tags = db.prepare('SELECT * FROM tags ORDER BY name ASC').all();
  res.json(tags.map(t => ({
    id: t.id,
    name: t.name,
    color: t.color,
    createdBy: t.created_by,
    createdAt: t.created_at,
  })));
}

async function createTag(req, res) {
  const db = getDb();
  const { name, color = '#1677ff' } = req.body;
  if (!name) return res.status(400).json({ message: 'Tag name is required' });

  const existing = db.prepare('SELECT id FROM tags WHERE name = ?').get(name);
  if (existing) return res.status(409).json({ message: 'Tag already exists' });

  const id = uuidv4();
  db.prepare('INSERT INTO tags (id, name, color, created_by) VALUES (?, ?, ?, ?)').run(id, name, color, req.user.id);

  db.prepare(`INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name) VALUES (?, ?, 'create_tag', 'tag', ?, ?)`).run(uuidv4(), req.user.id, id, name);

  logger.info('Tag created', { tagId: id, name, userId: req.user.id });
  res.status(201).json({ id, name, color });
}

async function deleteTag(req, res) {
  const db = getDb();
  const tag = db.prepare('SELECT * FROM tags WHERE id = ?').get(req.params.id);
  if (!tag) return res.status(404).json({ message: 'Tag not found' });

  db.prepare('DELETE FROM file_tags WHERE tag_id = ?').run(tag.id);
  db.prepare('DELETE FROM tags WHERE id = ?').run(tag.id);

  db.prepare(`INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name) VALUES (?, ?, 'delete_tag', 'tag', ?, ?)`).run(uuidv4(), req.user.id, tag.id, tag.name);

  logger.info('Tag deleted', { tagId: tag.id, name: tag.name, userId: req.user.id });
  res.json({ message: 'Tag deleted' });
}

async function addFileTags(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT id FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  const { tagIds } = req.body;
  if (!Array.isArray(tagIds)) return res.status(400).json({ message: 'tagIds must be an array' });

  const insertTag = db.prepare('INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)');
  const insertMany = db.transaction((ids) => {
    for (const tagId of ids) {
      const tag = db.prepare('SELECT id FROM tags WHERE id = ?').get(tagId);
      if (tag) insertTag.run(file.id, tagId);
    }
  });
  insertMany(tagIds);

  db.prepare(`INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name, details) VALUES (?, ?, 'add_file_tags', 'file', ?, ?, ?)`).run(uuidv4(), req.user.id, file.id, req.params.id, JSON.stringify({ tagIds }));

  res.json({ message: 'Tags added' });
}

async function removeFileTag(req, res) {
  const db = getDb();
  const file = db.prepare('SELECT id FROM files WHERE id = ? AND is_deleted = 0').get(req.params.id);
  if (!file) return res.status(404).json({ message: 'File not found' });

  db.prepare('DELETE FROM file_tags WHERE file_id = ? AND tag_id = ?').run(file.id, req.params.tagId);

  res.json({ message: 'Tag removed' });
}

module.exports = { listTags, createTag, deleteTag, addFileTags, removeFileTag };
