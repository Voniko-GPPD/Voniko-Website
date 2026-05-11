const bcrypt = require('bcryptjs');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const { getDb } = require('../models/database');
const logger = require('../utils/logger');
const config = require('../config');
const { getOnlineUserIds, getLastSeen } = require('../utils/notifications');

async function listUsers(req, res) {
  const db = getDb();
  const users = db.prepare(`
    SELECT u.id, u.username, u.display_name, u.role, u.is_active, u.avatar_url, u.created_at,
      (SELECT a.created_at FROM activity_log a WHERE a.user_id = u.id AND a.action = 'login' ORDER BY a.created_at DESC LIMIT 1) as last_login
    FROM users u
    ORDER BY u.created_at DESC
  `).all();
  const onlineIds = getOnlineUserIds();
  res.json(users.map(u => {
    const isOnline = onlineIds.has(u.id);
    const lastSeenTs = isOnline ? null : getLastSeen(u.id);
    return {
      id: u.id,
      username: u.username,
      displayName: u.display_name,
      role: u.role,
      isActive: !!u.is_active,
      avatarUrl: u.avatar_url,
      createdAt: u.created_at,
      lastLogin: u.last_login || null,
      isOnline,
      lastSeen: lastSeenTs ? new Date(lastSeenTs).toISOString() : null,
    };
  }));
}

async function createUser(req, res) {
  const { username, password, displayName, role } = req.body;
  if (!username || !password || !displayName) {
    return res.status(400).json({ message: 'username, password, displayName required' });
  }
  if (!['admin', 'engineer', 'qc', 'lab'].includes(role)) {
    return res.status(400).json({ message: 'Invalid role. Use: admin, engineer, qc, lab' });
  }

  const db = getDb();
  const existing = db.prepare('SELECT id FROM users WHERE username = ?').get(username.trim().toLowerCase());
  if (existing) {
    return res.status(409).json({ message: 'Username already exists' });
  }

  if (password.length < 6) {
    return res.status(400).json({ message: 'Password must be at least 6 characters' });
  }

  const id = uuidv4();
  const passwordHash = await bcrypt.hash(password, 10);
  db.prepare(`
    INSERT INTO users (id, username, password_hash, display_name, role)
    VALUES (?, ?, ?, ?, ?)
  `).run(id, username.trim().toLowerCase(), passwordHash, displayName.trim(), role);

  // Log activity
  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'create_user', 'user', ?, ?)
  `).run(uuidv4(), req.user.id, id, username);

  logger.info('User created', { createdBy: req.user.id, newUser: username });
  res.status(201).json({ id, username: username.trim().toLowerCase(), displayName: displayName.trim(), role });
}

async function getUser(req, res) {
  const db = getDb();
  const user = db
    .prepare('SELECT id, username, display_name, role, is_active, avatar_url, created_at FROM users WHERE id = ?')
    .get(req.params.id);
  if (!user) return res.status(404).json({ message: 'User not found' });
  res.json({
    id: user.id,
    username: user.username,
    displayName: user.display_name,
    role: user.role,
    isActive: !!user.is_active,
    avatarUrl: user.avatar_url,
    createdAt: user.created_at,
  });
}

async function updateUser(req, res) {
  const { displayName, role, isActive } = req.body;
  const db = getDb();

  const user = db.prepare('SELECT id, username FROM users WHERE id = ?').get(req.params.id);
  if (!user) return res.status(404).json({ message: 'User not found' });

  // Prevent admin from deactivating themselves
  if (req.params.id === req.user.id && isActive === false) {
    return res.status(400).json({ message: 'Cannot deactivate yourself' });
  }

  // Prevent admin from changing their own role if they are the only active admin
  if (req.params.id === req.user.id && role !== undefined && role !== 'admin') {
    const otherAdminCount = db.prepare(
      "SELECT COUNT(*) as cnt FROM users WHERE role='admin' AND is_active=1 AND id != ?"
    ).get(req.params.id).cnt;
    if (otherAdminCount === 0) {
      return res.status(400).json({ message: 'Cannot change your own role: you are the only active admin' });
    }
  }

  const updates = [];
  const values = [];

  if (displayName !== undefined) { updates.push('display_name = ?'); values.push(displayName.trim()); }
  if (role !== undefined) {
    if (!['admin', 'engineer', 'qc', 'lab'].includes(role)) {
      return res.status(400).json({ message: 'Invalid role' });
    }
    updates.push('role = ?'); values.push(role);
  }
  if (isActive !== undefined) { updates.push('is_active = ?'); values.push(isActive ? 1 : 0); }

  if (updates.length === 0) return res.status(400).json({ message: 'Nothing to update' });

  updates.push("updated_at = datetime('now') || 'Z'");
  values.push(req.params.id);

  db.prepare(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`).run(...values);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'update_user', 'user', ?, ?)
  `).run(uuidv4(), req.user.id, req.params.id, user.username);

  res.json({ message: 'User updated' });
}

async function deleteUser(req, res) {
  const db = getDb();
  if (req.params.id === req.user.id) {
    return res.status(400).json({ message: 'Cannot delete yourself' });
  }
  const user = db.prepare('SELECT id, username FROM users WHERE id = ?').get(req.params.id);
  if (!user) return res.status(404).json({ message: 'User not found' });

  db.prepare('UPDATE users SET is_active = 0, updated_at = datetime(\'now\') || \'Z\' WHERE id = ?').run(req.params.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'delete_user', 'user', ?, ?)
  `).run(uuidv4(), req.user.id, req.params.id, user.username);

  res.json({ message: 'User deactivated' });
}

async function permanentDeleteUser(req, res) {
  const db = getDb();
  if (req.params.id === req.user.id) {
    return res.status(400).json({ message: 'Cannot delete yourself' });
  }
  const user = db.prepare('SELECT id, username, is_active FROM users WHERE id = ?').get(req.params.id);
  if (!user) return res.status(404).json({ message: 'User not found' });
  if (user.is_active) {
    return res.status(400).json({ message: 'Cannot permanently delete an active user. Deactivate first.' });
  }

  db.prepare('DELETE FROM refresh_tokens WHERE user_id = ?').run(req.params.id);
  db.prepare('DELETE FROM users WHERE id = ?').run(req.params.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'permanent_delete_user', 'user', ?, ?)
  `).run(uuidv4(), req.user.id, req.params.id, user.username);

  logger.info('User permanently deleted', { deletedBy: req.user.id, deletedUser: user.username });
  res.json({ message: 'User permanently deleted' });
}

async function updateProfile(req, res) {
  const { displayName, avatarUrl } = req.body;
  const db = getDb();

  const updates = [];
  const values = [];

  if (displayName !== undefined) { updates.push('display_name = ?'); values.push(displayName.trim()); }
  if (avatarUrl !== undefined) { updates.push('avatar_url = ?'); values.push(avatarUrl || null); }

  if (values.length === 0) return res.status(400).json({ message: 'Nothing to update' });

  updates.push("updated_at = datetime('now') || 'Z'");
  values.push(req.user.id);
  db.prepare(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`).run(...values);

  res.json({ message: 'Profile updated' });
}

async function changePassword(req, res) {
  const { currentPassword, newPassword } = req.body;
  if (!currentPassword || !newPassword) {
    return res.status(400).json({ message: 'currentPassword and newPassword required' });
  }
  if (newPassword.length < 6) {
    return res.status(400).json({ message: 'Password must be at least 6 characters' });
  }

  const db = getDb();
  const user = db.prepare('SELECT password_hash FROM users WHERE id = ?').get(req.user.id);

  const valid = await bcrypt.compare(currentPassword, user.password_hash);
  if (!valid) {
    return res.status(400).json({ message: 'Current password is incorrect' });
  }

  const newHash = await bcrypt.hash(newPassword, 10);
  db.prepare("UPDATE users SET password_hash = ?, updated_at = datetime('now') || 'Z' WHERE id = ?")
    .run(newHash, req.user.id);

  // Revoke all refresh tokens
  db.prepare('DELETE FROM refresh_tokens WHERE user_id = ?').run(req.user.id);

  res.json({ message: 'Password changed successfully' });
}

async function uploadAvatar(req, res) {
  if (!req.file) {
    return res.status(400).json({ message: 'No file uploaded' });
  }

  const db = getDb();
  const avatarsDir = path.join(config.dataDir, 'avatars');
  if (!fs.existsSync(avatarsDir)) {
    fs.mkdirSync(avatarsDir, { recursive: true });
  }

  const ext = path.extname(req.file.originalname).toLowerCase() || '.jpg';
  const filename = `${req.user.id}_${Date.now()}${ext}`;
  const destPath = path.join(avatarsDir, filename);

  // Remove old avatar file if it was uploaded (local path)
  const existingUser = db.prepare('SELECT avatar_url FROM users WHERE id = ?').get(req.user.id);
  if (existingUser?.avatar_url && existingUser.avatar_url.startsWith('/uploads/avatars/')) {
    const oldFile = path.join(avatarsDir, path.basename(existingUser.avatar_url));
    if (fs.existsSync(oldFile)) {
      try { fs.unlinkSync(oldFile); } catch {}
    }
  }

  try {
    fs.copyFileSync(req.file.path, destPath);
    try { fs.unlinkSync(req.file.path); } catch {}
  } catch (err) {
    logger.error('Failed to save avatar file', { error: err.message });
    return res.status(500).json({ message: 'Failed to save uploaded file' });
  }

  const avatarUrl = `/uploads/avatars/${filename}`;
  db.prepare("UPDATE users SET avatar_url = ?, updated_at = datetime('now') || 'Z' WHERE id = ?")
    .run(avatarUrl, req.user.id);

  res.json({ avatarUrl });
}

module.exports = { listUsers, createUser, getUser, updateUser, deleteUser, permanentDeleteUser, updateProfile, changePassword, uploadAvatar };
