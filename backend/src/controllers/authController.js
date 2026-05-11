const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../models/database');
const config = require('../config');
const logger = require('../utils/logger');

function generateTokens(userId) {
  const accessToken = jwt.sign({ userId }, config.jwt.secret, {
    expiresIn: config.jwt.expiresIn,
  });
  const refreshToken = jwt.sign({ userId, type: 'refresh' }, config.jwt.secret, {
    expiresIn: config.jwt.refreshExpiresIn,
  });
  return { accessToken, refreshToken };
}

async function login(req, res) {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ message: 'Username and password required' });
  }

  const db = getDb();
  const user = db
    .prepare('SELECT * FROM users WHERE username = ? AND is_active = 1')
    .get(username.trim().toLowerCase());

  if (!user) {
    return res.status(401).json({ message: 'Invalid credentials' });
  }

  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) {
    return res.status(401).json({ message: 'Invalid credentials' });
  }

  const { accessToken, refreshToken } = generateTokens(user.id);

  // Store refresh token hash
  const tokenHash = require('crypto').createHash('sha256').update(refreshToken).digest('hex');
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000)
    .toISOString()
    .replace('T', ' ')
    .split('.')[0];

  db.prepare(`
    INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at)
    VALUES (?, ?, ?, ?)
  `).run(uuidv4(), user.id, tokenHash, expiresAt);

  // Log activity
  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_name)
    VALUES (?, ?, 'login', 'session', ?)
  `).run(uuidv4(), user.id, user.username);

  logger.info('User logged in', { userId: user.id, username: user.username });

  res.json({
    accessToken,
    refreshToken,
    user: {
      id: user.id,
      username: user.username,
      displayName: user.display_name,
      role: user.role,
      avatarUrl: user.avatar_url,
    },
  });
}

async function refresh(req, res) {
  const { refreshToken } = req.body;
  if (!refreshToken) {
    return res.status(400).json({ message: 'Refresh token required' });
  }

  try {
    const payload = jwt.verify(refreshToken, config.jwt.secret);
    if (payload.type !== 'refresh') {
      return res.status(401).json({ message: 'Invalid token type' });
    }

    const db = getDb();
    const tokenHash = require('crypto').createHash('sha256').update(refreshToken).digest('hex');
    const stored = db
      .prepare('SELECT * FROM refresh_tokens WHERE token_hash = ? AND user_id = ?')
      .get(tokenHash, payload.userId);

    if (!stored) {
      return res.status(401).json({ message: 'Refresh token not found or revoked' });
    }

    if (new Date(stored.expires_at) < new Date()) {
      db.prepare('DELETE FROM refresh_tokens WHERE id = ?').run(stored.id);
      return res.status(401).json({ message: 'Refresh token expired' });
    }

    const { accessToken, refreshToken: newRefreshToken } = generateTokens(payload.userId);

    // Rotate refresh token
    const newHash = require('crypto').createHash('sha256').update(newRefreshToken).digest('hex');
    const newExpiry = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000)
      .toISOString()
      .replace('T', ' ')
      .split('.')[0];

    db.prepare('DELETE FROM refresh_tokens WHERE id = ?').run(stored.id);
    db.prepare(`
      INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at)
      VALUES (?, ?, ?, ?)
    `).run(uuidv4(), payload.userId, newHash, newExpiry);

    res.json({ accessToken, refreshToken: newRefreshToken });
  } catch {
    return res.status(401).json({ message: 'Invalid or expired refresh token' });
  }
}

async function logout(req, res) {
  const { refreshToken } = req.body;
  const db = getDb();

  if (refreshToken) {
    const tokenHash = require('crypto').createHash('sha256').update(refreshToken).digest('hex');
    db.prepare('DELETE FROM refresh_tokens WHERE token_hash = ?').run(tokenHash);
  }

  // Log activity
  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_name)
    VALUES (?, ?, 'logout', 'session', ?)
  `).run(uuidv4(), req.user.id, req.user.username);

  res.json({ message: 'Logged out successfully' });
}

async function getMe(req, res) {
  res.json({
    id: req.user.id,
    username: req.user.username,
    displayName: req.user.display_name,
    role: req.user.role,
    avatarUrl: req.user.avatar_url,
  });
}

module.exports = { login, refresh, logout, getMe };
