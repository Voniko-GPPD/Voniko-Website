const jwt = require('jsonwebtoken');
const config = require('../config');
const { getDb } = require('../models/database');

function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader?.split(' ')[1] || req.query.token;

  if (!token) {
    return res.status(401).json({ message: 'Access token required' });
  }

  try {
    const payload = jwt.verify(token, config.jwt.secret);
    const db = getDb();
    const user = db
      .prepare('SELECT id, username, display_name, role, is_active, avatar_url FROM users WHERE id = ?')
      .get(payload.userId);

    if (!user || !user.is_active) {
      return res.status(401).json({ message: 'User not found or deactivated' });
    }

    req.user = user;
    next();
  } catch (err) {
    return res.status(401).json({ message: 'Invalid or expired token' });
  }
}

function requireAdmin(req, res, next) {
  if (!req.user || req.user.role !== 'admin') {
    return res.status(403).json({ message: 'Admin access required' });
  }
  next();
}

function requireEngineerOrAbove(req, res, next) {
  if (!req.user || !['admin', 'engineer'].includes(req.user.role)) {
    return res.status(403).json({ message: 'Engineer or above access required' });
  }
  next();
}

module.exports = { authenticateToken, requireAdmin, requireEngineerOrAbove };
