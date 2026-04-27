const express = require('express');
const router = express.Router();
const multer = require('multer');
const os = require('os');
const bcrypt = require('bcryptjs');
const { authenticateToken, requireAdmin } = require('../middleware/auth');
const {
  listUsers, createUser, getUser, updateUser, deleteUser, permanentDeleteUser,
  updateProfile, changePassword, uploadAvatar,
} = require('../controllers/userController');
const { getDb } = require('../models/database');

const avatarUpload = multer({
  dest: os.tmpdir(),
  limits: { fileSize: 5 * 1024 * 1024 }, // 5 MB max
});

// Profile operations (for self)
router.get('/me', authenticateToken, (req, res) => {
  req.params.id = req.user.id;
  return getUser(req, res);
});
router.put('/me/profile', authenticateToken, updateProfile);
router.put('/me/password', authenticateToken, changePassword);
router.post('/me/avatar', authenticateToken, avatarUpload.single('avatar'), uploadAvatar);

// Admin-only user management
router.get('/', authenticateToken, requireAdmin, listUsers);
router.post('/', authenticateToken, requireAdmin, createUser);
router.get('/:id', authenticateToken, requireAdmin, getUser);
router.put('/:id', authenticateToken, requireAdmin, updateUser);
router.delete('/:id', authenticateToken, requireAdmin, deleteUser);
router.delete('/:id/permanent', authenticateToken, requireAdmin, permanentDeleteUser);

// Reset password (admin only)
router.put('/:id/reset-password', authenticateToken, requireAdmin, async (req, res) => {
  const { password } = req.body;
  if (!password || password.length < 6) {
    return res.status(400).json({ message: 'Password must be at least 6 characters' });
  }
  const db = getDb();
  const user = db.prepare('SELECT id FROM users WHERE id = ?').get(req.params.id);
  if (!user) return res.status(404).json({ message: 'User not found' });
  const passwordHash = await bcrypt.hash(password, 10);
  db.prepare('UPDATE users SET password_hash = ? WHERE id = ?').run(passwordHash, req.params.id);
  res.json({ message: 'Password reset successfully' });
});

module.exports = router;
