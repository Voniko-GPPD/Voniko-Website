const express = require('express');
const router = express.Router();
const { authenticateToken, requireAdmin } = require('../middleware/auth');
const { listTags, createTag, deleteTag } = require('../controllers/tagController');

router.get('/', authenticateToken, listTags);
router.post('/', authenticateToken, createTag);
router.delete('/:id', authenticateToken, requireAdmin, deleteTag);

module.exports = router;
