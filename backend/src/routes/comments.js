const express = require('express');
const router = express.Router();
const { authenticateToken } = require('../middleware/auth');
const { getVersionComments, addVersionComment, updateComment, deleteComment } = require('../controllers/commentController');

router.get('/:id/comments', authenticateToken, getVersionComments);
router.post('/:id/comments', authenticateToken, addVersionComment);

// Update/delete comment by id
router.put('/comments/:id', authenticateToken, updateComment);
router.delete('/comments/:id', authenticateToken, deleteComment);

module.exports = router;
