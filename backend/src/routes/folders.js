const express = require('express');
const router = express.Router();
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage() });
const { authenticateToken, requireAdmin } = require('../middleware/auth');
const { listFolders, createFolder, updateFolder, deleteFolder, exportFolders, importFolders } = require('../controllers/folderController');

router.get('/export', authenticateToken, requireAdmin, exportFolders);
router.post('/import', authenticateToken, requireAdmin, upload.single('file'), importFolders);
router.get('/', authenticateToken, listFolders);
router.post('/', authenticateToken, requireAdmin, createFolder);
router.put('/:id', authenticateToken, requireAdmin, updateFolder);
router.delete('/:id', authenticateToken, requireAdmin, deleteFolder);

module.exports = router;
