const express = require('express');
const router = express.Router();
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage() });
const { authenticateToken, requireEngineerOrAbove } = require('../middleware/auth');
const { listFolders, createFolder, updateFolder, deleteFolder, exportFolders, importFolders } = require('../controllers/folderController');

router.get('/export', authenticateToken, requireEngineerOrAbove, exportFolders);
router.post('/import', authenticateToken, requireEngineerOrAbove, upload.single('file'), importFolders);
router.get('/', authenticateToken, listFolders);
router.post('/', authenticateToken, requireEngineerOrAbove, createFolder);
router.put('/:id', authenticateToken, requireEngineerOrAbove, updateFolder);
router.delete('/:id', authenticateToken, requireEngineerOrAbove, deleteFolder);

module.exports = router;
