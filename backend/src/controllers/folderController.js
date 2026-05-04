const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../models/database');
const logger = require('../utils/logger');
const ExcelJS = require('exceljs');

async function listFolders(req, res) {
  const db = getDb();

  const workshops = db.prepare(`
    SELECT f.*, u.display_name as creator_name,
      (SELECT COUNT(*) FROM files fi WHERE fi.is_deleted = 0 AND (
        fi.folder_id IN (SELECT id FROM folders WHERE parent_id IN
          (SELECT id FROM folders WHERE parent_id = f.id AND type = 'line' AND is_deleted = 0) AND is_deleted = 0)
        OR fi.folder_id IN (SELECT id FROM folders WHERE parent_id = f.id AND is_deleted = 0)
      )) as file_count
    FROM folders f
    LEFT JOIN users u ON f.created_by = u.id
    WHERE f.type = 'workshop' AND f.is_deleted = 0
    ORDER BY f.name
  `).all();

  const lines = db.prepare(`
    SELECT f.*, u.display_name as creator_name,
      (
        SELECT COUNT(*) FROM files fi
        WHERE fi.is_deleted = 0 AND (
          fi.folder_id = f.id OR
          fi.folder_id IN (SELECT id FROM folders WHERE parent_id = f.id AND is_deleted = 0)
        )
      ) as file_count
    FROM folders f
    LEFT JOIN users u ON f.created_by = u.id
    WHERE f.type = 'line' AND f.is_deleted = 0
    ORDER BY f.name
  `).all();

  const machines = db.prepare(`
    SELECT f.*, u.display_name as creator_name,
      (SELECT COUNT(*) FROM files fi WHERE fi.folder_id = f.id AND fi.is_deleted = 0) as file_count
    FROM folders f
    LEFT JOIN users u ON f.created_by = u.id
    WHERE f.type = 'machine' AND f.is_deleted = 0
    ORDER BY f.name
  `).all();

  const machinesByParent = {};
  for (const m of machines) {
    if (!machinesByParent[m.parent_id]) machinesByParent[m.parent_id] = [];
    machinesByParent[m.parent_id].push({
      id: m.id,
      name: m.name,
      description: m.description,
      createdBy: m.creator_name,
      createdAt: m.created_at,
      fileCount: m.file_count,
    });
  }

  const linesByWorkshop = {};
  const standaloneLines = [];
  for (const l of lines) {
    const lineObj = {
      id: l.id,
      name: l.name,
      description: l.description,
      createdBy: l.creator_name,
      createdAt: l.created_at,
      fileCount: l.file_count,
      parentId: l.parent_id,
      machines: machinesByParent[l.id] || [],
    };
    if (l.parent_id) {
      if (!linesByWorkshop[l.parent_id]) linesByWorkshop[l.parent_id] = [];
      linesByWorkshop[l.parent_id].push(lineObj);
    } else {
      standaloneLines.push(lineObj);
    }
  }

  const workshopResult = workshops.map(w => ({
    id: w.id,
    name: w.name,
    description: w.description,
    createdBy: w.creator_name,
    createdAt: w.created_at,
    fileCount: w.file_count,
    lines: linesByWorkshop[w.id] || [],
  }));

  // Return workshops array + standalone lines (lines without a workshop parent) for backwards compat
  res.json({ workshops: workshopResult, lines: standaloneLines });
}

async function createFolder(req, res) {
  const db = getDb();
  const { name, type, parentId, description } = req.body;

  if (!name || !name.trim()) {
    return res.status(400).json({ message: 'Folder name is required' });
  }

  if (!type || !['workshop', 'line', 'machine'].includes(type)) {
    return res.status(400).json({ message: 'type must be "workshop", "line" or "machine"' });
  }

  // workshop cannot have a parent
  if (type === 'workshop' && parentId) {
    return res.status(400).json({ message: 'Workshop folders cannot have a parent' });
  }

  // line can optionally have a workshop parent
  if (type === 'line' && parentId) {
    const parent = db.prepare('SELECT id, type FROM folders WHERE id = ? AND is_deleted = 0').get(parentId);
    if (!parent) return res.status(404).json({ message: 'Parent workshop not found' });
    if (parent.type !== 'workshop') return res.status(400).json({ message: 'Parent of a line must be a workshop' });
  }

  // machine must have a line parent
  if (type === 'machine' && !parentId) {
    return res.status(400).json({ message: 'parentId (line id) is required for machine folders' });
  }

  if (type === 'machine' && parentId) {
    const parent = db.prepare('SELECT id, type FROM folders WHERE id = ? AND is_deleted = 0').get(parentId);
    if (!parent) return res.status(404).json({ message: 'Parent line not found' });
    if (parent.type !== 'line') return res.status(400).json({ message: 'Parent must be a line folder' });
  }

  const id = uuidv4();
  try {
    db.prepare(`
      INSERT INTO folders (id, name, type, parent_id, description, created_by)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(id, name.trim(), type, parentId || null, description || null, req.user.id);
  } catch (err) {
    if (err.message && err.message.includes('UNIQUE')) {
      return res.status(409).json({ message: 'A folder with this name already exists in the same location' });
    }
    throw err;
  }

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'create_folder', 'folder', ?, ?)
  `).run(uuidv4(), req.user.id, id, name.trim());

  logger.info('Folder created', { folderId: id, name, type, userId: req.user.id });

  const folder = db.prepare('SELECT * FROM folders WHERE id = ?').get(id);
  res.status(201).json({
    id: folder.id,
    name: folder.name,
    type: folder.type,
    parentId: folder.parent_id,
    description: folder.description,
    createdAt: folder.created_at,
  });
}

async function updateFolder(req, res) {
  const db = getDb();
  const { name, description } = req.body;
  const folder = db.prepare('SELECT * FROM folders WHERE id = ? AND is_deleted = 0').get(req.params.id);

  if (!folder) return res.status(404).json({ message: 'Folder not found' });

  if (name !== undefined && !name.trim()) {
    return res.status(400).json({ message: 'Folder name cannot be empty' });
  }

  const newName = name !== undefined ? name.trim() : folder.name;
  const newDescription = description !== undefined ? description : folder.description;

  try {
    db.prepare(`
      UPDATE folders SET name = ?, description = ?, updated_at = datetime('now') || 'Z'
      WHERE id = ?
    `).run(newName, newDescription, folder.id);
  } catch (err) {
    if (err.message && err.message.includes('UNIQUE')) {
      return res.status(409).json({ message: 'A folder with this name already exists in the same location' });
    }
    throw err;
  }

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'update_folder', 'folder', ?, ?)
  `).run(uuidv4(), req.user.id, folder.id, newName);

  logger.info('Folder updated', { folderId: folder.id, name: newName, userId: req.user.id });

  res.json({ id: folder.id, name: newName, description: newDescription });
}

async function deleteFolder(req, res) {
  const db = getDb();
  const folder = db.prepare('SELECT * FROM folders WHERE id = ? AND is_deleted = 0').get(req.params.id);

  if (!folder) return res.status(404).json({ message: 'Folder not found' });

  // Reject if there are active files in this folder
  const fileCount = db.prepare(
    'SELECT COUNT(*) as cnt FROM files WHERE folder_id = ? AND is_deleted = 0'
  ).get(folder.id).cnt;

  if (fileCount > 0) {
    return res.status(409).json({ message: 'Cannot delete folder with active files' });
  }

  // For workshop folders, check all descendant lines and machines
  if (folder.type === 'workshop') {
    const lineIds = db.prepare(
      'SELECT id FROM folders WHERE parent_id = ? AND is_deleted = 0'
    ).all(folder.id).map(r => r.id);

    for (const lineId of lineIds) {
      const childFileCount = db.prepare(`
        SELECT COUNT(*) as cnt FROM files fi
        INNER JOIN folders fo ON fi.folder_id = fo.id
        WHERE fo.parent_id = ? AND fo.is_deleted = 0 AND fi.is_deleted = 0
      `).get(lineId).cnt;
      if (childFileCount > 0) {
        return res.status(409).json({ message: 'Cannot delete workshop: machines inside still have active files' });
      }
      const lineFileCount = db.prepare(
        'SELECT COUNT(*) as cnt FROM files WHERE folder_id = ? AND is_deleted = 0'
      ).get(lineId).cnt;
      if (lineFileCount > 0) {
        return res.status(409).json({ message: 'Cannot delete workshop: lines inside still have active files' });
      }
      // Soft-delete machines under this line
      db.prepare(`
        UPDATE folders SET is_deleted = 1, updated_at = datetime('now') || 'Z'
        WHERE parent_id = ? AND is_deleted = 0
      `).run(lineId);
    }
    // Soft-delete all lines under the workshop
    db.prepare(`
      UPDATE folders SET is_deleted = 1, updated_at = datetime('now') || 'Z'
      WHERE parent_id = ? AND is_deleted = 0
    `).run(folder.id);
  }

  // For line folders, also check if any child machine has files
  if (folder.type === 'line') {
    const childFileCount = db.prepare(`
      SELECT COUNT(*) as cnt FROM files fi
      INNER JOIN folders fo ON fi.folder_id = fo.id
      WHERE fo.parent_id = ? AND fo.is_deleted = 0 AND fi.is_deleted = 0
    `).get(folder.id).cnt;

    if (childFileCount > 0) {
      return res.status(409).json({ message: 'Cannot delete line folder: machines inside still have active files' });
    }

    // Soft-delete child machines too
    db.prepare(`
      UPDATE folders SET is_deleted = 1, updated_at = datetime('now') || 'Z'
      WHERE parent_id = ? AND is_deleted = 0
    `).run(folder.id);
  }

  db.prepare(`
    UPDATE folders SET is_deleted = 1, updated_at = datetime('now') || 'Z'
    WHERE id = ?
  `).run(folder.id);

  db.prepare(`
    INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name)
    VALUES (?, ?, 'delete_folder', 'folder', ?, ?)
  `).run(uuidv4(), req.user.id, folder.id, folder.name);

  logger.info('Folder deleted', { folderId: folder.id, name: folder.name, userId: req.user.id });

  res.json({ message: 'Folder deleted' });
}

// Export folders as CSV
async function exportFolders(req, res) {
  const db = getDb();
  const folders = db.prepare(`
    SELECT f.name, f.type, f.description,
           p.name as parent_name, gp.name as grandparent_name
    FROM folders f
    LEFT JOIN folders p ON f.parent_id = p.id
    LEFT JOIN folders gp ON p.parent_id = gp.id
    WHERE f.is_deleted = 0
    ORDER BY f.type, p.name, f.name
  `).all();

  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet('Folders');

  sheet.columns = [
    { header: 'Workshop', key: 'workshop', width: 20 },
    { header: 'Line', key: 'line', width: 20 },
    { header: 'Machine', key: 'machine', width: 20 },
    { header: 'Description', key: 'description', width: 30 },
  ];

  for (const f of folders) {
    let workshop = '', line = '', machine = '';
    if (f.type === 'workshop') {
      workshop = f.name;
    } else if (f.type === 'line') {
      workshop = f.parent_name || '';
      line = f.name;
    } else {
      workshop = f.grandparent_name || '';
      line = f.parent_name || '';
      machine = f.name;
    }
    sheet.addRow({ workshop, line, machine, description: f.description || '' });
  }

  const buffer = await workbook.xlsx.writeBuffer();
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', 'attachment; filename="folders.xlsx"');
  res.send(buffer);
}

// Import folders from CSV or Excel
// Helper to safely extract plain text from an ExcelJS cell value.
// ExcelJS can return strings, numbers, Date objects, rich-text objects
// ({ richText: [{text:...},...] }), formula results ({ formula, result }),
// or hyperlink objects ({ text, hyperlink }) — all of which would render as
// "[object Object]" if passed through String() directly.
function getCellText(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  if (value instanceof Date) return '';
  // Rich text: { richText: [{ text: '...' }, ...] }
  if (Array.isArray(value?.richText)) {
    return value.richText.map(r => (typeof r.text === 'string' ? r.text : '')).join('').trim();
  }
  // Formula result: { formula: '...', result: <value> }
  if (value?.result !== undefined) return getCellText(value.result);
  // Hyperlink / shared string: { text: '...', hyperlink: '...' }
  if (value?.text !== undefined) return getCellText(value.text);
  return '';
}

async function importFolders(req, res) {
  if (!req.file) return res.status(400).json({ message: 'No file uploaded' });

  let rows = [];
  const ext = (req.file.originalname || '').toLowerCase();

  if (ext.endsWith('.csv')) {
    const text = req.file.buffer.toString('utf8');
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    // Skip header
    for (let i = 1; i < lines.length; i++) {
      const [workshop, line, machine, description] = lines[i].split(',').map(s => s.trim());
      rows.push({ workshop: workshop || '', line: line || '', machine: machine || '', description: description || '' });
    }
  } else if (ext.endsWith('.xlsx') || ext.endsWith('.xls')) {
    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(req.file.buffer);
    const worksheet = workbook.worksheets[0];
    if (!worksheet) return res.status(400).json({ message: 'Excel file has no worksheets' });
    worksheet.eachRow((row, rowNumber) => {
      if (rowNumber === 1) return; // skip header
      const workshop = getCellText(row.getCell(1).value);
      const line = getCellText(row.getCell(2).value);
      const machine = getCellText(row.getCell(3).value);
      const description = getCellText(row.getCell(4).value);
      if (workshop || line || machine) {
        rows.push({ workshop, line, machine, description });
      }
    });
  } else {
    return res.status(400).json({ message: 'Only .csv and .xlsx files are supported' });
  }

  const db = getDb();
  let created = 0, skipped = 0;

  const getOrCreate = (name, type, parentId) => {
    if (!name) return null;
    let existing;
    if (parentId) {
      existing = db.prepare(
        'SELECT id FROM folders WHERE name = ? AND type = ? AND parent_id = ? AND is_deleted = 0'
      ).get(name, type, parentId);
    } else {
      existing = db.prepare(
        'SELECT id FROM folders WHERE name = ? AND type = ? AND parent_id IS NULL AND is_deleted = 0'
      ).get(name, type);
    }
    if (existing) return existing.id;
    const id = uuidv4();
    db.prepare('INSERT INTO folders (id, name, type, parent_id, description, created_by) VALUES (?, ?, ?, ?, ?, ?)')
      .run(id, name, type, parentId || null, null, req.user.id);
    db.prepare(
      "INSERT INTO activity_log (id, user_id, action, entity_type, entity_id, entity_name) VALUES (?, ?, 'create_folder', 'folder', ?, ?)"
    ).run(uuidv4(), req.user.id, id, name);
    created++;
    return id;
  };

  try {
    for (const row of rows) {
      const workshopId = row.workshop ? getOrCreate(row.workshop, 'workshop', null) : null;
      const lineId = row.line ? getOrCreate(row.line, 'line', workshopId) : null;
      if (row.machine && lineId) {
        getOrCreate(row.machine, 'machine', lineId);
      } else if (row.machine && !lineId) {
        skipped++;
      }
    }
    res.json({ message: `Import complete. Created: ${created}, Skipped: ${skipped}` });
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
}

module.exports = { listFolders, createFolder, updateFolder, deleteFolder, exportFolders, importFolders };
