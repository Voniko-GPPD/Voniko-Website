'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const ExcelJS = require('exceljs');
const bwipjs = require('bwip-js');
const PDFDocument = require('pdfkit');
const logger = require('../utils/logger');

// ---------------------------------------------------------------------------
// CJK font resolution (supports Chinese / Japanese / Korean + Vietnamese)
// ---------------------------------------------------------------------------
const BUNDLED_FONT_DIR = path.join(__dirname, '../assets/fonts');

/**
 * Returns the path to a TrueType/OpenType font that supports CJK characters,
 * or null if none is found.  Bundled NotoSansSC fonts are checked first;
 * common system font locations are used as a fallback.
 */
function findCjkFont(variant) {
  const isBold = variant === 'bold';

  // 1. Bundled fonts (placed by scripts/download-fonts.js)
  const bundledCandidates = isBold
    ? ['NotoSansSC-Bold.otf', 'NotoSansSC-Bold.ttf']
    : ['NotoSansSC-Regular.otf', 'NotoSansSC-Regular.ttf'];

  for (const name of bundledCandidates) {
    const p = path.join(BUNDLED_FONT_DIR, name);
    if (fs.existsSync(p)) return p;
  }

  // 2. System font candidates - ONLY .ttf and .otf
  // NOTE: PDFKit does NOT support .ttc (TrueType Collection) files.
  // Never add .ttc paths here.
  const systemCandidates = [
    // Linux - Noto Sans individual .ttf files
    '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttf',
    '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttf',
    // Linux - WQY fonts (common on servers, pure .ttf)
    '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttf',
    '/usr/share/fonts/truetype/wqy/wqy-microhei.ttf',
    '/usr/share/fonts/wqy-zenhei/wqy-zenhei.ttf',
    '/usr/share/fonts/wqy-microhei/wqy-microhei.ttf',
    // Linux - Arphic fonts
    '/usr/share/fonts/truetype/arphic/uming.ttf',
    '/usr/share/fonts/truetype/arphic/ukai.ttf',
    // Windows - individual .ttf (NOT .ttc - msyh.ttc is NOT supported)
    'C:\\Windows\\Fonts\\simhei.ttf',
    'C:\\Windows\\Fonts\\simfang.ttf',
    'C:\\Windows\\Fonts\\SIMYOU.TTF',
    // macOS
    '/System/Library/Fonts/Supplemental/Arial Unicode MS.ttf',
    '/Library/Fonts/Arial Unicode MS.ttf',
  ];

  for (const candidate of systemCandidates) {
    // Runtime guard: skip .ttc files - PDFKit does not support TrueType Collections
    if (candidate.toLowerCase().endsWith('.ttc')) continue;
    if (fs.existsSync(candidate)) return candidate;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Column auto-detection aliases (multi-language)
// ---------------------------------------------------------------------------
const COLUMN_ALIASES = {
  order: ['订单', 'order', 'order number', 'mã đơn hàng', 'order_number', 'order no'],
  date:  ['基本完成日期', 'date', 'completion date', 'ngày hoàn thành', 'ngày', 'due_date'],
  line:  ['产线', 'line', 'production line', 'chuyền', 'chuyền sản xuất', 'line_number'],
  desc:  ['物料描述', 'description', 'mô tả', 'material description', 'desc', 'vật liệu'],
};

function detectColumn(headers, aliases) {
  for (const alias of aliases) {
    const found = headers.find(
      (h) => h != null && String(h).toLowerCase().trim() === alias.toLowerCase()
    );
    if (found) return found;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Default production-line group configuration
// ---------------------------------------------------------------------------
const DEFAULT_GROUPS = [
  { name: 'Group 1: 501 & 504', type: 'split', leftLines: ['501'], rightLines: ['504'] },
  { name: 'Group 2: 701 & 401', type: 'split', leftLines: ['701'], rightLines: ['401'] },
  { name: 'Group 3: 702 & 502', type: 'split', leftLines: ['702'], rightLines: ['502'] },
  { name: 'Group 4: 503',       type: 'mixed', targetLines: ['503'] },
];

// ---------------------------------------------------------------------------
// Parse CSV / Excel file with ExcelJS
// ---------------------------------------------------------------------------
async function parseFile(filePath, originalName) {
  if (!originalName) throw new Error('Original file name is required to determine file format');

  const workbook = new ExcelJS.Workbook();

  const ext = path.extname(originalName).toLowerCase();
  if (ext === '.csv') {
    await workbook.csv.readFile(filePath);
  } else if (ext === '.xls') {
    throw new Error('File format .xls (Excel 97-2003) is not supported. Please save the file as .xlsx or .csv');
  } else {
    // .xlsx
    await workbook.xlsx.readFile(filePath);
  }

  const worksheet = workbook.worksheets[0];
  if (!worksheet) throw new Error('No worksheet found in file');

  const rows = [];
  worksheet.eachRow({ includeEmpty: false }, (row, rowNumber) => {
    const values = row.values.slice(1); // row.values[0] is always undefined
    rows.push(values);
  });

  if (rows.length < 2) throw new Error('File contains no data rows');

  const headers = rows[0].map((h) => (h != null ? String(h) : ''));
  const dataRows = rows.slice(1);

  return { headers, dataRows };
}

// ---------------------------------------------------------------------------
// Format a date cell value to YYYY-MM-DD
// ---------------------------------------------------------------------------
function formatDate(value) {
  if (!value) return '';
  if (value instanceof Date) {
    const y = value.getFullYear();
    const m = String(value.getMonth() + 1).padStart(2, '0');
    const d = String(value.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  // Try to parse string dates
  const str = String(value).trim();
  const d = new Date(str);
  if (!isNaN(d.getTime())) {
    const y = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, '0');
    const dy = String(d.getDate()).padStart(2, '0');
    return `${y}-${mo}-${dy}`;
  }
  return str;
}

// ---------------------------------------------------------------------------
// Generate a Code128 barcode PNG buffer via bwip-js
// ---------------------------------------------------------------------------
async function generateBarcodePng(text) {
  return bwipjs.toBuffer({
    bcid:        'code128',
    text:        String(text),
    scale:       2,
    height:      12,
    includetext: false,
    backgroundcolor: 'ffffff',
  });
}

// ---------------------------------------------------------------------------
// Arrange rows into ordered items based on group config
// ---------------------------------------------------------------------------
function arrangeByGroups(records, groups) {
  const arranged = [];

  for (const group of groups) {
    if (group.type === 'split') {
      const leftItems  = records.filter((r) => (group.leftLines  || []).includes(String(r.line)));
      const rightItems = records.filter((r) => (group.rightLines || []).includes(String(r.line)));
      const maxLen = Math.max(leftItems.length, rightItems.length);
      for (let i = 0; i < maxLen; i++) {
        if (i < leftItems.length)  arranged.push(leftItems[i]);
        if (i < rightItems.length) arranged.push(rightItems[i]);
      }
    } else if (group.type === 'mixed') {
      const items = records.filter((r) => (group.targetLines || []).includes(String(r.line)));
      arranged.push(...items);
    }
  }

  // Append any records that did not match any group
  const handledLines = new Set(
    groups.flatMap((g) => [
      ...(g.leftLines  || []),
      ...(g.rightLines || []),
      ...(g.targetLines || []),
    ])
  );
  const unhandled = records.filter((r) => !handledLines.has(String(r.line)));
  arranged.push(...unhandled);

  return arranged;
}

// ---------------------------------------------------------------------------
// Main controller: POST /api/barcode/generate
// ---------------------------------------------------------------------------
async function generateBarcode(req, res) {
  const tempPath = req.file?.path;

  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Validate that the temp file is inside os.tmpdir() to prevent path injection
    const resolvedTemp = path.resolve(tempPath);
    const tmpDir = path.resolve(os.tmpdir());
    if (!resolvedTemp.startsWith(tmpDir + path.sep)) {
      return res.status(400).json({ error: 'Invalid file path' });
    }

    // Parse groups config from request body (optional)
    let groups = DEFAULT_GROUPS;
    if (req.body?.groups) {
      try {
        const parsed = JSON.parse(req.body.groups);
        if (Array.isArray(parsed) && parsed.length > 0) groups = parsed;
      } catch {
        // Use defaults on parse error
      }
    }

    // Parse file – use the validated resolved path
    const { headers, dataRows } = await parseFile(resolvedTemp, req.file.originalname);

    // Detect columns
    const colOrder = detectColumn(headers, COLUMN_ALIASES.order);
    const colDate  = detectColumn(headers, COLUMN_ALIASES.date);
    const colLine  = detectColumn(headers, COLUMN_ALIASES.line);
    const colDesc  = detectColumn(headers, COLUMN_ALIASES.desc);

    if (!colOrder) return res.status(400).json({ error: 'Cannot find order number column (订单 / order / mã đơn hàng)' });
    if (!colDate)  return res.status(400).json({ error: 'Cannot find date column (基本完成日期 / date / ngày)' });
    if (!colLine)  return res.status(400).json({ error: 'Cannot find production line column (产线 / line / chuyền)' });
    if (!colDesc)  return res.status(400).json({ error: 'Cannot find description column (物料描述 / description / mô tả)' });

    const orderIdx = headers.indexOf(colOrder);
    const dateIdx  = headers.indexOf(colDate);
    const lineIdx  = headers.indexOf(colLine);
    const descIdx  = headers.indexOf(colDesc);

    // Build records
    const records = dataRows
      .map((row) => ({
        order: row[orderIdx] != null ? String(row[orderIdx]).trim() : '',
        date:  formatDate(row[dateIdx]),
        line:  row[lineIdx]  != null ? String(row[lineIdx]).trim()  : '',
        desc:  row[descIdx]  != null ? String(row[descIdx]).trim()  : '',
      }))
      .filter((r) => r.order);

    if (records.length === 0) {
      return res.status(400).json({ error: 'No valid data rows found in file' });
    }

    // Arrange records
    const items = arrangeByGroups(records, groups);

    // --- PDF Layout constants (A4 = 595.28 x 841.89 pts) ---
    const PAGE_W   = 595.28;
    const PAGE_H   = 841.89;
    const MARGIN   = 30;
    const COLS     = 2;
    const ROWS     = 6;
    const CELL_W   = (PAGE_W - MARGIN * 2) / COLS;
    const CELL_H   = (PAGE_H - MARGIN * 2) / ROWS;
    const BAR_W    = CELL_W - 20;
    const BAR_H    = 50;

    const doc = new PDFDocument({ size: 'A4', margin: 0, autoFirstPage: true });

    // Register CJK-capable fonts when available; fall back to Helvetica (Latin only)
    let fontRegular = 'Helvetica';
    let fontBold    = 'Helvetica-Bold';

    const regularFontPath = findCjkFont('regular');
    if (regularFontPath) {
      try {
        doc.registerFont('NotoSans', regularFontPath);
        fontRegular = 'NotoSans';
      } catch (fontErr) {
        logger.warn('Could not register regular CJK font, falling back to Helvetica', { path: regularFontPath, error: fontErr.message });
      }
    } else {
      logger.warn('No CJK font found. Run `node scripts/download-fonts.js` to enable Chinese/Japanese/Korean text rendering.');
    }

    const boldFontPath = findCjkFont('bold');
    if (boldFontPath) {
      try {
        doc.registerFont('NotoSansBold', boldFontPath);
        fontBold = 'NotoSansBold';
      } catch (fontErr) {
        logger.warn('Could not register bold CJK font, falling back to Helvetica-Bold', { path: boldFontPath, error: fontErr.message });
      }
    }

    // Stream PDF directly to response
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="barcodes.pdf"');
    doc.pipe(res);

    for (let i = 0; i < items.length; i++) {
      const item = items[i];

      // Page break
      if (i > 0 && i % (COLS * ROWS) === 0) doc.addPage();

      const posInPage = i % (COLS * ROWS);
      const col = posInPage % COLS;
      const row = Math.floor(posInPage / COLS);

      const x = MARGIN + col * CELL_W;
      const y = MARGIN + row * CELL_H;

      // Draw cell border
      doc.rect(x, y, CELL_W, CELL_H).stroke('#cccccc');

      const padding = 8;
      let curY = y + padding;

      // Date + Line (bold)
      doc.font(fontBold).fontSize(9);
      doc.text(`${item.date}  |  Line ${item.line}`, x + padding, curY, {
        width: CELL_W - padding * 2,
        ellipsis: true,
      });
      curY += 14;

      // Description (regular, small)
      doc.font(fontRegular).fontSize(8);
      doc.text(item.desc || '-', x + padding, curY, {
        width: CELL_W - padding * 2,
        ellipsis: true,
      });
      curY += 13;

      // Barcode image
      try {
        const barcodeBuffer = await generateBarcodePng(item.order);
        const barX = x + (CELL_W - BAR_W) / 2;
        doc.image(barcodeBuffer, barX, curY, { width: BAR_W, height: BAR_H });
      } catch (err) {
        logger.warn('Barcode generation failed for order', { order: item.order, error: err.message });
        // Draw placeholder text when barcode cannot be generated
        doc.font(fontRegular).fontSize(8).fillColor('#cc0000');
        doc.text('[barcode error]', x + padding, curY + BAR_H / 2 - 5, {
          width: CELL_W - padding * 2,
          align: 'center',
        });
        doc.fillColor('black');
      }
      curY += BAR_H + 4;

      // Order number text (large, centered)
      doc.font(fontBold).fontSize(10);
      doc.text(item.order, x + padding, curY, {
        width: CELL_W - padding * 2,
        align: 'center',
        ellipsis: true,
      });
    }

    doc.end();
  } catch (err) {
    logger.error('Barcode generation error', { err: err.message });
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || 'Failed to generate barcode PDF' });
    }
  } finally {
    // Cleanup temp file
    if (tempPath) {
      fs.unlink(tempPath, (unlinkErr) => {
        if (unlinkErr) logger.warn('Failed to delete temp file', { path: tempPath, error: unlinkErr.message });
      });
    }
  }
}

module.exports = { generateBarcode };
