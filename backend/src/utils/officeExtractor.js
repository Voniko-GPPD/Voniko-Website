const fs = require('fs');
const path = require('path');

/**
 * Supported Office MIME types and extensions for text extraction.
 */
const OFFICE_MIME_TYPES = new Set([
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', // .xlsx
  'application/vnd.ms-excel', // .xls
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document', // .docx
  'application/vnd.openxmlformats-officedocument.presentationml.presentation', // .pptx
  'text/csv',
  'application/rtf',
  'text/rtf',
]);

/**
 * Returns true if the given MIME type is a supported Office format.
 */
function isOfficeFile(mimeType) {
  if (!mimeType) return false;
  return OFFICE_MIME_TYPES.has(mimeType.toLowerCase().split(';')[0].trim());
}

/**
 * Extract text from an Excel file (.xlsx / .xls) using exceljs.
 * Returns a string with each sheet formatted as:
 *   [SheetName]
 *   row1col1\trow1col2\t...
 *   row2col1\t...
 */
async function extractExcel(filePath) {
  const ExcelJS = require('exceljs');
  const workbook = new ExcelJS.Workbook();

  // exceljs can read both xlsx and xls (xls via compatibility layer)
  await workbook.xlsx.readFile(filePath);

  const lines = [];
  workbook.eachSheet((sheet) => {
    lines.push(`[${sheet.name}]`);
    sheet.eachRow({ includeEmpty: false }, (row) => {
      const cells = [];
      row.eachCell({ includeEmpty: true }, (cell) => {
        cells.push(cell.text !== undefined ? String(cell.text) : '');
      });
      lines.push(cells.join('\t'));
    });
    lines.push('');
  });

  return lines.join('\n').trim();
}

/**
 * Extract text from a Word document (.docx) using mammoth.
 */
async function extractDocx(filePath) {
  const mammoth = require('mammoth');
  const result = await mammoth.extractRawText({ path: filePath });
  return result.value.trim();
}

/**
 * Extract text from a PowerPoint file (.pptx) by parsing the XML inside the zip.
 */
async function extractPptx(filePath) {
  const JSZip = require('jszip');
  const data = fs.readFileSync(filePath);
  const zip = await JSZip.loadAsync(data);

  // Collect slide file names and sort numerically
  const slideNames = Object.keys(zip.files)
    .filter((name) => /^ppt\/slides\/slide\d+\.xml$/.test(name))
    .sort((a, b) => {
      const numA = parseInt(a.match(/slide(\d+)/)[1], 10);
      const numB = parseInt(b.match(/slide(\d+)/)[1], 10);
      return numA - numB;
    });

  if (slideNames.length === 0) return null;

  const lines = [];
  let slideIndex = 0;
  for (const name of slideNames) {
    slideIndex++;
    const xml = await zip.files[name].async('string');
    // Extract all <a:t>…</a:t> text nodes
    const texts = [];
    const re = /<a:t[^>]*>([^<]*)<\/a:t>/g;
    let m;
    while ((m = re.exec(xml)) !== null) {
      const text = m[1].trim();
      if (text) texts.push(text);
    }
    if (texts.length > 0) {
      lines.push(`[Slide ${slideIndex}]`);
      lines.push(texts.join(' '));
      lines.push('');
    }
  }

  return lines.join('\n').trim() || null;
}

/**
 * Extract text from a CSV file.
 */
function extractCsv(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

/**
 * Strip RTF control words and return plain text.
 */
function extractRtf(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  // Remove RTF control words and groups, then clean up whitespace
  let text = content
    .replace(/\{[^{}]*\}/g, '') // remove simple groups like \{...\}
    .replace(/\\[a-z*]+[-\d]* ?/gi, ' ') // remove control words
    .replace(/[{}\\]/g, '') // remove remaining braces/backslashes
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return text || null;
}

/**
 * Extract plain text from an Office file.
 * @param {string} filePath - Absolute path to the file on disk.
 * @param {string} mimeType - MIME type of the file.
 * @returns {Promise<string|null>} Extracted text, or null if unsupported/failed.
 */
async function extractTextFromFile(filePath, mimeType) {
  if (!filePath || !mimeType) return null;
  const mime = mimeType.toLowerCase().split(';')[0].trim();

  try {
    switch (mime) {
      case 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
      case 'application/vnd.ms-excel':
        return await extractExcel(filePath);

      case 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
        return await extractDocx(filePath);

      case 'application/vnd.openxmlformats-officedocument.presentationml.presentation':
        return await extractPptx(filePath);

      case 'text/csv':
        return extractCsv(filePath);

      case 'application/rtf':
      case 'text/rtf':
        return extractRtf(filePath);

      default:
        return null;
    }
  } catch (err) {
    // Extraction failure is non-fatal; caller logs a warning and falls back to binary diff
    void err;
    return null;
  }
}

module.exports = { extractTextFromFile, isOfficeFile };
