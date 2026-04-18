const ExcelJS = require('exceljs');

const TAG_REGEX = /\{\{\s*([A-Za-z0-9_]+)\s*\}\}/g;
const OPEN_BLOCK_REGEX = /^\{\{\s*#([A-Za-z0-9_]+)\s*\}\}$/;
const CLOSE_BLOCK_REGEX = /^\{\{\s*\/([A-Za-z0-9_]+)\s*\}\}$/;
const BLOCK_TAGS_REGEX = /\{\{\s*[#/][A-Za-z0-9_]+\s*\}\}/g;

function getResolvedValue(key, localContext, rootContext) {
  if (localContext && Object.prototype.hasOwnProperty.call(localContext, key)) {
    return localContext[key];
  }
  if (Object.prototype.hasOwnProperty.call(rootContext, key)) {
    return rootContext[key];
  }
  return '';
}

function replaceScalarTags(value, localContext, rootContext) {
  if (typeof value !== 'string') return value;
  return value.replace(TAG_REGEX, (_match, key) => {
    const resolved = getResolvedValue(key, localContext, rootContext);
    return resolved == null ? '' : String(resolved);
  });
}

function getRowBoundaryTags(row) {
  const textCells = row.values
    .slice(1)
    .filter((cellValue) => typeof cellValue === 'string' && cellValue.trim() !== '')
    .map((cellValue) => cellValue.trim());

  if (textCells.length < 2) return null;

  const openMatch = textCells[0].match(OPEN_BLOCK_REGEX);
  const closeMatch = textCells[textCells.length - 1].match(CLOSE_BLOCK_REGEX);

  if (!openMatch || !closeMatch) return null;
  if (openMatch[1] !== closeMatch[1]) return null;

  return openMatch[1];
}

function renderArrayRow(row, rowContext, rootContext) {
  row.eachCell({ includeEmpty: false }, (cell) => {
    if (typeof cell.value !== 'string') return;
    const withoutBlockTags = cell.value.replace(BLOCK_TAGS_REGEX, '');
    cell.value = replaceScalarTags(withoutBlockTags, rowContext, rootContext);
  });
}

async function renderExcelTemplate(templatePath, context) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(templatePath);

  workbook.eachSheet((sheet) => {
    for (let rowNumber = 1; rowNumber <= sheet.rowCount; rowNumber += 1) {
      const row = sheet.getRow(rowNumber);
      const arrayKey = getRowBoundaryTags(row);
      if (!arrayKey) continue;

      const rowsData = Array.isArray(context[arrayKey]) ? context[arrayKey] : [];
      if (rowsData.length === 0) {
        sheet.spliceRows(rowNumber, 1);
        rowNumber -= 1;
        continue;
      }

      if (rowsData.length > 1) {
        sheet.duplicateRow(rowNumber, rowsData.length - 1, true);
      }

      rowsData.forEach((rowContext, index) => {
        const expandedRow = sheet.getRow(rowNumber + index);
        renderArrayRow(expandedRow, rowContext, context);
      });

      rowNumber += rowsData.length - 1;
    }

    sheet.eachRow((row) => {
      row.eachCell({ includeEmpty: false }, (cell) => {
        if (typeof cell.value !== 'string') return;
        cell.value = replaceScalarTags(cell.value, null, context);
      });
    });
  });

  return workbook.xlsx.writeBuffer();
}

module.exports = { renderExcelTemplate };
