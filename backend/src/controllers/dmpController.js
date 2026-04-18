const fs = require('fs/promises');
const path = require('path');

const logger = require('../utils/logger');
const { withShadowCopy } = require('../utils/mdbShadowCopy');
const { queryMdb } = require('../utils/adodbQuery');
const { renderExcelTemplate } = require('../utils/excelTemplateEngine');

const DEFAULT_TEMPLATES_DIR = path.resolve(__dirname, '../assets/dmp_templates');

function createHttpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function getDmpBaseDir() {
  const baseDir = process.env.DMP_MDB_DIR;
  if (!baseDir) {
    throw createHttpError(500, 'DMP_MDB_DIR is not configured');
  }
  return path.resolve(baseDir);
}

function resolveWithinBase(baseDir, targetPath) {
  const resolvedBase = path.resolve(baseDir);
  const resolvedTarget = path.resolve(resolvedBase, targetPath);

  if (resolvedTarget !== resolvedBase && !resolvedTarget.startsWith(`${resolvedBase}${path.sep}`)) {
    throw createHttpError(400, 'Invalid path');
  }

  return resolvedTarget;
}

function resolveDynamicMdbPath(cdmc) {
  if (!cdmc || typeof cdmc !== 'string') {
    throw createHttpError(400, 'cdmc is required');
  }

  const trimmed = cdmc.trim();
  if (!trimmed) {
    throw createHttpError(400, 'cdmc is required');
  }

  const parsed = path.parse(trimmed);
  if (parsed.dir || parsed.root) {
    throw createHttpError(400, 'Invalid cdmc');
  }

  const extension = parsed.ext.toLowerCase();
  if (extension && extension !== '.mdb') {
    throw createHttpError(400, 'cdmc must be an .mdb file');
  }

  const fileStem = parsed.name || parsed.base;
  if (!/^[A-Za-z0-9_-]+$/.test(fileStem)) {
    throw createHttpError(400, 'Invalid cdmc');
  }

  const withExt = `${fileStem}.mdb`;
  return resolveWithinBase(getDmpBaseDir(), withExt);
}

function validateBatchId(batchId) {
  const value = String(batchId || '').trim();
  if (!value) {
    throw createHttpError(400, 'batchId is required');
  }
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw createHttpError(400, 'Invalid batchId');
  }
  return value;
}

function parseChannelNumber(channel) {
  const value = Number(channel);
  if (!Number.isInteger(value)) {
    throw createHttpError(400, 'channel must be an integer');
  }
  return value;
}

function getDmpDataPath() {
  return resolveWithinBase(getDmpBaseDir(), 'DMPDATA.mdb');
}

function getTemplatesDir() {
  return process.env.DMP_TEMPLATES_DIR
    ? path.resolve(process.env.DMP_TEMPLATES_DIR)
    : DEFAULT_TEMPLATES_DIR;
}

function toFiniteNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function round4(value) {
  return value == null ? null : Number(value.toFixed(4));
}

function computeStats(rows) {
  const voltValues = rows.map((row) => toFiniteNumber(row.VOLT)).filter((value) => value != null);
  const imValues = rows.map((row) => toFiniteNumber(row.Im)).filter((value) => value != null);

  const aggregate = (values) => {
    if (values.length === 0) return { max: null, min: null, avg: null };
    const sum = values.reduce((acc, value) => acc + value, 0);
    return {
      max: round4(Math.max(...values)),
      min: round4(Math.min(...values)),
      avg: round4(sum / values.length),
    };
  };

  const volt = aggregate(voltValues);
  const im = aggregate(imValues);

  return {
    VOLT_MAX: volt.max,
    VOLT_MIN: volt.min,
    VOLT_AVG: volt.avg,
    IM_MAX: im.max,
    IM_MIN: im.min,
    IM_AVG: im.avg,
  };
}

async function queryWithShadowCopy(mdbPath, sql) {
  return withShadowCopy(mdbPath, async (shadowPath) => queryMdb(shadowPath, sql));
}

async function getBatches(req, res, next) {
  try {
    const batches = await queryWithShadowCopy(
      getDmpDataPath(),
      'SELECT id, dcxh, fdrq, fdfs FROM para_pub ORDER BY fdrq DESC'
    );
    res.json({ batches });
  } catch (error) {
    next(error);
  }
}

async function getChannels(req, res, next) {
  try {
    const batchId = validateBatchId(req.params.batchId);
    const channels = await queryWithShadowCopy(
      getDmpDataPath(),
      `SELECT baty, cdmc FROM para_singl WHERE id = '${batchId}'`
    );

    res.json({ channels });
  } catch (error) {
    next(error);
  }
}

async function getTelemetry(req, res, next) {
  try {
    const { cdmc } = req.query;
    const channelNumber = parseChannelNumber(req.query.channel);

    const telemetry = await queryWithShadowCopy(
      resolveDynamicMdbPath(cdmc),
      `SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = ${channelNumber} ORDER BY TIM ASC`
    );

    res.json({ telemetry });
  } catch (error) {
    next(error);
  }
}

async function getStats(req, res, next) {
  try {
    const { cdmc } = req.query;
    const channelNumber = parseChannelNumber(req.query.channel);

    const telemetry = await queryWithShadowCopy(
      resolveDynamicMdbPath(cdmc),
      `SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = ${channelNumber} ORDER BY TIM ASC`
    );

    res.json(computeStats(telemetry));
  } catch (error) {
    next(error);
  }
}

async function getTemplates(req, res, next) {
  try {
    const templateDir = getTemplatesDir();
    const entries = await fs.readdir(templateDir, { withFileTypes: true });
    const templates = entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.xlsx'))
      .map((entry) => entry.name)
      .sort((a, b) => a.localeCompare(b));

    res.json({ templates });
  } catch (error) {
    next(error);
  }
}

async function generateReport(req, res, next) {
  try {
    const { batchId: rawBatchId, cdmc, channel, templateName } = req.body || {};

    if (!rawBatchId || !cdmc || channel == null || !templateName) {
      throw createHttpError(400, 'batchId, cdmc, channel and templateName are required');
    }

    const batchId = validateBatchId(rawBatchId);
    const channelNumber = parseChannelNumber(channel);

    const batchRows = await queryWithShadowCopy(
      getDmpDataPath(),
      `SELECT id, dcxh, fdrq, fdfs FROM para_pub WHERE id = '${batchId}'`
    );

    if (batchRows.length === 0) {
      throw createHttpError(404, 'Batch not found');
    }

    const telemetry = await queryWithShadowCopy(
      resolveDynamicMdbPath(cdmc),
      `SELECT baty, TIM, VOLT, Im FROM vidata WHERE baty = ${channelNumber} ORDER BY TIM ASC`
    );

    const batch = batchRows[0];
    const stats = computeStats(telemetry);

    const context = {
      BATCH_ID: batch.id,
      MODEL: batch.dcxh,
      DATE: batch.fdrq,
      DISCHARGE_PATTERN: batch.fdfs,
      CHANNEL: channelNumber,
      ...stats,
      HISTORY_DATA: telemetry.map((row) => ({
        TIM: toFiniteNumber(row.TIM),
        VOLT: toFiniteNumber(row.VOLT),
        Im: toFiniteNumber(row.Im),
        BATY: toFiniteNumber(row.baty),
      })),
    };

    const templatePath = resolveWithinBase(getTemplatesDir(), templateName);
    const reportBuffer = await renderExcelTemplate(templatePath, context);

    const safeBatch = String(batchId).replace(/[^a-zA-Z0-9._-]/g, '_');
    const safeChannel = String(channelNumber).replace(/[^a-zA-Z0-9._-]/g, '_');
    const fileName = `dmp_report_${safeBatch}_${safeChannel}.xlsx`;

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.send(Buffer.from(reportBuffer));
  } catch (error) {
    logger.error('DMP report generation failed', { error: error.message });
    next(error);
  }
}

module.exports = {
  getBatches,
  getChannels,
  getTelemetry,
  getStats,
  getTemplates,
  generateReport,
};
