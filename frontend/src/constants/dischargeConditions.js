/**
 * Discharge condition presets and helpers for battery test reports.
 *
 * The hard-coded presets in this file are only used as a *fallback* when the
 * backend service that stores the editable list is unreachable. The runtime
 * source of truth lives in the SQLite database and is exposed via
 *   GET /api/dmp/discharge-presets
 *   GET /api/dmp/family-keywords
 * (see `src/api/dischargeConditionsApi.js` and `useDischargeConditions()`).
 *
 * Suffix convention (last character in parentheses on each line):
 *   (h) — result is reported in hours
 *   (m) — result is reported in minutes
 *   (t) — result is reported in number of times / pulses
 *   ""  — no explicit suffix; report unit is implied by the cycle string
 */

/** Hard-coded fallback presets (mirrors the seed data on the backend). */
export const FALLBACK_DISCHARGE_PRESETS = [
  {
    family: 'LR6',
    label: 'LR6 (AA)',
    conditions: [
      { text: '10ohm 24h/d-0.9V', suffix: 'h' },
      { text: '1000mA 24h/d-0.9V', suffix: 'm' },
      { text: '(1500mW2s,650mW28s) 10T/h,24h/d-1.05V', suffix: 't' },
      { text: '(1500mW2s,650mW28s) 10T/h,24h/d-1.05V 15 DAY', suffix: 't' },
      { text: '3.9ohm 1h/d-0.8V', suffix: 'h' },
      { text: '3.9ohm 4m/h 8h/d-0.9V', suffix: 'm' },
      { text: '250mA 1h/d-0.9V', suffix: 'h' },
      { text: '3.9ohm 24h/d-0.8V', suffix: 'm' },
      { text: '1000mA 10s/m 1h/d-0.9V', suffix: 't' },
      { text: '100mA 1h/d-0.9V', suffix: 'h' },
      { text: '50mA 1h/8h 24h/d-1V', suffix: 'h' },
      { text: '750mA 2m/h 8h/d-1.1V', suffix: 'm' },
      { text: '(450mW5s,45mW175s) 3h/124h-1.1V', suffix: 'h' },
      { text: '(1ohm,0.25s.3.0ohm,19.75s), 10m/h,1h/12h-1.0V', suffix: '' },
    ],
  },
  {
    family: 'LR03',
    label: 'LR03 (AAA)',
    conditions: [
      { text: '20ohm 24h/d-0.9V', suffix: 'h' },
      { text: '600mA 24h/d-0.9V', suffix: 'm' },
      { text: '5.1ohm 1h/d-0.8V', suffix: 'm' },
      { text: '5.1ohm 4m/h 8h/d-0.9V', suffix: 'm' },
      { text: '600mA 10s/m 1h/d-0.9V', suffix: 't' },
      { text: '50mA 1h/12h-0.9V', suffix: 'h' },
      { text: '250mA 5m/h 12h/d-1.1V', suffix: 'm' },
      { text: '100mA 1h/d-0.9V', suffix: 'h' },
      { text: '24ohm 15s/m 8h/d-1V', suffix: 'h' },
      { text: '3.9ohm 24h/d-0.8V', suffix: 'm' },
      { text: '75mA 1h/12h.24/d-0.9V', suffix: 'h' },
    ],
  },
  {
    family: 'LR61',
    label: 'LR61 (AAAA)',
    conditions: [
      { text: '35mA 24h/d-0.9V', suffix: 'h' },
      { text: '5.1ohm 5m/d-0.9V', suffix: 'm' },
      { text: '75ohm 1h/d-0.9V', suffix: 'h' },
      { text: '75ohm 1h/d-1.1V', suffix: 'h' },
    ],
  },
  {
    family: '9V',
    label: '9V (6F22 / 6LR61)',
    conditions: [
      { text: '35mA 24h/d-5.4V', suffix: 'h' },
      { text: '180ohm 4h/d-6.8V', suffix: 'h' },
      { text: '270ohm 1h/d-5.4V', suffix: 'h' },
      { text: '620ohm 2h/d-5.4V', suffix: 'h' },
      { text: '620ohm+10Kohm 1s/60m.24h/d-7.5V', suffix: 'h' },
    ],
  },
];

/** Hard-coded fallback family-detection keywords (used when API is down). */
export const FALLBACK_FAMILY_KEYWORDS = [
  { keyword: 'LR03', family: 'LR03' },
  { keyword: 'LR61', family: 'LR61' },
  { keyword: 'LR6', family: 'LR6' },
  { keyword: '9V', family: '9V' },
  { keyword: '6F22', family: '9V' },
  { keyword: '6LR61', family: '9V' },
];

/** Build a default human-readable label for a family identifier. */
export function defaultFamilyLabel(family) {
  switch (String(family || '').toUpperCase()) {
    case 'LR6': return 'LR6 (AA)';
    case 'LR03': return 'LR03 (AAA)';
    case 'LR61': return 'LR61 (AAAA)';
    case '9V': return '9V (6F22 / 6LR61)';
    default: return String(family || '');
  }
}

/** Suffix → unit description (i18n keys live in the locale file). */
export const SUFFIX_INFO = {
  h: { unitKey: 'remarkSuffixHours', short: 'h' },
  m: { unitKey: 'remarkSuffixMinutes', short: 'm' },
  t: { unitKey: 'remarkSuffixTimes', short: 't' },
};

/** Format an entry's display string, e.g. "10ohm 24h/d-0.9V (h)". */
export function formatPresetEntry(entry) {
  if (!entry) return '';
  const base = String(entry.text || '').trim();
  if (!base) return '';
  return entry.suffix ? `${base} (${entry.suffix})` : base;
}

/**
 * Group flat preset rows ([{family, condition_text, suffix, ...}]) returned
 * from the backend into the same {family, label, conditions[]} shape used
 * by the FALLBACK_DISCHARGE_PRESETS array. Preserves the row order, which
 * the API returns sorted by sort_order.
 */
export function groupPresets(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const out = [];
  const idx = new Map();
  rows.forEach((row) => {
    const family = row.family || '';
    if (!family) return;
    if (!idx.has(family)) {
      idx.set(family, out.length);
      out.push({ family, label: defaultFamilyLabel(family), conditions: [] });
    }
    out[idx.get(family)].conditions.push({
      id: row.id,
      text: row.condition_text ?? row.text ?? '',
      suffix: (row.suffix || '').toLowerCase(),
      sortOrder: row.sort_order ?? row.sortOrder ?? 0,
    });
  });
  return out;
}

/**
 * Heuristically detect the battery family from a free-form battery type
 * label such as "LR6 ALKALINE" or "9V/6F22". Iterates through the supplied
 * keyword list (lowest sort_order first) and returns the first match's
 * family identifier (or null when nothing matches).
 *
 * @param {string} batteryType   The battery type text (e.g. dcxh).
 * @param {Array}  keywords      Optional override; defaults to fallback list.
 */
export function detectBatteryFamily(batteryType, keywords) {
  if (!batteryType) return null;
  const upper = String(batteryType).toUpperCase();
  const list = Array.isArray(keywords) && keywords.length > 0
    ? keywords
    : FALLBACK_FAMILY_KEYWORDS;
  for (const kw of list) {
    const k = String(kw.keyword || '').toUpperCase();
    if (k && upper.includes(k)) {
      return kw.family || null;
    }
  }
  return null;
}

/** Append a unit to a value string when missing. */
function withUnit(value, unit) {
  const s = String(value ?? '').trim();
  if (!s) return '';
  if (s.toLowerCase().endsWith(unit.toLowerCase())) return s;
  return `${s}${unit}`;
}

/**
 * Compose a "Discharge Condition" string from underlying data fields. See
 * the inline comment in the previous version for the full description.
 */
export function composeDischargeCondition({ load, cycle, endpoint, suffix } = {}) {
  const cycleStr = String(cycle ?? '').trim();
  const loadStr = String(load ?? '').trim();
  const epStr = String(endpoint ?? '').trim();

  const cycleLooksComplete = cycleStr
    && /(ohm|mA|mW|kohm)/i.test(cycleStr)
    && /-?\s*\d+(\.\d+)?\s*V/i.test(cycleStr);

  let body;
  if (cycleLooksComplete) {
    body = cycleStr;
  } else {
    const loadPart = loadStr ? loadStr.replace(/\s+/g, '') : '';
    const loadFmt = loadPart && /^\d+(\.\d+)?$/.test(loadPart)
      ? `${loadPart}ohm`
      : loadPart;
    const epFmt = epStr ? withUnit(epStr.replace(/\s+/g, ''), 'V') : '';
    const segments = [loadFmt, cycleStr].filter(Boolean).join(' ').trim();
    body = epFmt ? `${segments}${segments ? '-' : ''}${epFmt}` : segments;
  }

  body = body.trim();
  if (!body) return '';
  if (suffix && !/\([hmt]\)\s*$/i.test(body)) {
    return `${body} (${suffix})`;
  }
  return body;
}

/** Best-effort suffix detection from a composed string. */
export function extractSuffix(text) {
  if (!text) return null;
  const m = String(text).trim().match(/\(([hmt])\)\s*$/i);
  return m ? m[1].toLowerCase() : null;
}

/**
 * Backwards-compat alias kept for any consumer that imported the old
 * constant name. New code should use FALLBACK_DISCHARGE_PRESETS or the
 * useDischargeConditions() hook.
 */
export const BATTERY_DISCHARGE_PRESETS = FALLBACK_DISCHARGE_PRESETS;
