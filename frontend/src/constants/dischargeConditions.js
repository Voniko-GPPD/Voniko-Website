/**
 * Discharge condition presets and helpers for battery test reports.
 *
 * The conditions below come from the customer's reference list (LR6, LR03,
 * LR61, 9V battery families). Each entry encodes the standardised text used
 * in the "Dis-condition" / "Remarks" cells of an exported report.
 *
 * Suffix convention (last character in parentheses on each line):
 *   (h) — result is reported in hours
 *   (m) — result is reported in minutes
 *   (t) — result is reported in number of times / pulses
 *   ""  — no explicit suffix; report unit is implied by the cycle string
 */

/**
 * Battery family presets. The `family` is matched (case-insensitive,
 * substring) against the battery type (`dcxh`) to suggest a default group.
 */
export const BATTERY_DISCHARGE_PRESETS = [
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

/** Suffix → human readable unit description (i18n keys are in the locale file). */
export const SUFFIX_INFO = {
  h: { unitKey: 'remarkSuffixHours', short: 'h' },
  m: { unitKey: 'remarkSuffixMinutes', short: 'm' },
  t: { unitKey: 'remarkSuffixTimes', short: 't' },
};

/**
 * Format an entry's display string, e.g. "10ohm 24h/d-0.9V (h)".
 */
export function formatPresetEntry(entry) {
  if (!entry) return '';
  const base = String(entry.text || '').trim();
  if (!base) return '';
  return entry.suffix ? `${base} (${entry.suffix})` : base;
}

/**
 * Heuristically detect the battery family from a free-form battery type
 * label such as "LR6 ALKALINE" or "9V/6F22". Returns the matching preset
 * group (or null when nothing matches).
 */
export function detectBatteryFamily(batteryType) {
  if (!batteryType) return null;
  const upper = String(batteryType).toUpperCase();
  // Check the most specific family first (LR03/LR61 before LR6).
  const order = ['LR03', 'LR61', 'LR6', '9V', '6F22', '6LR61'];
  for (const key of order) {
    if (upper.includes(key)) {
      const familyKey = key === '6F22' || key === '6LR61' ? '9V' : key;
      return BATTERY_DISCHARGE_PRESETS.find((p) => p.family === familyKey) || null;
    }
  }
  return null;
}

/**
 * Append a unit to a value string when the value is non-empty and does not
 * already end with that unit.
 */
function withUnit(value, unit) {
  const s = String(value ?? '').trim();
  if (!s) return '';
  if (s.toLowerCase().endsWith(unit.toLowerCase())) return s;
  return `${s}${unit}`;
}

/**
 * Compose a "Discharge Condition" string from the underlying data fields.
 *
 * For DMP, `cycle` (=`jstj`) almost always already contains the full text
 * (e.g. "24h/d") so this function will return that as-is when it already
 * looks complete. For DM2000, the `load` (Load Resistance), `cycle`
 * (Dis-condition / fdfs) and `endpoint` (End-point Voltage) typically need
 * to be glued together.
 *
 * @param {object} parts
 * @param {string} parts.load     Load resistance, e.g. "10ohm" or "1000mA".
 * @param {string} parts.cycle    Cycle / dis-condition text, e.g. "24h/d".
 * @param {string} parts.endpoint Endpoint voltage, e.g. "0.9V".
 * @param {string} [parts.suffix] Suffix character (h/m/t).
 * @returns {string} Composed string, e.g. "10ohm 24h/d-0.9V (h)".
 */
export function composeDischargeCondition({ load, cycle, endpoint, suffix } = {}) {
  const cycleStr = String(cycle ?? '').trim();
  const loadStr = String(load ?? '').trim();
  const epStr = String(endpoint ?? '').trim();

  // If the cycle string already encodes the full condition (contains a
  // load unit AND an endpoint voltage), don't double-prepend.
  const cycleLooksComplete = cycleStr
    && /(ohm|mA|mW|kohm)/i.test(cycleStr)
    && /-?\s*\d+(\.\d+)?\s*V/i.test(cycleStr);

  let body;
  if (cycleLooksComplete) {
    body = cycleStr;
  } else {
    const loadPart = loadStr ? loadStr.replace(/\s+/g, '') : '';
    // Add 'ohm' suffix when load is purely numeric.
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

/**
 * Best-effort suffix detection from a composed discharge condition string.
 * Returns one of "h", "m", "t" or null when no trailing suffix tag is
 * present.
 */
export function extractSuffix(text) {
  if (!text) return null;
  const m = String(text).trim().match(/\(([hmt])\)\s*$/i);
  return m ? m[1].toLowerCase() : null;
}
