import dayjs from 'dayjs';

export const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD';
export const DISPLAY_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ss';

function formatDateTime(value, formatter) {
  if (!value) return '-';
  const parsed = formatter(value);
  return parsed.isValid() ? parsed.format(DISPLAY_DATETIME_FORMAT) : String(value);
}

export function formatServerUtcDateTime(value) {
  return formatDateTime(value, (input) => dayjs.utc(input).local());
}

export function formatLocalDateTime(value) {
  return formatDateTime(value, (input) => dayjs(input));
}
