const BASE = '/api/dmp/dm2000';

async function apiFetch(url, options = {}) {
  const token = localStorage.getItem('accessToken');
  const res = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message || err.error || err.detail || 'Request failed');
  }
  return res;
}

export async function fetchDM2000Config(stationId, { signal } = {}) {
  const res = await apiFetch(`${BASE}/config?stationId=${encodeURIComponent(stationId)}`, { signal });
  return res.json();
}

export async function fetchDM2000Archives(stationId, filters = {}, { signal } = {}) {
  const params = new URLSearchParams({ stationId });
  if (filters.date_from) params.set('date_from', filters.date_from);
  if (filters.date_to) params.set('date_to', filters.date_to);
  if (filters.type_filter) params.set('type_filter', filters.type_filter);
  if (filters.name_filter) params.set('name_filter', filters.name_filter);
  if (filters.mfr_filter) params.set('mfr_filter', filters.mfr_filter);
  if (filters.serial_filter) params.set('serial_filter', filters.serial_filter);
  const res = await apiFetch(`${BASE}/archives?${params.toString()}`, { signal });
  const data = await res.json();
  return { archives: data.archives || [], total: data.total || 0 };
}

export async function fetchDM2000Batteries(stationId, archname, { signal } = {}) {
  const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/batteries?stationId=${encodeURIComponent(stationId)}`, { signal });
  const data = await res.json();
  return data.batteries || [];
}

export async function fetchDM2000Curve(stationId, archname, baty, { signal } = {}) {
  const params = new URLSearchParams({ stationId, baty });
  const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/curve?${params.toString()}`, { signal });
  const data = await res.json();
  return data.curve || [];
}

export async function fetchDM2000AverageCurve(stationId, archname, { signal } = {}) {
  const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/average-curve?stationId=${encodeURIComponent(stationId)}`, { signal });
  const data = await res.json();
  return data.curve || [];
}

export async function fetchDM2000Stats(stationId, archname, baty, { signal } = {}) {
  const params = new URLSearchParams({ stationId, baty });
  const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/stats?${params.toString()}`, { signal });
  return res.json();
}

export async function fetchDM2000DailyVoltage(stationId, archname, baty, { signal } = {}) {
  const params = new URLSearchParams({ stationId, baty });
  const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/daily-voltage?${params.toString()}`, { signal });
  const data = await res.json();
  return data.daily_voltage || [];
}

export async function fetchDM2000TimeAtVoltage(stationId, archname, baty, { signal } = {}) {
  const params = new URLSearchParams({ stationId, baty });
  const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/time-at-voltage?${params.toString()}`, { signal });
  const data = await res.json();
  return data.time_at_voltage || [];
}

export async function fetchDM2000Templates(stationId, { signal } = {}) {
  const res = await apiFetch(`${BASE}/templates?stationId=${encodeURIComponent(stationId)}`, { signal });
  const data = await res.json();
  return data.templates || [];
}

export async function downloadDM2000Report({
  stationId,
  archname,
  baty,
  templateName,
  overrideArchname,
  overrideStartDate,
  overrideBatteryType,
  overrideBatchName,
  overrideDischargeCondition,
  overrideManufacturer,
  overrideMadeDate,
  overrideSerialNo,
  overrideRemarks,
}) {
  const token = localStorage.getItem('accessToken');
  const body = {
    stationId,
    archname,
    baty,
    template_name: templateName,
  };
  if (overrideArchname != null) body.override_archname = overrideArchname;
  if (overrideStartDate != null) body.override_start_date = overrideStartDate;
  if (overrideBatteryType != null) body.override_battery_type = overrideBatteryType;
  if (overrideBatchName != null) body.override_batch_name = overrideBatchName;
  if (overrideDischargeCondition != null) body.override_discharge_condition = overrideDischargeCondition;
  if (overrideManufacturer != null) body.override_manufacturer = overrideManufacturer;
  if (overrideMadeDate != null) body.override_made_date = overrideMadeDate;
  if (overrideSerialNo != null) body.override_serial_no = overrideSerialNo;
  if (overrideRemarks != null) body.override_remarks = overrideRemarks;

  const res = await fetch(`${BASE}/report`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message || err.error || 'Report generation failed');
  }
  const blob = await res.blob();
  const disposition = res.headers.get('Content-Disposition') || '';
  const nameMatch = disposition.match(/filename="([^"]+)"/);
  const filename = nameMatch ? nameMatch[1] : `dm2000_report_${archname}.xlsx`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
