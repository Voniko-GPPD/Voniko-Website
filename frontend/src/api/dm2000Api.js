// Shared API helpers for the historic DM2000 / DM3000 modules.
//
// Both modules expose the same REST endpoints (under different prefixes
// `/api/dmp/dm2000` vs `/api/dmp/dm3000`).  All fetcher helpers are
// therefore generated from a single factory so the modules stay in lock-
// step.  The original `fetchDM2000*` / `downloadDM2000*` exports continue
// to work unchanged; new `fetchDM3000*` / `downloadDM3000*` exports are
// also provided.

async function apiFetch(url, options = {}, _retried = false) {
  const token = localStorage.getItem('accessToken');
  const res = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...options,
  });

  // On 401, attempt a one-time token refresh then retry the original request.
  // This mirrors the axios interceptor in api/index.js so that saves don't
  // fail when the access token has expired.
  if (res.status === 401 && !_retried) {
    const refreshToken = localStorage.getItem('refreshToken');
    if (refreshToken) {
      try {
        const refreshRes = await fetch('/api/auth/refresh', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refreshToken }),
        });
        if (refreshRes.ok) {
          const data = await refreshRes.json();
          localStorage.setItem('accessToken', data.accessToken);
          if (data.refreshToken) localStorage.setItem('refreshToken', data.refreshToken);
          return apiFetch(url, options, true);
        }
      } catch (_refreshError) {
        // refresh failed — fall through to clear tokens and redirect
      }
    }
    localStorage.removeItem('accessToken');
    localStorage.removeItem('refreshToken');
    window.location.href = '/login';
    throw new Error('Session expired');
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message || err.error || err.detail || 'Request failed');
  }
  return res;
}

/**
 * Build a complete API surface for one historic DM module.
 * @param {string} MODULE - 'dm2000' or 'dm3000'
 */
function createDmHistoricApi(MODULE) {
  const BASE = `/api/dmp/${MODULE}`;
  const reportFilenamePrefix = `${MODULE}_report`;
  const previewFilenamePrefix = `${MODULE}_preview`;

  async function fetchConfig(stationId, { signal } = {}) {
    const res = await apiFetch(`${BASE}/config?stationId=${encodeURIComponent(stationId)}`, { signal });
    return res.json();
  }

  async function refreshArchives(stationId) {
    const res = await apiFetch(`${BASE}/refresh-archives`, {
      method: 'POST',
      body: JSON.stringify({ stationId }),
    });
    return res.json();
  }

  async function fetchArchives(stationId, filters = {}, { signal } = {}) {
    const params = new URLSearchParams({ stationId });
    if (filters.date_from) params.set('date_from', filters.date_from);
    if (filters.date_to) params.set('date_to', filters.date_to);
    if (filters.type_filter) params.set('type_filter', filters.type_filter);
    if (filters.name_filter) params.set('name_filter', filters.name_filter);
    if (filters.mfr_filter) params.set('mfr_filter', filters.mfr_filter);
    if (filters.serial_filter) params.set('serial_filter', filters.serial_filter);
    if (filters.dis_condition_filter) params.set('dis_condition_filter', filters.dis_condition_filter);
    if (filters.keyword) params.set('keyword', filters.keyword);
    const res = await apiFetch(`${BASE}/archives?${params.toString()}`, { signal });
    const data = await res.json();
    return { archives: data.archives || [], total: data.total || 0 };
  }

  async function fetchDisConditionOptions(stationId, { signal } = {}) {
    const params = new URLSearchParams({ stationId });
    const res = await apiFetch(`${BASE}/dis-condition-options?${params.toString()}`, { signal });
    const data = await res.json();
    return data.options || [];
  }

  async function fetchBatteries(stationId, archname, { signal } = {}) {
    const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/batteries?stationId=${encodeURIComponent(stationId)}`, { signal });
    const data = await res.json();
    return data.batteries || [];
  }

  async function fetchCurve(stationId, archname, baty, { signal } = {}) {
    const params = new URLSearchParams({ stationId, baty });
    const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/curve?${params.toString()}`, { signal });
    const data = await res.json();
    return data.curve || [];
  }

  async function fetchAverageCurve(stationId, archname, { signal } = {}) {
    const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/average-curve?stationId=${encodeURIComponent(stationId)}`, { signal });
    const data = await res.json();
    return data.curve || [];
  }

  async function fetchStats(stationId, archname, baty, { signal } = {}) {
    const params = new URLSearchParams({ stationId, baty });
    const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/stats?${params.toString()}`, { signal });
    return res.json();
  }

  async function fetchDailyVoltage(stationId, archname, baty, { signal } = {}) {
    const params = new URLSearchParams({ stationId, baty });
    const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/daily-voltage?${params.toString()}`, { signal });
    const data = await res.json();
    return data.daily_voltage || [];
  }

  async function fetchTimeAtVoltage(stationId, archname, baty, { signal } = {}) {
    const params = new URLSearchParams({ stationId, baty });
    const res = await apiFetch(`${BASE}/archives/${encodeURIComponent(archname)}/time-at-voltage?${params.toString()}`, { signal });
    const data = await res.json();
    return data.time_at_voltage || [];
  }

  async function fetchTemplates(stationId, { signal } = {}) {
    const res = await apiFetch(`${BASE}/templates?stationId=${encodeURIComponent(stationId)}`, { signal });
    const data = await res.json();
    return data.templates || [];
  }

  async function downloadReport({
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
    const filename = nameMatch ? nameMatch[1] : `${reportFilenamePrefix}_${archname}.xlsx`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function downloadSimpleReport({
    stationId,
    archname,
    batys,
    overrideBatteryType,
    overrideManufacturer,
    endpointCutoff,
  }) {
    const token = localStorage.getItem('accessToken');
    const body = { stationId, archname, batys: batys || [] };
    if (overrideBatteryType != null && overrideBatteryType !== '') body.override_battery_type = overrideBatteryType;
    if (overrideManufacturer != null && overrideManufacturer !== '') body.override_manufacturer = overrideManufacturer;
    if (endpointCutoff != null) body.endpoint_cutoff = endpointCutoff;

    const res = await fetch(`${BASE}/report-simple`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ message: res.statusText }));
      throw new Error(err.message || err.error || err.detail || 'Report generation failed');
    }
    const blob = await res.blob();
    const disposition = res.headers.get('Content-Disposition') || '';
    const nameMatch = disposition.match(/filename="([^"]+)"/);
    const filename = nameMatch ? nameMatch[1] : `${previewFilenamePrefix}_${archname}.xlsx`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function downloadPerfReport({ stationId, entries, templateName }) {
    const token = localStorage.getItem('accessToken');
    const body = { stationId, entries: entries || [] };
    if (templateName) body.template_name = templateName;
    const res = await fetch(`${BASE}/perf-report`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ message: res.statusText }));
      throw new Error(err.message || err.error || err.detail || 'Report generation failed');
    }
    const blob = await res.blob();
    const disposition = res.headers.get('Content-Disposition') || '';
    const nameMatch = disposition.match(/filename="([^"]+)"/);
    const filename = nameMatch ? nameMatch[1] : 'perf_report.xlsx';
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function fetchPerfTemplates(stationId, { signal } = {}) {
    const res = await apiFetch(`${BASE}/perf-templates?stationId=${encodeURIComponent(stationId)}`, { signal });
    const data = await res.json();
    return data.templates || [];
  }

  async function uploadPerfTemplate(stationId, file) {
    const token = localStorage.getItem('accessToken');
    const formData = new FormData();
    formData.append('file', file);
    const res = await fetch(`${BASE}/perf-template/upload?stationId=${encodeURIComponent(stationId)}`, {
      method: 'POST',
      headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      body: formData,
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ message: res.statusText }));
      throw new Error(err.message || err.error || err.detail || 'Upload failed');
    }
    return res.json();
  }

  async function fetchOptions(field) {
    const params = field ? `?field=${encodeURIComponent(field)}` : '';
    const res = await apiFetch(`${BASE}/options${params}`);
    const data = await res.json();
    return data.options || [];
  }

  async function addOption(field, value) {
    const res = await apiFetch(`${BASE}/options`, {
      method: 'POST',
      body: JSON.stringify({ field, value }),
    });
    return res.json();
  }

  async function deleteOption(id) {
    const res = await apiFetch(`${BASE}/options/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
    return res.json();
  }

  async function fetchArchiveOverride(stationId, archname) {
    const params = new URLSearchParams({ stationId, archname });
    const res = await apiFetch(`${BASE}/archive-overrides?${params.toString()}`);
    const data = await res.json();
    return data.override || null;
  }

  async function saveArchiveOverride(stationId, archname, { serialno, remarks } = {}) {
    const res = await apiFetch(`${BASE}/archive-overrides`, {
      method: 'PUT',
      body: JSON.stringify({ stationId, archname, serialno: serialno ?? null, remarks: remarks ?? null }),
    });
    return res.json();
  }

  return {
    module: MODULE,
    fetchConfig,
    refreshArchives,
    fetchArchives,
    fetchDisConditionOptions,
    fetchBatteries,
    fetchCurve,
    fetchAverageCurve,
    fetchStats,
    fetchDailyVoltage,
    fetchTimeAtVoltage,
    fetchTemplates,
    downloadReport,
    downloadSimpleReport,
    downloadPerfReport,
    fetchPerfTemplates,
    uploadPerfTemplate,
    fetchOptions,
    addOption,
    deleteOption,
    fetchArchiveOverride,
    saveArchiveOverride,
  };
}

// Pre-built API surfaces for the two historic modules.
export const dm2000Api = createDmHistoricApi('dm2000');
export const dm3000Api = createDmHistoricApi('dm3000');

// Pick the right API surface for a given module key.  Components that
// support both DM2000 and DM3000 should call this to obtain the matching
// API helpers from a `module` prop.
export function getDmHistoricApi(module) {
  return module === 'dm3000' ? dm3000Api : dm2000Api;
}

// ── Backward-compatible named exports ─────────────────────────────────
// Existing call sites import these symbols by name; we keep them so the
// refactor does not require touching every import.
export const fetchDM2000Config = dm2000Api.fetchConfig;
export const refreshDM2000Archives = dm2000Api.refreshArchives;
export const fetchDM2000Archives = dm2000Api.fetchArchives;
export const fetchDM2000DisConditionOptions = dm2000Api.fetchDisConditionOptions;
export const fetchDM2000Batteries = dm2000Api.fetchBatteries;
export const fetchDM2000Curve = dm2000Api.fetchCurve;
export const fetchDM2000AverageCurve = dm2000Api.fetchAverageCurve;
export const fetchDM2000Stats = dm2000Api.fetchStats;
export const fetchDM2000DailyVoltage = dm2000Api.fetchDailyVoltage;
export const fetchDM2000TimeAtVoltage = dm2000Api.fetchTimeAtVoltage;
export const fetchDM2000Templates = dm2000Api.fetchTemplates;
export const downloadDM2000Report = dm2000Api.downloadReport;
export const downloadDM2000SimpleReport = dm2000Api.downloadSimpleReport;
export const downloadDM2000PerfReport = dm2000Api.downloadPerfReport;
export const fetchDM2000PerfTemplates = dm2000Api.fetchPerfTemplates;
export const uploadDM2000PerfTemplate = dm2000Api.uploadPerfTemplate;
export const fetchDM2000Options = dm2000Api.fetchOptions;
export const addDM2000Option = dm2000Api.addOption;
export const deleteDM2000Option = dm2000Api.deleteOption;
export const fetchDM2000ArchiveOverride = dm2000Api.fetchArchiveOverride;
export const saveDM2000ArchiveOverride = dm2000Api.saveArchiveOverride;
