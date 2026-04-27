const BASE = '/api/dmp';

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

export async function fetchStations() {
  const res = await apiFetch(`${BASE}/stations`);
  const data = await res.json();
  return data.stations || [];
}

export async function fetchBatchYears(stationId) {
  const res = await apiFetch(`${BASE}/batches/years?stationId=${encodeURIComponent(stationId)}`);
  const data = await res.json();
  return data.years || [];
}

export async function fetchBatches(stationId, year) {
  const params = new URLSearchParams({ stationId });
  if (year != null) params.set('year', String(year));
  const res = await apiFetch(`${BASE}/batches?${params.toString()}`);
  const data = await res.json();
  return data.batches || [];
}

export async function fetchChannels(stationId, batchId) {
  const res = await apiFetch(`${BASE}/batches/${encodeURIComponent(batchId)}/channels?stationId=${encodeURIComponent(stationId)}`);
  const data = await res.json();
  return data.channels || [];
}

export async function fetchTelemetry(stationId, cdmc, channel, signal) {
  const params = new URLSearchParams({ stationId, cdmc, channel });
  const res = await apiFetch(`${BASE}/telemetry?${params.toString()}`, { signal });
  const data = await res.json();
  return data.telemetry || [];
}

export async function fetchChanges(stationId, since) {
  const params = new URLSearchParams({ stationId, since: String(since) });
  const res = await apiFetch(`${BASE}/changes?${params.toString()}`);
  const data = await res.json();
  return {
    changes: data.changes || [],
    timestamp: Number(data.timestamp) || Math.floor(Date.now() / 1000),
  };
}

export async function fetchStats(stationId, cdmc, channel, signal) {
  const params = new URLSearchParams({ stationId, cdmc, channel });
  const res = await apiFetch(`${BASE}/stats?${params.toString()}`, { signal });
  return res.json();
}

export async function fetchTemplates(stationId) {
  const res = await apiFetch(`${BASE}/templates?stationId=${encodeURIComponent(stationId)}`);
  const data = await res.json();
  return data.templates || [];
}

export async function downloadSimpleReport({ stationId, batchId, cdmc, channel }) {
  const token = localStorage.getItem('accessToken');
  const res = await fetch(`${BASE}/report-simple`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    // stationId is consumed by the Node.js proxy; Python-bound fields use snake_case to match Pydantic models
    body: JSON.stringify({ stationId, batch_id: batchId, cdmc, channel }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message || err.error || 'Report generation failed');
  }

  const blob = await res.blob();
  const disposition = res.headers.get('Content-Disposition') || '';
  const nameMatch = disposition.match(/filename="([^"]+)"/);
  const filename = nameMatch ? nameMatch[1] : 'DMP_Report.xlsx';
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export async function downloadReport({ stationId, batchId, cdmc, channel, templateName }) {
  const token = localStorage.getItem('accessToken');
  const res = await fetch(`${BASE}/report`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    // stationId is consumed by the Node.js proxy; Python-bound fields use snake_case to match Pydantic models
    body: JSON.stringify({ stationId, batch_id: batchId, cdmc, channel, template_name: templateName }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message || err.error || 'Report generation failed');
  }

  const blob = await res.blob();
  const disposition = res.headers.get('Content-Disposition') || '';
  const nameMatch = disposition.match(/filename="([^"]+)"/);
  const filename = nameMatch ? nameMatch[1] : 'DMP_Report.xlsx';
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
