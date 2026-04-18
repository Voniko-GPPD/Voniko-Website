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
    throw new Error(err.message || err.error || 'Request failed');
  }
  return res;
}

export async function fetchStations() {
  const res = await apiFetch(`${BASE}/stations`);
  const data = await res.json();
  return data.stations || [];
}

export async function fetchBatches(stationId) {
  const res = await apiFetch(`${BASE}/batches?stationId=${encodeURIComponent(stationId)}`);
  const data = await res.json();
  return data.batches || [];
}

export async function fetchChannels(stationId, batchId) {
  const res = await apiFetch(`${BASE}/batches/${batchId}/channels?stationId=${encodeURIComponent(stationId)}`);
  const data = await res.json();
  return data.channels || [];
}

export async function fetchTelemetry(stationId, cdmc, channel) {
  const params = new URLSearchParams({ stationId, cdmc, channel });
  const res = await apiFetch(`${BASE}/telemetry?${params.toString()}`);
  const data = await res.json();
  return data.telemetry || [];
}

export async function fetchStats(stationId, cdmc, channel) {
  const params = new URLSearchParams({ stationId, cdmc, channel });
  const res = await apiFetch(`${BASE}/stats?${params.toString()}`);
  return res.json();
}

export async function fetchTemplates(stationId) {
  const res = await apiFetch(`${BASE}/templates?stationId=${encodeURIComponent(stationId)}`);
  const data = await res.json();
  return data.templates || [];
}

export async function downloadReport({ stationId, batchId, cdmc, channel, templateName }) {
  const token = localStorage.getItem('accessToken');
  const res = await fetch(`${BASE}/report`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify({ stationId, batchId, cdmc, channel, templateName }),
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
