const API_BASE = '/api/dmp';

function getAuthToken() {
  return localStorage.getItem('token') || localStorage.getItem('accessToken') || '';
}

async function apiFetch(endpoint, options = {}) {
  const token = getAuthToken();
  const headers = {
    ...(options.headers || {}),
    Authorization: `Bearer ${token}`,
  };

  if (!(options.body instanceof FormData) && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const body = await response.json();
      message = body.message || body.error || message;
    } catch {
      // ignore parse failure
    }
    throw new Error(message);
  }

  return response;
}

export async function fetchBatches() {
  const response = await apiFetch('/batches', { method: 'GET' });
  const data = await response.json();
  return data.batches || [];
}

export async function fetchChannels(batchId) {
  const response = await apiFetch(`/batches/${encodeURIComponent(batchId)}/channels`, { method: 'GET' });
  const data = await response.json();
  return data.channels || [];
}

export async function fetchTelemetry(cdmc, channel) {
  const params = new URLSearchParams({ cdmc: String(cdmc), channel: String(channel) });
  const response = await apiFetch(`/telemetry?${params.toString()}`, { method: 'GET' });
  const data = await response.json();
  return data.telemetry || [];
}

export async function fetchStats(cdmc, channel) {
  const params = new URLSearchParams({ cdmc: String(cdmc), channel: String(channel) });
  const response = await apiFetch(`/stats?${params.toString()}`, { method: 'GET' });
  return response.json();
}

export async function fetchTemplates() {
  const response = await apiFetch('/templates', { method: 'GET' });
  const data = await response.json();
  return data.templates || [];
}

function triggerBrowserDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

export async function downloadReport({ batchId, cdmc, channel, templateName }) {
  const response = await apiFetch('/report', {
    method: 'POST',
    body: JSON.stringify({ batchId, cdmc, channel, templateName }),
  });

  const blob = await response.blob();
  const disposition = response.headers.get('content-disposition') || '';
  const match = disposition.match(/filename="?([^\";]+)"?/i);
  const filename = match?.[1] || 'dmp_report.xlsx';
  triggerBrowserDownload(blob, filename);
}
