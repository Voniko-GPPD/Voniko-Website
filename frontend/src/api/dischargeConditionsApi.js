/**
 * Discharge condition presets & battery-family keyword API client.
 *
 * Backend endpoints:
 *   GET    /api/dmp/discharge-presets
 *   POST   /api/dmp/discharge-presets             (admin)
 *   PUT    /api/dmp/discharge-presets/:id         (admin)
 *   DELETE /api/dmp/discharge-presets/:id         (admin)
 *   GET    /api/dmp/family-keywords
 *   POST   /api/dmp/family-keywords               (admin)
 *   PUT    /api/dmp/family-keywords/:id           (admin)
 *   DELETE /api/dmp/family-keywords/:id           (admin)
 */
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
  return res.json();
}

// ─── Presets ────────────────────────────────────────────────────────────────
export async function fetchDischargePresets() {
  const data = await apiFetch(`${BASE}/discharge-presets`);
  return data.presets || [];
}

export async function createDischargePreset({ family, conditionText, suffix = '', sortOrder }) {
  const body = { family, condition_text: conditionText, suffix };
  if (sortOrder != null) body.sort_order = sortOrder;
  const data = await apiFetch(`${BASE}/discharge-presets`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
  return data.preset;
}

export async function updateDischargePreset(id, { family, conditionText, suffix, sortOrder } = {}) {
  const body = {};
  if (family != null) body.family = family;
  if (conditionText != null) body.condition_text = conditionText;
  if (suffix != null) body.suffix = suffix;
  if (sortOrder != null) body.sort_order = sortOrder;
  const data = await apiFetch(`${BASE}/discharge-presets/${encodeURIComponent(id)}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
  return data.preset;
}

export async function deleteDischargePreset(id) {
  return apiFetch(`${BASE}/discharge-presets/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

// ─── Family keywords ────────────────────────────────────────────────────────
export async function fetchFamilyKeywords() {
  const data = await apiFetch(`${BASE}/family-keywords`);
  return data.keywords || [];
}

export async function createFamilyKeyword({ keyword, family, sortOrder }) {
  const body = { keyword, family };
  if (sortOrder != null) body.sort_order = sortOrder;
  const data = await apiFetch(`${BASE}/family-keywords`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
  return data.keyword;
}

export async function updateFamilyKeyword(id, { keyword, family, sortOrder } = {}) {
  const body = {};
  if (keyword != null) body.keyword = keyword;
  if (family != null) body.family = family;
  if (sortOrder != null) body.sort_order = sortOrder;
  const data = await apiFetch(`${BASE}/family-keywords/${encodeURIComponent(id)}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  });
  return data.keyword;
}

export async function deleteFamilyKeyword(id) {
  return apiFetch(`${BASE}/family-keywords/${encodeURIComponent(id)}`, { method: 'DELETE' });
}
