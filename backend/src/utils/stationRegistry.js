/**
 * stationRegistry.js
 *
 * In-memory registry of stations (DMP and battery-test) that self-register via
 *   POST /api/dmp/register   (type: 'dmp')
 *   POST /api/battery/register (type: 'battery')
 *
 * Persisted to station_registry.json (backend root) so data survives
 * Node.js restarts. Both battery.js route and batterySocket.js import
 * this module to share the same registry instance.
 *
 * Each station carries a `type` field ('dmp' or 'battery') so that
 * /api/dmp/stations and /api/battery/stations only return their own stations.
 */

const fs = require('fs');
const path = require('path');
const logger = require('./logger');

const REGISTRY_FILE = path.join(__dirname, '../../station_registry.json');
const OFFLINE_TIMEOUT_MS = 90_000; // 90 seconds without heartbeat → offline

/** @type {Map<string, { id: string, name: string, url: string, type: string, lastSeen: number }>} */
const registry = new Map();

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function _slugify(name) {
  return (name || '')
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .slice(0, 64);
}

function _loadFromDisk() {
  try {
    const raw = fs.readFileSync(REGISTRY_FILE, 'utf8');
    const list = JSON.parse(raw);
    if (Array.isArray(list)) {
      for (const s of list) {
        if (s.id && s.name && s.url) {
          // Backwards compat: entries without a type default to 'dmp'
          if (!s.type) s.type = 'dmp';
          registry.set(s.id, s);
        }
      }
    }
    logger.info('Station registry loaded from disk', { count: registry.size });
  } catch (_) {
    // File may not exist yet — that's fine
  }
}

function _saveToDisk() {
  try {
    fs.writeFileSync(
      REGISTRY_FILE,
      JSON.stringify([...registry.values()], null, 2),
      'utf8'
    );
  } catch (e) {
    logger.warn('Failed to persist station registry', { error: e.message });
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Register or update a station.
 * @param {string} name  Human-readable station name (e.g. "Tram 1 - Day chuyen A")
 * @param {string} url   Base URL of the Python service (e.g. "http://10.4.2.5:8765")
 * @param {string} [type]  Station type: 'dmp' or 'battery' (default: 'dmp' for backwards compat)
 * @returns {string} The station id (slug derived from name + type prefix to avoid collisions)
 */
function upsertStation(name, url, type = 'dmp') {
  const id = `${type}-${_slugify(name) || Date.now()}`;
  registry.set(id, { id, name, url, type, lastSeen: Date.now() });
  _saveToDisk();
  logger.info('Station registered / heartbeat', { id, name, url, type });
  return id;
}

/**
 * Return stations filtered by type, each with an `online` flag.
 * @param {string} [type]  If provided, only return stations of this type ('dmp' or 'battery').
 * @returns {Array<{ id, name, url, type, lastSeen, online }>}
 */
function getStations(type) {
  const now = Date.now();
  return [...registry.values()]
    .filter((s) => !type || s.type === type)
    .map((s) => ({
      id: s.id,
      name: s.name,
      url: s.url,
      type: s.type,
      lastSeen: s.lastSeen,
      online: now - s.lastSeen < OFFLINE_TIMEOUT_MS,
    }));
}

/**
 * Resolve the base URL for a given stationId.
 * Returns null if stationId is unknown or the station is offline.
 * @param {string|null|undefined} stationId
 * @returns {string|null}
 */
function resolveUrl(stationId) {
  if (!stationId) return null;
  const s = registry.get(stationId);
  if (!s) return null;
  if (Date.now() - s.lastSeen >= OFFLINE_TIMEOUT_MS) return null;
  return s.url;
}

// Load persisted data on module initialisation
_loadFromDisk();

module.exports = { upsertStation, getStations, resolveUrl };
