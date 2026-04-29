import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  fetchDischargePresets,
  fetchFamilyKeywords,
} from '../api/dischargeConditionsApi';
import {
  FALLBACK_DISCHARGE_PRESETS,
  FALLBACK_FAMILY_KEYWORDS,
  groupPresets,
  detectBatteryFamily as detectBatteryFamilyFromList,
} from '../constants/dischargeConditions';

/**
 * Hook that loads discharge condition presets and family-detection
 * keywords from the backend, falling back to the bundled hard-coded
 * defaults whenever the request is in flight or fails. Exposes a
 * `reload()` action so admin edit dialogs can refresh the cache.
 *
 * Returned shape:
 *   {
 *     presetsGrouped:  Array<{family, label, conditions: [{id?, text, suffix, ...}]}>,
 *     presetsRaw:      Array<{id, family, condition_text, suffix, sort_order}>,
 *     keywords:        Array<{id?, keyword, family, sort_order?}>,
 *     loading: boolean,
 *     error:   string | null,
 *     fromServer: boolean,        // true once the API call succeeded at least once
 *     reload:  () => Promise<void>,
 *     detectFamily: (text: string) => string | null,
 *   }
 */
export default function useDischargeConditions() {
  const [presetsRaw, setPresetsRaw] = useState([]);
  const [keywords, setKeywords] = useState([]);
  const [loading, setLoading] = useState(true);
  const [fromServer, setFromServer] = useState(false);
  const [error, setError] = useState(null);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [p, k] = await Promise.all([
        fetchDischargePresets(),
        fetchFamilyKeywords(),
      ]);
      setPresetsRaw(Array.isArray(p) ? p : []);
      setKeywords(Array.isArray(k) ? k : []);
      setFromServer(true);
    } catch (err) {
      setError(err?.message || 'Failed to load discharge conditions');
      // Keep using whatever we already had; caller will see fromServer=false
      // and will fall back to the hard-coded list.
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { reload(); }, [reload]);

  const presetsGrouped = useMemo(() => {
    if (fromServer && presetsRaw.length > 0) return groupPresets(presetsRaw);
    if (fromServer && presetsRaw.length === 0) return []; // admin emptied the list intentionally
    return FALLBACK_DISCHARGE_PRESETS;
  }, [fromServer, presetsRaw]);

  const effectiveKeywords = useMemo(() => {
    if (fromServer && keywords.length > 0) return keywords;
    if (fromServer && keywords.length === 0) return [];
    return FALLBACK_FAMILY_KEYWORDS;
  }, [fromServer, keywords]);

  const detectFamily = useCallback(
    (text) => detectBatteryFamilyFromList(text, effectiveKeywords),
    [effectiveKeywords],
  );

  return {
    presetsRaw,
    presetsGrouped,
    keywords: effectiveKeywords,
    loading,
    error,
    fromServer,
    reload,
    detectFamily,
  };
}
