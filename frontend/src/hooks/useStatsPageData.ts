import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChartData, StatsTabId } from '../components/stats/StatsPrimitives.js';
import { getCurrentSite } from '../config/site.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { chartStatsEndpoint, fetchJson, withScopeParams } from '../utils/api.js';
import { isLiveObserverSnapshot, type LiveObserverSnapshot } from '../pages/statsState.js';
import { parseStatsSection } from '../pages/statsSections.js';
import { useVisibilityPoll } from './useVisibilityPoll.js';

const STATS_REFRESH_MS = 5 * 60 * 1_000;
const STATS_TIMEOUT_MS = 15_000;
const STATS_MAX_BYTES = 8 * 1024 * 1024;

export interface StatsPageData {
  data: ChartData | null;
  loading: boolean;
  refreshing: boolean;
  loadError: string | null;
  lastUpdatedAt: number | null;
  reload: () => Promise<void>;
  liveObservers: LiveObserverSnapshot | null;
  observerError: string | null;
}

export function useStatsPageData(section: StatsTabId): StatsPageData {
  const [result, setResult] = useState<{ key: string; data: ChartData; generatedAt: number } | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [liveObservers, setLiveObservers] = useState<LiveObserverSnapshot | null>(null);
  const [observerError, setObserverError] = useState<string | null>(null);
  const dataRef = useRef<ChartData | null>(null);
  const requestControllerRef = useRef<AbortController | null>(null);
  const requestSequenceRef = useRef(0);
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const statsScope = useMemo(
    () => ({ network: site.networkFilter, observer: site.observerId }),
    [site.networkFilter, site.observerId],
  );
  const key = `stats:${statsScope.network ?? 'ukmesh'}:${statsScope.observer ?? 'all'}:${privacyGeneration}:${section}`;
  const data = result?.key === key ? result.data : null;

  const load = useCallback(async (externalSignal?: AbortSignal) => {
    requestControllerRef.current?.abort();
    const controller = new AbortController();
    requestControllerRef.current = controller;
    const signal = externalSignal
      ? AbortSignal.any([controller.signal, externalSignal])
      : controller.signal;
    const sequence = requestSequenceRef.current + 1;
    requestSequenceRef.current = sequence;
    if (dataRef.current) setRefreshing(true);
    else setLoading(true);
    setLoadError(null);

    try {
      const payload = await fetchJson<unknown>(
        `${chartStatsEndpoint(statsScope)}${chartStatsEndpoint(statsScope).includes('?') ? '&' : '?'}sections=${section}`,
        { cache: 'no-cache', signal },
        { timeoutMs: STATS_TIMEOUT_MS, maxBytes: STATS_MAX_BYTES },
      );
      const next = parseStatsSection(payload, section, { network: statsScope.network ?? 'ukmesh',
        observer: statsScope.observer ?? null, generation: privacyGeneration });
      if (!next) throw new Error('Stats response was malformed or no longer matches this scope');
      if (signal.aborted || sequence !== requestSequenceRef.current) return;
      dataRef.current = next.data;
      setResult({ key, ...next });
    } catch (error) {
      if (!signal.aborted && sequence === requestSequenceRef.current) {
        setLoadError(error instanceof Error ? error.message : 'Stats could not be loaded');
      }
    } finally {
      if (sequence === requestSequenceRef.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [statsScope, section, privacyGeneration, key]);

  useEffect(() => {
    requestControllerRef.current?.abort();
    dataRef.current = null;
    setResult(null);
    setLiveObservers(null);
    setObserverError(null);
    setLoadError(null);
    setLoading(true);
    return () => {
      requestControllerRef.current?.abort();
      requestSequenceRef.current++;
    };
  }, [load, privacyGeneration]);

  useVisibilityPoll(
    load,
    {
      enabled: privacyGeneration > 0,
      scopeKey: key,
      intervalMs: STATS_REFRESH_MS,
      timeoutMs: STATS_TIMEOUT_MS,
    },
  );

  const refreshObservers = useVisibilityPoll(async (signal) => {
    const payload = await fetchJson<LiveObserverSnapshot>(
      withScopeParams('/api/stats/observers/live', statsScope), { cache: 'no-store', signal },
      { timeoutMs: STATS_TIMEOUT_MS, maxBytes: 128 * 1024, validate: isLiveObserverSnapshot },
    );
    if (!signal.aborted) { setLiveObservers(payload); setObserverError(null); }
  }, {
    enabled: section === 'observers' && privacyGeneration > 0,
    scopeKey: `live-observers:${statsScope.network ?? 'all'}:${statsScope.observer ?? 'all'}:${privacyGeneration}`,
    intervalMs: 60_000, timeoutMs: STATS_TIMEOUT_MS,
    onError: () => setObserverError('Live observer updates are unavailable. Showing the last received data.'),
  });

  return {
    data,
    loading: loading || (!data && !loadError),
    refreshing,
    loadError,
    lastUpdatedAt: data ? result!.generatedAt : null,
    reload: async () => { refreshObservers(); await load(); },
    liveObservers,
    observerError,
  };
}
