import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChartData } from '../components/stats/StatsPrimitives.js';
import { getCurrentSite } from '../config/site.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { chartStatsEndpoint, fetchJson, uncachedEndpoint } from '../utils/api.js';
import { isStatsPayload } from '../pages/statsState.js';
import { useVisibilityPoll } from './useVisibilityPoll.js';

const STATS_REFRESH_MS = 30 * 60 * 1_000;
const STATS_TIMEOUT_MS = 15_000;
const STATS_MAX_BYTES = 8 * 1024 * 1024;

export interface StatsPageData {
  data: ChartData | null;
  loading: boolean;
  refreshing: boolean;
  loadError: string | null;
  lastUpdatedAt: number | null;
  reload: () => Promise<void>;
}

export function useStatsPageData(): StatsPageData {
  const [data, setData] = useState<ChartData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  const dataRef = useRef<ChartData | null>(null);
  const requestControllerRef = useRef<AbortController | null>(null);
  const requestSequenceRef = useRef(0);
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const statsScope = useMemo(
    () => ({ network: site.networkFilter, observer: site.observerId }),
    [site.networkFilter, site.observerId],
  );

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
        uncachedEndpoint(chartStatsEndpoint(statsScope)),
        { cache: 'no-store', signal },
        { timeoutMs: STATS_TIMEOUT_MS, maxBytes: STATS_MAX_BYTES },
      );
      if (!isStatsPayload(payload)) throw new Error('Stats response was malformed');
      if (signal.aborted || sequence !== requestSequenceRef.current) return;
      const next = payload as ChartData;
      dataRef.current = next;
      setData(next);
      setLastUpdatedAt(Date.now());
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
  }, [statsScope]);

  useEffect(() => {
    requestControllerRef.current?.abort();
    dataRef.current = null;
    setData(null);
    setLastUpdatedAt(null);
    setLoadError(null);
    setLoading(true);
    return () => {
      requestControllerRef.current?.abort();
    };
  }, [load, privacyGeneration]);

  useVisibilityPoll(
    load,
    {
      scopeKey: `stats:${statsScope.network ?? 'all'}:${statsScope.observer ?? 'all'}:${privacyGeneration}`,
      intervalMs: STATS_REFRESH_MS,
      timeoutMs: STATS_TIMEOUT_MS,
    },
  );

  return {
    data,
    loading,
    refreshing,
    loadError,
    lastUpdatedAt,
    reload: async () => load(),
  };
}
