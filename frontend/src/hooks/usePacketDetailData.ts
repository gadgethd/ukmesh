import { useCallback, useEffect, useState } from 'react';
import type { LazyPathResult } from '../features/packets/feedModel.js';
import { lazyPathSettleRemainingMs } from '../features/pathing/timing.js';
import type { MultiObserverBetaResponse } from '../features/pathing/model.js';
import type { MessageTags, TagConfidence } from '../types/domain.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { ApiResponseError, checkedJson, fetchJson, withScopeParams } from '../utils/api.js';
import { fetchPathResolution } from '../utils/pathResolutionRequest.js';
import { ScopedCache } from '../utils/scopedCache.js';
import { scopeIsReady, scopeKey as publicScopeKey } from '../utils/scope.js';

export type PacketDetail = {
  packetHash: string;
  time: string;
  rxNodeId: string | null;
  srcNodeId: string | null;
  topic: string;
  iata: string | null;
  packetType: number | null;
  routeType: number | null;
  hopCount: number | null;
  rssi: number | null;
  snr: number | null;
  payload: Record<string, unknown> | null;
  pathHashes: string[] | null;
  pathHashSizeBytes: number | null;
  rawHex: string | null;
  tags?: MessageTags | null;
  tagConfidence?: TagConfidence | null;
  observations: Array<{ rxNodeId: string | null; iata: string | null; time: string; rssi: number | null; snr: number | null; hopCount: number | null }>;
};

export type RadioState = {
  frequency?: number;
  sf?: number;
  bw?: number;
  cr?: number;
  channel?: string;
};

const CACHE_TTL_MS = 5 * 60_000;
const detailCache = new ScopedCache<PacketDetail | null>({
  name: 'packet-detail',
  ttlMs: CACHE_TTL_MS,
  maxEntries: 256,
  maxBytes: 16 * 1024 * 1024,
  maxInflight: 8,
});
const radioCache = new ScopedCache<RadioState | null>({
  name: 'radio-state',
  ttlMs: CACHE_TTL_MS,
  maxEntries: 16,
  maxBytes: 128 * 1024,
  maxInflight: 4,
});
const pathCache = new ScopedCache<MultiObserverBetaResponse[]>({
  name: 'packet-resolved-paths',
  ttlMs: CACHE_TTL_MS,
  maxEntries: 256,
  maxBytes: 24 * 1024 * 1024,
  maxInflight: 8,
});

async function cachedPacketDetail(
  packetHash: string,
  network: string,
  observer: string | undefined,
  scopeKey: string,
  signal: AbortSignal,
): Promise<PacketDetail | null> {
  return detailCache.getOrLoad(scopeKey, packetHash.toUpperCase(), async (loadSignal) => {
    try {
      return await fetchJson<PacketDetail>(
        withScopeParams(`/api/packets/${encodeURIComponent(packetHash)}`, { network, observer }),
        { signal: loadSignal, cache: 'no-store' },
        { timeoutMs: 10_000, maxBytes: 4 * 1024 * 1024 },
      );
    } catch (error) {
      if (error instanceof ApiResponseError && error.status === 404) return null;
      throw error;
    }
  }, signal);
}

async function cachedRadioState(
  network: string,
  observer: string | undefined,
  scopeKey: string,
  signal: AbortSignal,
): Promise<RadioState | null> {
  return radioCache.getOrLoad(scopeKey, 'global', async (loadSignal) => {
    try {
      return await fetchJson<RadioState>(
        withScopeParams('/api/radio-stats', { network, observer }),
        { signal: loadSignal, cache: 'no-store' },
        { timeoutMs: 8_000, maxBytes: 128 * 1024 },
      );
    } catch (error) {
      if (error instanceof ApiResponseError && error.status === 404) return null;
      throw error;
    }
  }, signal);
}

export function usePacketDetailData(input: {
  packetHash: string;
  packetTime: string;
  network: string;
  observer?: string;
  observerKey: string;
  hasPathHashes: boolean;
  cachedLazyPath?: LazyPathResult | null;
}) {
  const { packetHash, packetTime, network, observer, observerKey, hasPathHashes, cachedLazyPath } = input;
  const runtimeFeatures = useRuntimeFeatures();
  const scopeKey = publicScopeKey({ network, observer, privacyGeneration: runtimeFeatures.privacyGeneration });
  const ready = scopeIsReady({ privacyGeneration: runtimeFeatures.privacyGeneration });
  const representationKey = JSON.stringify([scopeKey, packetHash, observerKey]);
  const [displayKey, setDisplayKey] = useState(representationKey);
  useEffect(() => {
    detailCache.activateScope(scopeKey);
    radioCache.activateScope(scopeKey);
    pathCache.activateScope(scopeKey);
  }, [scopeKey]);
  const [detail, setDetail] = useState<PacketDetail | null>(null);
  const [radio, setRadio] = useState<RadioState | null>(null);
  const [resolvedPaths, setResolvedPaths] = useState<MultiObserverBetaResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [pathLoading, setPathLoading] = useState(false);
  const [lazyPath, setLazyPath] = useState<LazyPathResult | null>(null);
  const [lazyStatus, setLazyStatus] = useState<'idle' | 'settling' | 'loading' | 'done' | 'notfound' | 'error'>('idle');
  const [lazyCountdown, setLazyCountdown] = useState(0);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setLoading(true);
    setDetail(null);
    setRadio(null);
    setDisplayKey(representationKey);
    if (!ready) return () => controller.abort();
    void Promise.allSettled([
      cachedPacketDetail(packetHash, network, observer, scopeKey, controller.signal),
      cachedRadioState(network, observer, scopeKey, controller.signal),
    ]).then(([detailResult, radioResult]) => {
      if (!active || controller.signal.aborted) return;
      setDetail(detailResult.status === 'fulfilled' ? detailResult.value : null);
      setRadio(radioResult.status === 'fulfilled' ? radioResult.value : null);
      setLoading(false);
    });
    return () => {
      active = false;
      controller.abort();
    };
  }, [network, observer, packetHash, scopeKey, representationKey, ready]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    if (!ready || !hasPathHashes) {
      setResolvedPaths([]);
      setPathLoading(false);
      return () => controller.abort();
    }
    setResolvedPaths([]);
    const key = `${packetHash.toUpperCase()}:${observerKey}`;
    setPathLoading(true);
    void pathCache.getOrLoad(scopeKey, key, async (loadSignal) => {
      const response = await fetchPathResolution(
        withScopeParams(`/api/path-beta/resolve-multi?hash=${encodeURIComponent(packetHash)}`, {
          network,
          observer,
        }),
        { signal: loadSignal, cache: 'no-store' },
      );
      const value = await checkedJson<MultiObserverBetaResponse>(response, { maxBytes: 8 * 1024 * 1024 });
      return Array.isArray(value.canonicalPath) && Array.isArray(value.observers) ? [value] : [];
    }, controller.signal)
      .then((paths) => {
        if (!active) return;
        setResolvedPaths(paths);
      })
      .catch((error: unknown) => {
        if (active && !controller.signal.aborted && (error as DOMException).name !== 'AbortError') {
          setResolvedPaths([]);
        }
      })
      .finally(() => { if (active) setPathLoading(false); });
    return () => {
      active = false;
      controller.abort();
    };
  }, [hasPathHashes, network, observer, observerKey, packetHash, scopeKey, ready]);

  const fetchLazyPath = useCallback(async (signal: AbortSignal) => {
    setLazyStatus('loading');
    try {
      const value = await fetchJson<LazyPathResult>(
        withScopeParams(`/api/path-lazy/resolve?hash=${encodeURIComponent(packetHash)}`, {
          network,
          observer,
        }),
        { signal, cache: 'no-store' },
        { timeoutMs: 15_000, maxBytes: 4 * 1024 * 1024 },
      );
      if (signal.aborted) return;
      setLazyPath(value);
      setLazyStatus('done');
    } catch (error) {
      if (signal.aborted) return;
      if (error instanceof ApiResponseError && error.status === 404) {
        setLazyStatus('notfound');
        return;
      }
      if (!signal.aborted && (error as DOMException).name !== 'AbortError') {
        setLazyStatus('error');
      }
    }
  }, [packetHash, network, observer, scopeKey]);

  useEffect(() => {
    const controller = new AbortController();
    if (!ready) {
      setLazyPath(null); setLazyStatus('idle'); setLazyCountdown(0);
      return () => controller.abort();
    }
    if (cachedLazyPath) {
      setLazyPath(cachedLazyPath);
      setLazyStatus('done');
      setLazyCountdown(0);
      return () => controller.abort();
    }
    if (!hasPathHashes) {
      setLazyPath(null);
      setLazyStatus('notfound');
      setLazyCountdown(0);
      return () => controller.abort();
    }
    setLazyPath(null);
    const remainingMs = lazyPathSettleRemainingMs(packetTime);
    if (remainingMs === 0) {
      void fetchLazyPath(controller.signal);
      return () => controller.abort();
    }
    setLazyStatus('settling');
    setLazyCountdown(Math.ceil(remainingMs / 1_000));
    const settleDeadline = Date.now() + remainingMs;
    const tick = window.setInterval(() => {
      setLazyCountdown(Math.max(0, Math.ceil((settleDeadline - Date.now()) / 1_000)));
    }, 250);
    const timer = window.setTimeout(() => {
      window.clearInterval(tick);
      setLazyCountdown(0);
      void fetchLazyPath(controller.signal);
    }, remainingMs);
    return () => {
      window.clearInterval(tick);
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [cachedLazyPath, fetchLazyPath, hasPathHashes, observerKey, packetTime, ready]);

  const visible = ready && displayKey === representationKey;
  return { detail: visible ? detail : null, radio: visible ? radio : null,
    resolvedPaths: visible ? resolvedPaths : [], loading: !visible || loading,
    pathLoading: visible && pathLoading, lazyPath: visible ? lazyPath : null,
    lazyStatus: visible ? lazyStatus : 'idle' as const, lazyCountdown: visible ? lazyCountdown : 0 };
}
