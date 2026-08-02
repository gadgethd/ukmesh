import { useCallback, useEffect, useState } from 'react';
import type { LazyPathResult, ResolvedPath } from '../pages/ukmesh/PacketDetailPanel.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { ApiResponseError, fetchJson, withScopeParams } from '../utils/api.js';
import { ScopedCache } from '../utils/scopedCache.js';

export type PacketDetail = {
  packetHash: string;
  time: string;
  rxNodeId: string | null;
  srcNodeId: string | null;
  topic: string;
  packetType: number | null;
  routeType: number | null;
  hopCount: number | null;
  rssi: number | null;
  snr: number | null;
  payload: Record<string, unknown> | null;
  pathHashes: string[] | null;
  pathHashSizeBytes: number | null;
  rawHex: string | null;
  observations: Array<{ rxNodeId: string | null; time: string; rssi: number | null; snr: number | null; hopCount: number | null }>;
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
const pathCache = new ScopedCache<ResolvedPath[]>({
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
  return detailCache.getOrLoad(scopeKey, packetHash.toUpperCase(), async () => {
    try {
      return await fetchJson<PacketDetail>(
        withScopeParams(`/api/packets/${encodeURIComponent(packetHash)}`, { network, observer }),
        { signal, cache: 'no-store' },
        { timeoutMs: 10_000, maxBytes: 4 * 1024 * 1024 },
      );
    } catch (error) {
      if (error instanceof ApiResponseError && error.status === 404) return null;
      throw error;
    }
  });
}

async function cachedRadioState(
  network: string,
  observer: string | undefined,
  scopeKey: string,
  signal: AbortSignal,
): Promise<RadioState | null> {
  return radioCache.getOrLoad(scopeKey, 'global', async () => {
    try {
      return await fetchJson<RadioState>(
        withScopeParams('/api/radio-stats', { network, observer }),
        { signal, cache: 'no-store' },
        { timeoutMs: 8_000, maxBytes: 128 * 1024 },
      );
    } catch (error) {
      if (error instanceof ApiResponseError && error.status === 404) return null;
      throw error;
    }
  });
}

export function usePacketDetailData(input: {
  packetHash: string;
  network: string;
  observer?: string;
  observerKey: string;
  hasPathHashes: boolean;
  cachedLazyPath?: LazyPathResult | null;
}) {
  const { packetHash, network, observer, observerKey, hasPathHashes, cachedLazyPath } = input;
  const runtimeFeatures = useRuntimeFeatures();
  const scopeKey = `${network}|${observerKey}|privacy-${runtimeFeatures.privacyGeneration}`;
  const [detail, setDetail] = useState<PacketDetail | null>(null);
  const [radio, setRadio] = useState<RadioState | null>(null);
  const [resolvedPaths, setResolvedPaths] = useState<ResolvedPath[]>([]);
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
  }, [network, observer, packetHash, scopeKey]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    if (!hasPathHashes) {
      setResolvedPaths([]);
      setPathLoading(false);
      return () => controller.abort();
    }
    setResolvedPaths([]);
    const key = `${packetHash.toUpperCase()}:${observerKey}`;
    setPathLoading(true);
    void pathCache.getOrLoad(scopeKey, key, async () => {
      const value = await fetchJson<{ results?: ResolvedPath[] }>(
        withScopeParams(`/api/path-beta/resolve-multi?hash=${encodeURIComponent(packetHash)}`, {
          network,
          observer,
        }),
        { signal: controller.signal, cache: 'no-store' },
        { timeoutMs: 12_000, maxBytes: 8 * 1024 * 1024 },
      );
      return Array.isArray(value.results) ? value.results : [];
    })
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
  }, [hasPathHashes, network, observer, observerKey, packetHash, scopeKey]);

  const fetchLazyPath = useCallback(async (signal: AbortSignal) => {
    setLazyStatus('loading');
    try {
      const value = await fetchJson<LazyPathResult>(
        withScopeParams(`/api/path-lazy/resolve?hash=${encodeURIComponent(packetHash)}`, {
          network,
          observer,
        }),
        { signal, cache: 'no-store' },
        { timeoutMs: 12_000, maxBytes: 4 * 1024 * 1024 },
      );
      if (signal.aborted) return;
      setLazyPath(value);
      setLazyStatus('done');
    } catch (error) {
      if (error instanceof ApiResponseError && error.status === 404) {
        setLazyStatus('notfound');
        return;
      }
      if (!signal.aborted && (error as DOMException).name !== 'AbortError') {
        setLazyStatus('error');
      }
    }
  }, [packetHash, network, observer]);

  useEffect(() => {
    const controller = new AbortController();
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
    setLazyStatus('settling');
    setLazyCountdown(10);
    const tick = window.setInterval(() => setLazyCountdown((value) => Math.max(0, value - 1)), 1_000);
    const timer = window.setTimeout(() => {
      window.clearInterval(tick);
      setLazyCountdown(0);
      void fetchLazyPath(controller.signal);
    }, 10_000);
    return () => {
      window.clearInterval(tick);
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [cachedLazyPath, fetchLazyPath, hasPathHashes, observerKey]);

  return { detail, radio, resolvedPaths, loading, pathLoading, lazyPath, lazyStatus, lazyCountdown };
}
