import { useCallback, useEffect, useState } from 'react';
import type { LazyPathResult, ResolvedPath } from '../pages/ukmesh/PacketDetailPanel.js';

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
  observations: Array<{ rxNodeId: string | null; iata: string | null; time: string; rssi: number | null; snr: number | null; hopCount: number | null }>;
};

export type RadioState = {
  frequency?: number;
  sf?: number;
  bw?: number;
  cr?: number;
  channel?: string;
};

type StaticData = { detail: PacketDetail | null; radio: RadioState | null };
const CACHE_TTL_MS = 5 * 60_000;
export const LAZY_PATH_SETTLE_MS = 10_000;
const staticCache = new Map<string, { expiresAt: number; value: StaticData }>();
const staticInflight = new Map<string, Promise<StaticData>>();
const pathCache = new Map<string, { expiresAt: number; value: ResolvedPath[] }>();

export function lazyPathSettleRemainingMs(packetTime: string, nowMs = Date.now()): number {
  const parsedPacketTime = Date.parse(packetTime);
  const ageMs = Number.isFinite(parsedPacketTime) ? Math.max(0, nowMs - parsedPacketTime) : LAZY_PATH_SETTLE_MS;
  return Math.max(0, LAZY_PATH_SETTLE_MS - ageMs);
}

async function cachedStatic(packetHash: string, network: string): Promise<StaticData> {
  const key = `${network}:${packetHash}`;
  const cached = staticCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.value;
  const pending = staticInflight.get(key);
  if (pending) return pending;
  const request = Promise.all([
    fetch(`/api/packets/${packetHash}?network=${encodeURIComponent(network)}`, { signal: AbortSignal.timeout(10_000) })
      .then((response) => response.ok ? response.json() as Promise<PacketDetail> : null)
      .catch(() => null),
    fetch('/api/radio-stats', { signal: AbortSignal.timeout(8_000) })
      .then((response) => response.ok ? response.json() as Promise<RadioState> : null)
      .catch(() => null),
  ]).then(([detail, radio]) => {
    const value = { detail, radio };
    staticCache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, value });
    return value;
  }).finally(() => staticInflight.delete(key));
  staticInflight.set(key, request);
  return request;
}

export function usePacketDetailData(input: {
  packetHash: string;
  packetTime: string;
  network: string;
  observerKey: string;
  hasPathHashes: boolean;
  cachedLazyPath?: LazyPathResult | null;
}) {
  const { packetHash, packetTime, network, observerKey, hasPathHashes, cachedLazyPath } = input;
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
    setLoading(true);
    void cachedStatic(packetHash, network).then((value) => {
      if (!active) return;
      setDetail(value.detail);
      setRadio(value.radio);
      setLoading(false);
    });
    return () => { active = false; };
  }, [packetHash, network]);

  useEffect(() => {
    let active = true;
    const key = `${network}:${packetHash}:${observerKey}`;
    const cached = pathCache.get(key);
    if (cached && cached.expiresAt > Date.now()) {
      setResolvedPaths(cached.value);
      setPathLoading(false);
      return;
    }
    setPathLoading(true);
    const netParam = network ? `&network=${encodeURIComponent(network)}` : '';
    fetch(`/api/path-beta/resolve-multi?hash=${packetHash}${netParam}`, { signal: AbortSignal.timeout(12_000) })
      .then((response) => response.ok ? response.json() as Promise<{ results?: ResolvedPath[] }> : { results: [] })
      .then((value) => {
        if (!active) return;
        const paths = value.results ?? [];
        pathCache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, value: paths });
        setResolvedPaths(paths);
      })
      .catch(() => { if (active) setResolvedPaths([]); })
      .finally(() => { if (active) setPathLoading(false); });
    return () => { active = false; };
  }, [packetHash, network, observerKey]);

  const fetchLazyPath = useCallback(async () => {
    setLazyStatus('loading');
    try {
      const netParam = network ? `&network=${encodeURIComponent(network)}` : '';
      const response = await fetch(`/api/path-lazy/resolve?hash=${packetHash}${netParam}`, { signal: AbortSignal.timeout(15_000) });
      if (response.status === 404) { setLazyStatus('notfound'); return; }
      if (!response.ok) { setLazyStatus('error'); return; }
      setLazyPath(await response.json() as LazyPathResult);
      setLazyStatus('done');
    } catch {
      setLazyStatus('error');
    }
  }, [packetHash, network]);

  useEffect(() => {
    if (cachedLazyPath) {
      setLazyPath(cachedLazyPath);
      setLazyStatus('done');
      setLazyCountdown(0);
      return;
    }
    if (!hasPathHashes) {
      setLazyStatus('notfound');
      return;
    }
    setLazyPath(null);
    const remainingMs = lazyPathSettleRemainingMs(packetTime);
    if (remainingMs === 0) {
      void fetchLazyPath();
      return;
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
      void fetchLazyPath();
    }, remainingMs);
    return () => {
      window.clearInterval(tick);
      window.clearTimeout(timer);
    };
  }, [cachedLazyPath, fetchLazyPath, hasPathHashes, observerKey, packetTime]);

  return { detail, radio, resolvedPaths, loading, pathLoading, lazyPath, lazyStatus, lazyCountdown };
}
