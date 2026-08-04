import { useCallback, useEffect, useRef, useState } from 'react';
import type { AggregatedPacket } from './useNodes.js';
import { useNodeMap, useMessages, usePackets } from './useNodes.js';
import { withScopeParams, uncachedEndpoint } from '../utils/api.js';
import type { Filters } from '../components/FilterPanel/FilterPanel.js';
import {
  aggregateCanonicalPath,
  buildRegularPacketPaths,
  packetObserverIds,
  type AggregatedPredictionState,
  type CanonicalPathNode,
  type MultiObserverBetaResponse,
  type ResolvedPathRoute,
} from './packetPathOverlayUtils.js';
import { useOverlayStore } from '../store/overlayStore.js';
import { PATH_LINE_FADE_MS, PATH_LINE_TTL_MS } from '../components/Map/pathArcStyle.js';

// Retain resolved data beyond the renderer's 60-second visible lifetime so the
// hop animation and final fade can finish before React clears the path state.
const PATH_DATA_RETENTION_MS = PATH_LINE_TTL_MS + PATH_LINE_FADE_MS + 30_000;
const PREDICTION_CACHE_TTL_MS = 120_000;
const MAX_PREDICTION_CACHE = 1200;
const RECENT_PREDICTION_TTL_MS = 45_000;
const MAX_RECENT_PREDICTIONS = 48;

type UsePacketPathOverlayParams = {
  filters: Filters;
  network?: string;
  observer?: string;
};

type UsePacketPathOverlayResult = {
  packetPaths: [number, number][][];
  betaPacketPaths: [number, number][][];
  betaPathPacketHash: string | null;
  betaCanonicalPath: CanonicalPathNode[];
  betaPathRoutes: ResolvedPathRoute[];
  betaObserverIds: string[];
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  betaRemainingHops: number | null;
  /** True during the 1-second CSS fade-out before paths are cleared. */
  pathFadingOut: boolean;
  pinnedPacketId: string | null;
  pinnedPacketSnapshot: AggregatedPacket | null;
  activePacketSnapshot: AggregatedPacket | null;
  handlePacketPin: (packet: AggregatedPacket) => void;
};

type SlowModePendingResponse = { status: 'pending'; remainingMs: number };

function isSlowPending(
  result: MultiObserverBetaResponse | SlowModePendingResponse,
): result is SlowModePendingResponse {
  const maybe = result as SlowModePendingResponse;
  return maybe.status === 'pending' && typeof maybe.remainingMs === 'number';
}

async function fetchServerBetaMulti(
  packetHash: string,
  network?: string,
  mode?: 'slow',
): Promise<MultiObserverBetaResponse | SlowModePendingResponse | null> {
  const modeQuery = mode === 'slow' ? '&mode=slow' : '';
  const endpoint = withScopeParams(
    `/api/path-beta/resolve-multi?hash=${encodeURIComponent(packetHash)}${modeQuery}`,
    { network },
  );
  const response = await fetch(uncachedEndpoint(endpoint), { cache: 'no-store' });
  if (response.status === 202) {
    return response.json() as Promise<SlowModePendingResponse>;
  }
  if (!response.ok) return null;
  return response.json() as Promise<MultiObserverBetaResponse>;
}

/**
 * Slow-mode fetch: asks the backend to wait out the packet's propagation
 * window, retrying while the backend reports `pending`. Bounded so the
 * pinned-packet view cannot hang forever on a stalled resolver.
 */
async function fetchServerBetaMultiSlow(
  packetHash: string,
  network?: string,
): Promise<MultiObserverBetaResponse | null> {
  const maxAttempts = 3;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const result = await fetchServerBetaMulti(packetHash, network, 'slow');
    if (result === null) return null;
    if (isSlowPending(result)) {
      const waitMs = Math.min(Math.max(result.remainingMs + 500, 1_000), 30_000);
      await new Promise((resolve) => window.setTimeout(resolve, waitMs));
      continue;
    }
    return result;
  }
  return null;
}

function cacheKey(packetHash: string, observerIds: string[], network?: string): string {
  return `${network ?? 'all'}|${[...observerIds].sort().join(',')}|${packetHash}`;
}

function packetResolutionKey(packet: AggregatedPacket | null | undefined, network?: string, observer?: string): string | null {
  if (!packet) return null;
  return [
    packet.id,
    packet.packetHash,
    packet.packetType ?? '',
    packet.srcNodeId ?? '',
    packet.rxNodeId ?? '',
    packet.hopCount ?? '',
    packet.pathHashSizeBytes ?? '',
    packet.path?.join(',') ?? '',
    [...packet.observerIds].sort().join(','),
    packet.ts,
    network ?? 'all',
    observer ?? 'all',
  ].join('|');
}

export function usePacketPathOverlay({
  filters,
  network,
  observer,
}: UsePacketPathOverlayParams): UsePacketPathOverlayResult {
  const packets = usePackets();
  const messages = useMessages();
  const nodes = useNodeMap();
  const [packetPaths, setPacketPaths] = useState<[number, number][][]>([]);
  const [betaPacketPaths, setBetaPacketPaths] = useState<[number, number][][]>([]);
  const [betaPathPacketHash, setBetaPathPacketHash] = useState<string | null>(null);
  const [betaCanonicalPath, setBetaCanonicalPath] = useState<CanonicalPathNode[]>([]);
  const [betaPathRoutes, setBetaPathRoutes] = useState<ResolvedPathRoute[]>([]);
  const [betaObserverIds, setBetaObserverIds] = useState<string[]>([]);
  const [betaPathConfidence, setBetaPathConfidence] = useState<number | null>(null);
  // The canonical DTO intentionally does not expose alternative permutations
  // or an unresolved-hop completion path. Keep these metrics null for callers
  // that still render the existing evidence popover fields.
  const [betaPermutationCount, setBetaPermutationCount] = useState<number | null>(null);
  const [betaRemainingHops, setBetaRemainingHops] = useState<number | null>(null);
  const pinnedPacketId = useOverlayStore((state) => state.pinnedPacketId);
  const pinnedPacketSnapshot = useOverlayStore((state) => state.pinnedPacketSnapshot);
  const togglePinnedPacket = useOverlayStore((state) => state.togglePinnedPacket);
  const clearPinnedPacket = useOverlayStore((state) => state.clearPinnedPacket);
  // CSS-based fade: instead of animating opacity via 60fps rAF, set one boolean
  // that triggers the existing CSS transition on the path pane.
  const [pathFadingOut, setPathFadingOut] = useState(false);
  const [isPageVisible, setIsPageVisible] = useState(
    () => (typeof document === 'undefined' ? true : document.visibilityState === 'visible'),
  );

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeReqSeqRef = useRef(0);
  const pinnedOverlayKeyRef = useRef('');
  const recentPredictionsRef = useRef<Map<string, AggregatedPredictionState>>(new Map());
  const multiPredictionCacheRef = useRef<Map<string, { response: MultiObserverBetaResponse; ts: number }>>(new Map());
  const multiInflightRef = useRef<Map<string, Promise<MultiObserverBetaResponse | null>>>(new Map());

  const stopPathTimers = useCallback(() => {
    if (pathTimerRef.current) {
      clearTimeout(pathTimerRef.current);
      pathTimerRef.current = null;
    }
    if (pathFadeTimerRef.current !== null) {
      clearTimeout(pathFadeTimerRef.current);
      pathFadeTimerRef.current = null;
    }
  }, []);

  const clearBetaState = useCallback(() => {
    setBetaPacketPaths([]);
    setBetaPathPacketHash(null);
    setBetaCanonicalPath([]);
    setBetaPathRoutes([]);
    setBetaObserverIds([]);
    setBetaPathConfidence(null);
    setBetaPermutationCount(null);
    setBetaRemainingHops(null);
    useOverlayStore.getState().setPathExplanation(null);
  }, []);

  const clearPathState = useCallback(() => {
    setPacketPaths([]);
    clearBetaState();
    setPathFadingOut(false);
  }, [clearBetaState]);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const updateVisibility = () => setIsPageVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', updateVisibility);
    return () => document.removeEventListener('visibilitychange', updateVisibility);
  }, []);

  const pruneRecentPredictions = useCallback(() => {
    const now = Date.now();
    const recent = recentPredictionsRef.current;
    for (const [key, value] of recent) {
      if (now - value.ts > RECENT_PREDICTION_TTL_MS) recent.delete(key);
    }
    if (recent.size <= MAX_RECENT_PREDICTIONS) return;
    const sorted = Array.from(recent.entries()).sort((a, b) => a[1].ts - b[1].ts);
    for (let i = 0; i < Math.max(0, recent.size - MAX_RECENT_PREDICTIONS); i += 1) {
      const key = sorted[i]?.[0];
      if (key) recent.delete(key);
    }
  }, []);

  const applyAggregatedPrediction = useCallback((aggregated: AggregatedPredictionState) => {
    setBetaPacketPaths(aggregated.routes.map((route) => (
      route.nodes.map((node) => [node.lat, node.lon] as [number, number])
    )));
    setBetaPathPacketHash(aggregated.packetHash);
    setBetaCanonicalPath(aggregated.canonicalPath);
    setBetaPathRoutes(aggregated.routes);
    setBetaObserverIds(aggregated.observerIds);
    setBetaPathConfidence(aggregated.confidence);
    setBetaPermutationCount(null);
    setBetaRemainingHops(null);
    useOverlayStore.getState().setPathExplanation(null);
  }, []);

  const applyServerPrediction = useCallback((
    packetHash: string,
    response: MultiObserverBetaResponse | null,
  ) => {
    const aggregated = aggregateCanonicalPath(response);
    if (aggregated) {
      const state = { ...aggregated, ts: Date.now() };
      applyAggregatedPrediction(state);
      recentPredictionsRef.current.set(packetHash, state);
      pruneRecentPredictions();
      return;
    }

    pruneRecentPredictions();
    const recent = recentPredictionsRef.current.get(packetHash);
    if (recent && Date.now() - recent.ts <= RECENT_PREDICTION_TTL_MS) {
      applyAggregatedPrediction(recent);
      return;
    }
    clearBetaState();
  }, [applyAggregatedPrediction, clearBetaState, pruneRecentPredictions]);

  const prunePredictionCache = useCallback(() => {
    const now = Date.now();
    const cache = multiPredictionCacheRef.current;
    for (const [key, value] of cache) {
      if (now - value.ts > PREDICTION_CACHE_TTL_MS) cache.delete(key);
    }
    if (cache.size <= MAX_PREDICTION_CACHE) return;
    const sorted = Array.from(cache.entries()).sort((a, b) => a[1].ts - b[1].ts);
    const removeCount = Math.max(0, cache.size - MAX_PREDICTION_CACHE);
    for (let i = 0; i < removeCount; i += 1) {
      const key = sorted[i]?.[0];
      if (key) cache.delete(key);
    }
  }, []);

  const getPacketObserverIds = useCallback((packet: AggregatedPacket | undefined): string[] => packetObserverIds(packet), []);

  const buildLocalPaths = useCallback((packet: AggregatedPacket | undefined, observerIds: string[]) => (
    buildRegularPacketPaths(packet, observerIds, nodes)
  ), [nodes]);

  const resolveMultiPrediction = useCallback((
    packetHash: string,
    observerIds: string[],
    networkName?: string,
    minFreshTs = 0,
    mode?: 'slow',
  ): Promise<MultiObserverBetaResponse | null> => {
    prunePredictionCache();
    const key = cacheKey(packetHash, observerIds, networkName);
    // Slow-mode requests must not be served from the eager cache or deduped
    // onto an in-flight eager fetch — the whole point is the post-window set.
    if (mode !== 'slow') {
      const cached = multiPredictionCacheRef.current.get(key);
      if (cached && cached.ts >= minFreshTs && Date.now() - cached.ts <= PREDICTION_CACHE_TTL_MS) {
        return Promise.resolve(cached.response);
      }
      const inflight = multiInflightRef.current.get(key);
      if (inflight) return inflight;
    }

    const fetchFn: () => Promise<MultiObserverBetaResponse | null> = mode === 'slow'
      ? () => fetchServerBetaMultiSlow(packetHash, networkName)
      : async () => {
          const result = await fetchServerBetaMulti(packetHash, networkName);
          // fast path never receives a 202; treat it as unresolved anyway
          return result === null || isSlowPending(result) ? null : result;
        };
    const promise = fetchFn()
      .then((response) => {
        if (response) multiPredictionCacheRef.current.set(key, { response, ts: Date.now() });
        return response;
      })
      .catch(() => null)
      .finally(() => {
        multiInflightRef.current.delete(key);
      });
    multiInflightRef.current.set(key, promise);
    return promise;
  }, [prunePredictionCache]);

  const latestPacket = messages[0] ?? null;
  const latestResolutionKey = packetResolutionKey(latestPacket, network, observer);
  const activePacketSnapshot = pinnedPacketId !== null
    ? (packets.find((packet) => packet.id === pinnedPacketId) ?? pinnedPacketSnapshot)
    : (filters.betaPaths ? latestPacket : null);
  const betaEffectThrottleRef = useRef<number | null>(null);

  // Keep a ref to the latest message so this effect runs only when the active
  // packet identity changes, not on every packet-feed update.
  const messagesRef = useRef(messages);
  messagesRef.current = messages;

  useEffect(() => {
    if (pinnedPacketId !== null) return;

    if (betaEffectThrottleRef.current !== null) return;
    betaEffectThrottleRef.current = window.setTimeout(() => {
      betaEffectThrottleRef.current = null;
    }, 50);

    stopPathTimers();
    pruneRecentPredictions();

    const latest = messagesRef.current[0];
    const observerIds = getPacketObserverIds(latest);
    clearBetaState();
    setPacketPaths(buildLocalPaths(latest, observerIds));

    if (!isPageVisible) {
      setPathFadingOut(false);
      return;
    }

    if (filters.betaPaths && latest?.packetHash && latest.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      void resolveMultiPrediction(latest.packetHash, observerIds, network, latest.ts)
        .then((response) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(latest.packetHash, response);
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(latest.packetHash, null);
        });
    }

    if (!filters.betaPaths || !latest) {
      setPathFadingOut(false);
      return;
    }

    // Start a TTL timer: after PATH_TTL - FADE_MS, begin CSS fade-out. After
    // FADE_MS more, clear the path state entirely.
    const FADE_MS = 1_000;
    setPathFadingOut(false);
    pathTimerRef.current = setTimeout(() => {
      setPathFadingOut(true);
      pathFadeTimerRef.current = setTimeout(() => {
        pathFadeTimerRef.current = null;
        clearPathState();
      }, FADE_MS);
    }, PATH_DATA_RETENTION_MS - FADE_MS);
  // `packets` intentionally omitted — only the latest message drives this
  // effect, so it does not run on every packet-feed update.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestResolutionKey, filters.betaPaths, pinnedPacketId, network, observer, getPacketObserverIds, buildLocalPaths, resolveMultiPrediction, applyServerPrediction, isPageVisible, stopPathTimers, clearPathState, clearBetaState, pruneRecentPredictions]);

  const handlePacketPin = useCallback((packet: AggregatedPacket) => {
    togglePinnedPacket(packet);
  }, [togglePinnedPacket]);

  useEffect(() => {
    if (pinnedTimerRef.current) {
      clearTimeout(pinnedTimerRef.current);
      pinnedTimerRef.current = null;
    }

    if (pinnedPacketId === null) {
      pinnedOverlayKeyRef.current = '';
      stopPathTimers();
      clearPathState();
      return;
    }

    setPathFadingOut(false);
    const FADE_MS = 1_000;
    pinnedTimerRef.current = setTimeout(() => {
      setPathFadingOut(true);
      pathFadeTimerRef.current = setTimeout(() => {
        pathFadeTimerRef.current = null;
        clearPathState();
        clearPinnedPacket();
        pinnedOverlayKeyRef.current = '';
        pinnedTimerRef.current = null;
      }, FADE_MS);
    }, PATH_DATA_RETENTION_MS);
  }, [clearPathState, clearPinnedPacket, pinnedPacketId, stopPathTimers]);

  useEffect(() => {
    if (pinnedPacketId === null) return;
    pruneRecentPredictions();

    if (!isPageVisible) return;

    const pinnedPacket = packets.find((packet) => packet.id === pinnedPacketId) ?? pinnedPacketSnapshot;
    if (!pinnedPacket) return;

    const observerIds = getPacketObserverIds(pinnedPacket);
    const overlayKey = [
      pinnedPacket.id,
      pinnedPacket.packetHash ?? '',
      pinnedPacket.srcNodeId ?? '',
      pinnedPacket.path?.join(',') ?? '',
      observerIds.join(','),
      filters.betaPaths ? 'beta-on' : 'beta-off',
      network ?? 'all',
      observer ?? 'all',
      pinnedPacket.ts,
    ].join('|');

    if (overlayKey === pinnedOverlayKeyRef.current) return;
    pinnedOverlayKeyRef.current = overlayKey;

    clearBetaState();
    setPacketPaths(buildLocalPaths(pinnedPacket, observerIds));

    if (filters.betaPaths && pinnedPacket.packetHash && pinnedPacket.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      // Pinned packet: slow mode — wait out the propagation window so the
      // path is resolved against the COMPLETE observer set. The local path
      // (built above) renders meanwhile; this upgrades it once final.
      void resolveMultiPrediction(pinnedPacket.packetHash, observerIds, network, pinnedPacket.ts, 'slow')
        .then((response) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(pinnedPacket.packetHash!, response);
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPrediction(pinnedPacket.packetHash!, null);
        });
    }
  }, [
    pinnedPacketId,
    packets,
    pinnedPacketSnapshot,
    filters.betaPaths,
    network,
    observer,
    getPacketObserverIds,
    buildLocalPaths,
    resolveMultiPrediction,
    applyServerPrediction,
    isPageVisible,
    pruneRecentPredictions,
    clearBetaState,
  ]);

  useEffect(() => () => {
    stopPathTimers();
    if (pinnedTimerRef.current) clearTimeout(pinnedTimerRef.current);
    if (betaEffectThrottleRef.current !== null) clearTimeout(betaEffectThrottleRef.current);
  }, [stopPathTimers]);

  return {
    packetPaths,
    betaPacketPaths,
    betaPathPacketHash,
    betaCanonicalPath,
    betaPathRoutes,
    betaObserverIds,
    betaPathConfidence,
    betaPermutationCount,
    betaRemainingHops,
    pathFadingOut,
    pinnedPacketId,
    pinnedPacketSnapshot,
    activePacketSnapshot,
    handlePacketPin,
  };
}
