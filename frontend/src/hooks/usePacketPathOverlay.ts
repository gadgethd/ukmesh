import { useCallback, useEffect, useRef, useState } from 'react';
import type { AggregatedPacket } from './useNodes.js';
import { useNodeMap, usePackets } from './useNodes.js';
import { withScopeParams, uncachedEndpoint } from '../utils/api.js';
import type { Filters } from '../components/FilterPanel/FilterPanel.js';
import { hasCoords } from '../utils/pathing.js';
import {
  aggregateServerPredictions,
  buildRegularPacketPaths,
  packetObserverIds,
  type AggregatedPredictionState,
  type MultiObserverBetaResponse,
  type PathSegment,
  type ServerBetaResponse,
} from './packetPathOverlayUtils.js';
import { useOverlayStore } from '../store/overlayStore.js';

const PATH_TTL = 5_000;
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
  betaLowConfidencePaths: [number, number][][];
  betaLowConfidenceSegments: PathSegment[];
  betaCompletionPaths: [number, number][][];
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  betaRemainingHops: number | null;
  /** True during the 1-second CSS fade-out before paths are cleared. Use to apply a CSS transition class instead of animating opacity in React state. */
  pathFadingOut: boolean;
  pinnedPacketId: string | null;
  pinnedPacketSnapshot: AggregatedPacket | null;
  activePacketSnapshot: AggregatedPacket | null;
  handlePacketPin: (packet: AggregatedPacket) => void;
};

async function fetchServerBeta(packetHash: string, network?: string, observer?: string, signal?: AbortSignal): Promise<ServerBetaResponse | null> {
  const endpoint = withScopeParams(`/api/path-beta/resolve?hash=${encodeURIComponent(packetHash)}`, { network, observer });
  const response = await fetch(uncachedEndpoint(endpoint), { cache: 'no-store', signal });
  if (!response.ok) return null;
  return response.json() as Promise<ServerBetaResponse>;
}

async function fetchServerBetaMulti(packetHash: string, network?: string, signal?: AbortSignal): Promise<MultiObserverBetaResponse | null> {
  const endpoint = withScopeParams(`/api/path-beta/resolve-multi?hash=${encodeURIComponent(packetHash)}`, { network });
  const response = await fetch(uncachedEndpoint(endpoint), { cache: 'no-store', signal });
  if (!response.ok) return null;
  return response.json() as Promise<MultiObserverBetaResponse>;
}

function cacheKey(packetHash: string, network?: string, observer?: string): string {
  return `${network ?? 'all'}|${observer ?? 'all'}|${packetHash}`;
}

function multiCacheKey(packetHash: string, observerIds: string[], network?: string): string {
  return `multi|${network ?? 'all'}|${[...observerIds].sort().join(',')}|${packetHash}`;
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
  const nodes = useNodeMap();
  const [packetPaths, setPacketPaths] = useState<[number, number][][]>([]);
  const [betaPacketPaths, setBetaPacketPaths] = useState<[number, number][][]>([]);
  const [betaLowConfidencePaths, setBetaLowConfidencePaths] = useState<[number, number][][]>([]);
  const [betaLowConfidenceSegments, setBetaLowConfidenceSegments] = useState<PathSegment[]>([]);
  const [betaCompletionPaths, setBetaCompletionPaths] = useState<[number, number][][]>([]);
  const [betaPathConfidence, setBetaPathConfidence] = useState<number | null>(null);
  const [betaPermutationCount, setBetaPermutationCount] = useState<number | null>(null);
  const [betaRemainingHops, setBetaRemainingHops] = useState<number | null>(null);
  const pinnedPacketId = useOverlayStore((state) => state.pinnedPacketId);
  const pinnedPacketSnapshot = useOverlayStore((state) => state.pinnedPacketSnapshot);
  const togglePinnedPacket = useOverlayStore((state) => state.togglePinnedPacket);
  const clearPinnedPacket = useOverlayStore((state) => state.clearPinnedPacket);
  // CSS-based fade: instead of animating opacity via 60fps rAF (which caused ~60 MapView
  // re-renders/second), we set a single boolean that triggers a CSS transition on the pane.
  const [pathFadingOut, setPathFadingOut] = useState(false);
  const [isPageVisible, setIsPageVisible] = useState(
    () => (typeof document === 'undefined' ? true : document.visibilityState === 'visible'),
  );

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const betaReqRef = useRef<AbortController | null>(null);
  const predictionCacheRef = useRef<Map<string, { prediction: ServerBetaResponse | null; ts: number }>>(new Map());
  const inFlightRef = useRef<Map<string, Promise<ServerBetaResponse | null>>>(new Map());
  const activeReqSeqRef = useRef(0);
  const pinnedOverlayKeyRef = useRef('');
  const recentPredictionsRef = useRef<Map<string, AggregatedPredictionState>>(new Map());

  const stopPathTimers = useCallback(() => {
    if (pathTimerRef.current) {
      clearTimeout(pathTimerRef.current);
      pathTimerRef.current = null;
    }
    if (pathFadeTimerRef.current !== null) {
      clearTimeout(pathFadeTimerRef.current);
      pathFadeTimerRef.current = null;
    }
    if (betaReqRef.current) {
      betaReqRef.current.abort();
      betaReqRef.current = null;
    }
  }, []);

  const clearPathState = useCallback(() => {
    setPacketPaths([]);
    setBetaPacketPaths([]);
    setBetaLowConfidencePaths([]);
    setBetaLowConfidenceSegments([]);
    setBetaCompletionPaths([]);
    setBetaPathConfidence(null);
    setBetaPermutationCount(null);
    setBetaRemainingHops(null);
    setPathFadingOut(false);
  }, []);

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

  const applyServerPredictions = useCallback((
    packetHash: string,
    predictions: Array<ServerBetaResponse | null>,
    options?: {
      allowCompletionPaths?: boolean;
      collapseUnanchoredAdvertPartials?: boolean;
    },
  ) => {
    const validPredictions = predictions.filter((prediction): prediction is ServerBetaResponse => Boolean(prediction?.ok));
    if (validPredictions.length < 1) {
      pruneRecentPredictions();
      const recent = recentPredictionsRef.current.get(packetHash);
      if (!recent || Date.now() - recent.ts > RECENT_PREDICTION_TTL_MS) {
        setBetaPacketPaths([]);
        setBetaLowConfidencePaths([]);
        setBetaLowConfidenceSegments([]);
        setBetaCompletionPaths([]);
        setBetaPathConfidence(null);
        setBetaPermutationCount(null);
        setBetaRemainingHops(null);
        return;
      }
      setBetaPacketPaths(recent.purplePaths);
      setBetaLowConfidencePaths(recent.redPaths);
      setBetaLowConfidenceSegments(recent.redSegments);
      setBetaCompletionPaths(options?.allowCompletionPaths === false ? [] : recent.completionPaths);
      setBetaPathConfidence(recent.confidence);
      setBetaPermutationCount(recent.permutations);
      setBetaRemainingHops(recent.remainingHops);
      return;
    }
    const aggregated = aggregateServerPredictions(validPredictions, options);
    if (!aggregated) return;
    setBetaPacketPaths(aggregated.purplePaths);
    setBetaLowConfidencePaths(aggregated.redPaths);
    setBetaLowConfidenceSegments(aggregated.redSegments);
    setBetaCompletionPaths(aggregated.completionPaths);
    setBetaPathConfidence(aggregated.confidence);
    setBetaPermutationCount(aggregated.permutations);
    setBetaRemainingHops(aggregated.remainingHops);

    recentPredictionsRef.current.set(packetHash, { ...aggregated, ts: Date.now() });
    pruneRecentPredictions();
  }, [pruneRecentPredictions]);

  const prunePredictionCache = useCallback(() => {
    const now = Date.now();
    const cache = predictionCacheRef.current;
    for (const [key, value] of cache) {
      if (now - value.ts > PREDICTION_CACHE_TTL_MS) cache.delete(key);
    }
    if (cache.size <= MAX_PREDICTION_CACHE) return;
    const sorted = Array.from(cache.entries()).sort((a, b) => a[1].ts - b[1].ts);
    const removeCount = Math.max(0, cache.size - MAX_PREDICTION_CACHE);
    for (let i = 0; i < removeCount; i++) {
      const k = sorted[i]?.[0];
      if (k) cache.delete(k);
    }
  }, []);

  const getPacketObserverIds = useCallback((packet: AggregatedPacket | undefined): string[] => packetObserverIds(packet), []);

  const buildLocalPaths = useCallback((packet: AggregatedPacket | undefined, observerIds: string[]) => (
    buildRegularPacketPaths(packet, observerIds, nodes)
  ), [nodes]);

  const shouldCollapseAdvertObserverPartials = useCallback((packet: AggregatedPacket | undefined): boolean => {
    if (!packet || packet.packetType !== 4 || !packet.srcNodeId) return false;
    const src = nodes.get(packet.srcNodeId);
    return !hasCoords(src);
  }, [nodes]);

  const resolvePrediction = useCallback((packetHash: string, networkName?: string, observerId?: string, minFreshTs = 0): Promise<ServerBetaResponse | null> => {
    prunePredictionCache();
    const key = cacheKey(packetHash, networkName, observerId);
    const cached = predictionCacheRef.current.get(key);
    if (cached && cached.ts >= minFreshTs && Date.now() - cached.ts <= PREDICTION_CACHE_TTL_MS) {
      return Promise.resolve(cached.prediction);
    }

    const inflight = inFlightRef.current.get(key);
    if (inflight) return inflight;

    const p = fetchServerBeta(packetHash, networkName, observerId)
      .then((prediction) => {
        if (prediction !== null) {
          predictionCacheRef.current.set(key, { prediction, ts: Date.now() });
        }
        return prediction;
      })
      .catch(() => null)
      .finally(() => {
        inFlightRef.current.delete(key);
      });
    inFlightRef.current.set(key, p);
    return p;
  }, [prunePredictionCache]);

  const multiPredictionCacheRef = useRef<Map<string, { results: ServerBetaResponse[]; ts: number }>>(new Map());
  const multiInflightRef = useRef<Map<string, Promise<ServerBetaResponse[]>>>(new Map());

  const resolveMultiPrediction = useCallback((packetHash: string, observerIds: string[], networkName?: string, minFreshTs = 0): Promise<ServerBetaResponse[]> => {
    prunePredictionCache();
    const key = multiCacheKey(packetHash, observerIds, networkName);

    const cached = multiPredictionCacheRef.current.get(key);
    if (cached && cached.ts >= minFreshTs && Date.now() - cached.ts <= PREDICTION_CACHE_TTL_MS) {
      return Promise.resolve(cached.results);
    }

    const inflight = multiInflightRef.current.get(key);
    if (inflight) return inflight;

    const p = fetchServerBetaMulti(packetHash, networkName)
      .then((response) => {
        const results = response?.ok ? response.results : [];
        if (results.length > 0) {
          multiPredictionCacheRef.current.set(key, { results, ts: Date.now() });
          // Evict stale multi-cache entries
          const now = Date.now();
          for (const [k, v] of multiPredictionCacheRef.current) {
            if (now - v.ts > PREDICTION_CACHE_TTL_MS) multiPredictionCacheRef.current.delete(k);
          }
          if (multiPredictionCacheRef.current.size > MAX_PREDICTION_CACHE) {
            const sorted = Array.from(multiPredictionCacheRef.current.entries()).sort((a, b) => a[1].ts - b[1].ts);
            for (let i = 0; i < Math.max(0, multiPredictionCacheRef.current.size - MAX_PREDICTION_CACHE); i++) {
              const k2 = sorted[i]?.[0];
              if (k2) multiPredictionCacheRef.current.delete(k2);
            }
          }
        }
        return results;
      })
      .catch(() => [] as ServerBetaResponse[])
      .finally(() => {
        multiInflightRef.current.delete(key);
      });
    multiInflightRef.current.set(key, p);
    return p;
  }, [prunePredictionCache]);

  const latestPacket = packets.find((p) => p.packetType === 4 || p.packetType === 5) ?? null;
  const latestResolutionKey = packetResolutionKey(latestPacket, network, observer);
  const activePacketSnapshot = pinnedPacketId !== null
    ? (packets.find((packet) => packet.id === pinnedPacketId) ?? pinnedPacketSnapshot)
    : (filters.betaPaths ? latestPacket : null);
  const betaEffectThrottleRef = useRef<number | null>(null);

  // Keep a ref to the latest packets so we can read them inside effects without
  // adding `packets` to the dependency array (which would trigger on every packet
  // arrival, not just when the active path packet changes).
  const packetsRef = useRef(packets);
  packetsRef.current = packets;

  useEffect(() => {
    if (pinnedPacketId !== null) return;

    // Throttle beta path resolution to max once per 50ms
    if (betaEffectThrottleRef.current !== null) return;
    betaEffectThrottleRef.current = window.setTimeout(() => {
      betaEffectThrottleRef.current = null;
    }, 50);

    stopPathTimers();
    pruneRecentPredictions();

    // Use the ref so we always see the latest packets without re-triggering this
    // effect on every packet arrival.
    const latest = packetsRef.current.find((p) => p.packetType === 4 || p.packetType === 5);
    const observerIds = getPacketObserverIds(latest);
    setPacketPaths(buildLocalPaths(latest, observerIds));

    if (!isPageVisible) {
      setPathFadingOut(false);
      return;
    }

    if (filters.betaPaths && latest?.packetHash && latest.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      const resolveFn = observerIds.length > 1
        ? resolveMultiPrediction(latest.packetHash, observerIds, network, latest.ts)
        : Promise.all(observerIds.map((observerId) => resolvePrediction(latest.packetHash, network, observerId, latest.ts)));
      void resolveFn
        .then((predictions) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(latest.packetHash, predictions, {
            allowCompletionPaths: latest.packetType === 4,
            collapseUnanchoredAdvertPartials: shouldCollapseAdvertObserverPartials(latest),
          });
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(latest.packetHash, [], {
            allowCompletionPaths: latest.packetType === 4,
            collapseUnanchoredAdvertPartials: shouldCollapseAdvertObserverPartials(latest),
          });
        });
    } else {
      setBetaPacketPaths([]);
      setBetaLowConfidencePaths([]);
      setBetaLowConfidenceSegments([]);
      setBetaCompletionPaths([]);
      setBetaPathConfidence(null);
      setBetaPermutationCount(null);
      setBetaRemainingHops(null);
    }

    if (!filters.betaPaths || !latest) {
      setPathFadingOut(false);
      return;
    }

    // Start a TTL timer: after PATH_TTL - FADE_MS, begin CSS fade-out (2 state updates total
    // instead of 60 rAF updates). After FADE_MS more, clear paths entirely.
    const FADE_MS = 1_000;
    setPathFadingOut(false);
    pathTimerRef.current = setTimeout(() => {
      setPathFadingOut(true);
      pathFadeTimerRef.current = setTimeout(() => {
        pathFadeTimerRef.current = null;
        clearPathState();
      }, FADE_MS);
    }, PATH_TTL - FADE_MS);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  // `packets` intentionally omitted — accessed via packetsRef to avoid firing on every
  // packet arrival. Effect only re-runs when latestId changes (a new distinct path packet).
  }, [latestResolutionKey, filters.betaPaths, pinnedPacketId, network, observer, getPacketObserverIds, buildLocalPaths, resolvePrediction, resolveMultiPrediction, stopPathTimers, clearPathState, applyServerPredictions, isPageVisible, pruneRecentPredictions]);

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
    }, 30_000);
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

    setPacketPaths(buildLocalPaths(pinnedPacket, observerIds));

    if (filters.betaPaths && pinnedPacket.packetHash && pinnedPacket.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      const resolveFn = observerIds.length > 1
        ? resolveMultiPrediction(pinnedPacket.packetHash!, observerIds, network, pinnedPacket.ts)
        : Promise.all(observerIds.map((observerId) => resolvePrediction(pinnedPacket.packetHash!, network, observerId, pinnedPacket.ts)));
      void resolveFn
        .then((predictions) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(pinnedPacket.packetHash!, predictions, {
            allowCompletionPaths: pinnedPacket.packetType === 4,
            collapseUnanchoredAdvertPartials: shouldCollapseAdvertObserverPartials(pinnedPacket),
          });
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(pinnedPacket.packetHash!, [], {
            allowCompletionPaths: pinnedPacket.packetType === 4,
            collapseUnanchoredAdvertPartials: shouldCollapseAdvertObserverPartials(pinnedPacket),
          });
        });
    } else {
      setBetaPacketPaths([]);
      setBetaLowConfidencePaths([]);
      setBetaLowConfidenceSegments([]);
      setBetaCompletionPaths([]);
      setBetaPathConfidence(null);
      setBetaPermutationCount(null);
      setBetaRemainingHops(null);
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
    shouldCollapseAdvertObserverPartials,
    resolvePrediction,
    resolveMultiPrediction,
    applyServerPredictions,
    isPageVisible,
    pruneRecentPredictions,
  ]);

  useEffect(() => () => {
    stopPathTimers();
    if (pinnedTimerRef.current) clearTimeout(pinnedTimerRef.current);
  }, [stopPathTimers]);

  return {
    packetPaths,
    betaPacketPaths,
    betaLowConfidencePaths,
    betaLowConfidenceSegments,
    betaCompletionPaths,
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
