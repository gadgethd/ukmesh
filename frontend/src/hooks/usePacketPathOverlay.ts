import { useCallback, useEffect, useRef, useState } from 'react';
import type { AggregatedPacket, MeshNode } from './useNodes.js';
import { withScopeParams, uncachedEndpoint } from '../utils/api.js';
import type { Filters } from '../components/FilterPanel/FilterPanel.js';
import { hasCoords } from '../utils/pathing.js';
import {
  aggregateServerPredictions,
  packetObserverIds,
  type AggregatedPredictionState,
  type MultiObserverBetaResponse,
  type PathSegment,
  type ServerBetaResponse,
} from './packetPathOverlayUtils.js';

const PATH_TTL = 5_000;
const PREDICTION_CACHE_TTL_MS = 120_000;
const MAX_PREDICTION_CACHE = 1200;

type UsePacketPathOverlayParams = {
  packets: AggregatedPacket[];
  nodes: Map<string, MeshNode>;
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
  pathOpacity: number;
  pinnedPacketId: string | null;
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
  return `multi|${network ?? 'all'}|${observerIds.sort().join(',')}|${packetHash}`;
}

export function usePacketPathOverlay({
  packets,
  nodes,
  filters,
  network,
  observer,
}: UsePacketPathOverlayParams): UsePacketPathOverlayResult {
  const [packetPaths, setPacketPaths] = useState<[number, number][][]>([]);
  const [betaPacketPaths, setBetaPacketPaths] = useState<[number, number][][]>([]);
  const [betaLowConfidencePaths, setBetaLowConfidencePaths] = useState<[number, number][][]>([]);
  const [betaLowConfidenceSegments, setBetaLowConfidenceSegments] = useState<PathSegment[]>([]);
  const [betaCompletionPaths, setBetaCompletionPaths] = useState<[number, number][][]>([]);
  const [betaPathConfidence, setBetaPathConfidence] = useState<number | null>(null);
  const [betaPermutationCount, setBetaPermutationCount] = useState<number | null>(null);
  const [betaRemainingHops, setBetaRemainingHops] = useState<number | null>(null);
  const [pinnedPacketId, setPinnedPacketId] = useState<string | null>(null);
  const [pinnedPacketSnapshot, setPinnedPacketSnapshot] = useState<AggregatedPacket | null>(null);
  const [pathOpacity, setPathOpacity] = useState(0.75);

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeRef = useRef<number | null>(null);
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
    if (pathFadeRef.current !== null) {
      cancelAnimationFrame(pathFadeRef.current);
      pathFadeRef.current = null;
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
    setPathOpacity(0.75);
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
      const recent = recentPredictionsRef.current.get(packetHash);
      if (!recent || Date.now() - recent.ts > 45_000) {
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
  }, []);

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

  const shouldCollapseAdvertObserverPartials = useCallback((packet: AggregatedPacket | undefined): boolean => {
    if (!packet || packet.packetType !== 4 || !packet.srcNodeId) return false;
    const src = nodes.get(packet.srcNodeId);
    return !hasCoords(src);
  }, [nodes]);

  const resolvePrediction = useCallback((packetHash: string, networkName?: string, observerId?: string): Promise<ServerBetaResponse | null> => {
    prunePredictionCache();
    const key = cacheKey(packetHash, networkName, observerId);
    const cached = predictionCacheRef.current.get(key);
    if (cached && Date.now() - cached.ts <= PREDICTION_CACHE_TTL_MS) {
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

  const resolveMultiPrediction = useCallback((packetHash: string, observerIds: string[], networkName?: string): Promise<ServerBetaResponse[]> => {
    prunePredictionCache();
    const key = multiCacheKey(packetHash, observerIds, networkName);

    const cached = multiPredictionCacheRef.current.get(key);
    if (cached && Date.now() - cached.ts <= PREDICTION_CACHE_TTL_MS) {
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

  const latestId = packets[0]?.id;
  useEffect(() => {
    if (pinnedPacketId !== null) return;
    stopPathTimers();

    const latest = packets[0];
    const observerIds = getPacketObserverIds(latest);
    setPacketPaths([]);

    if (filters.betaPaths && latest?.packetHash && latest.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      const resolveFn = observerIds.length > 1
        ? resolveMultiPrediction(latest.packetHash, observerIds, network)
        : Promise.all(observerIds.map((observerId) => resolvePrediction(latest.packetHash, network, observerId)));
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

    if (!filters.betaPaths) {
      setPathOpacity(0.75);
      return;
    }
    if (!latest) {
      setPathOpacity(0.75);
      return;
    }

    setPathOpacity(0.75);
    pathTimerRef.current = setTimeout(() => {
      const FADE_MS = 1_000;
      const startTime = performance.now();
      const animate = (now: number) => {
        const t = Math.min(1, (now - startTime) / FADE_MS);
        setPathOpacity(0.75 * (1 - t));
        if (t < 1) {
          pathFadeRef.current = requestAnimationFrame(animate);
        } else {
          pathFadeRef.current = null;
          clearPathState();
        }
      };
      pathFadeRef.current = requestAnimationFrame(animate);
    }, PATH_TTL - 1_000);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestId, filters.betaPaths, pinnedPacketId, network, observer, packets, getPacketObserverIds, resolvePrediction, resolveMultiPrediction, stopPathTimers, clearPathState, applyServerPredictions]);

  const handlePacketPin = useCallback((packet: AggregatedPacket) => {
    if (pinnedPacketId === packet.id) {
      setPinnedPacketId(null);
      setPinnedPacketSnapshot(null);
      pinnedOverlayKeyRef.current = '';
      if (pinnedTimerRef.current) {
        clearTimeout(pinnedTimerRef.current);
        pinnedTimerRef.current = null;
      }
      stopPathTimers();
      clearPathState();
      return;
    }

    stopPathTimers();
    if (pinnedTimerRef.current) {
      clearTimeout(pinnedTimerRef.current);
      pinnedTimerRef.current = null;
    }

    setPathOpacity(0.75);
    setPinnedPacketId(packet.id);
    setPinnedPacketSnapshot(packet);

    pinnedTimerRef.current = setTimeout(() => {
      const FADE_MS = 1_000;
      const startTime = performance.now();
      const animate = (now: number) => {
        const t = Math.min(1, (now - startTime) / FADE_MS);
        setPathOpacity(0.75 * (1 - t));
        if (t < 1) {
          pathFadeRef.current = requestAnimationFrame(animate);
        } else {
          pathFadeRef.current = null;
          clearPathState();
          setPinnedPacketId(null);
          setPinnedPacketSnapshot(null);
          pinnedOverlayKeyRef.current = '';
          pinnedTimerRef.current = null;
        }
      };
      pathFadeRef.current = requestAnimationFrame(animate);
    }, 30_000);
  }, [pinnedPacketId, stopPathTimers, clearPathState]);

  useEffect(() => {
    if (pinnedPacketId === null) {
      pinnedOverlayKeyRef.current = '';
      return;
    }

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
    ].join('|');

    if (overlayKey === pinnedOverlayKeyRef.current) return;
    pinnedOverlayKeyRef.current = overlayKey;

    setPacketPaths([]);

    if (filters.betaPaths && pinnedPacket.packetHash && pinnedPacket.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      const resolveFn = observerIds.length > 1
        ? resolveMultiPrediction(pinnedPacket.packetHash!, observerIds, network)
        : Promise.all(observerIds.map((observerId) => resolvePrediction(pinnedPacket.packetHash!, network, observerId)));
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
    shouldCollapseAdvertObserverPartials,
    resolvePrediction,
    resolveMultiPrediction,
    applyServerPredictions,
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
    pathOpacity,
    pinnedPacketId,
    handlePacketPin,
  };
}
