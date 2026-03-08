import { useCallback, useEffect, useRef, useState } from 'react';
import type { AggregatedPacket, MeshNode } from './useNodes.js';
import { hasCoords, resolvePathWaypoints } from '../utils/pathing.js';
import { withScopeParams, uncachedEndpoint } from '../utils/api.js';
import type { Filters } from '../components/FilterPanel/FilterPanel.js';

const PATH_TTL = 5_000;
const PREDICTION_CACHE_TTL_MS = 120_000;
const MAX_PREDICTION_CACHE = 1200;
type PathSegment = [[number, number], [number, number]];

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

type ServerBetaResponse = {
  ok: boolean;
  packetHash: string;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  permutationCount: number;
  remainingHops: number | null;
  purplePath: [number, number][] | null;
  extraPurplePaths: [number, number][][];
  redPath: [number, number][] | null;
  redSegments: PathSegment[];
  completionPaths: [number, number][][];
};

function segmentizePath(path: [number, number][] | null): PathSegment[] {
  if (!path || path.length < 2) return [];
  const segments: PathSegment[] = [];
  for (let i = 0; i < path.length - 1; i++) {
    const a = path[i];
    const b = path[i + 1];
    if (!a || !b) continue;
    segments.push([a, b]);
  }
  return segments;
}

async function fetchServerBeta(packetHash: string, network?: string, observer?: string, signal?: AbortSignal): Promise<ServerBetaResponse | null> {
  const endpoint = withScopeParams(`/api/path-beta/resolve?hash=${encodeURIComponent(packetHash)}`, { network, observer });
  const response = await fetch(uncachedEndpoint(endpoint), { cache: 'no-store', signal });
  if (!response.ok) return null;
  return response.json() as Promise<ServerBetaResponse>;
}

function cacheKey(packetHash: string, network?: string, observer?: string): string {
  return `${network ?? 'all'}|${observer ?? 'all'}|${packetHash}`;
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
  const [pathOpacity, setPathOpacity] = useState(0.75);

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeRef = useRef<number | null>(null);
  const betaReqRef = useRef<AbortController | null>(null);
  const predictionCacheRef = useRef<Map<string, { prediction: ServerBetaResponse | null; ts: number }>>(new Map());
  const inFlightRef = useRef<Map<string, Promise<ServerBetaResponse | null>>>(new Map());
  const activeReqSeqRef = useRef(0);
  const pinnedOverlayKeyRef = useRef('');
  const recentPredictionsRef = useRef<Map<string, {
    purplePaths: [number, number][][];
    redPaths: [number, number][][];
    redSegments: PathSegment[];
    completionPaths: [number, number][][];
    confidence: number | null;
    permutations: number | null;
    remainingHops: number | null;
    ts: number;
  }>>(new Map());

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

  const applyServerPredictions = useCallback((packetHash: string, predictions: Array<ServerBetaResponse | null>) => {
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
      setBetaCompletionPaths(recent.completionPaths);
      setBetaPathConfidence(recent.confidence);
      setBetaPermutationCount(recent.permutations);
      setBetaRemainingHops(recent.remainingHops);
      return;
    }

    const purplePaths = validPredictions.flatMap((prediction) => {
      const paths: [number, number][][] = [];
      if (prediction.purplePath && prediction.purplePath.length >= 2) paths.push(prediction.purplePath);
      for (const path of prediction.extraPurplePaths ?? []) {
        if (path.length >= 2) paths.push(path);
      }
      return paths;
    });
    const allRedPaths = validPredictions.flatMap((prediction) => (
      prediction.redPath && prediction.redPath.length >= 2 ? [prediction.redPath] : []
    ));
    const allRedSegments = validPredictions.flatMap((prediction) => (
      prediction.redSegments?.length ? prediction.redSegments : segmentizePath(prediction.redPath && prediction.redPath.length >= 2 ? prediction.redPath : null)
    ));

    // Build a set of purple segment keys so red segments covering the same edge are suppressed
    const purpleSegmentKeys = new Set<string>();
    for (const path of purplePaths) {
      for (let i = 0; i < path.length - 1; i++) {
        const a = path[i]!; const b = path[i + 1]!;
        purpleSegmentKeys.add(`${a[0]},${a[1]}|${b[0]},${b[1]}`);
        purpleSegmentKeys.add(`${b[0]},${b[1]}|${a[0]},${a[1]}`);
      }
    }
    const segKey = (a: [number, number], b: [number, number]) =>
      `${a[0]},${a[1]}|${b[0]},${b[1]}`;
    // Deduplicate red segments across observers (same edge drawn by multiple observers) and
    // suppress any segment already covered by a purple path.
    const seenRedKeys = new Set<string>();
    const redSegments = allRedSegments.filter(([a, b]) => {
      if (purpleSegmentKeys.has(segKey(a, b)) || purpleSegmentKeys.has(segKey(b, a))) return false;
      const key = segKey(a, b);
      const revKey = segKey(b, a);
      if (seenRedKeys.has(key) || seenRedKeys.has(revKey)) return false;
      seenRedKeys.add(key);
      return true;
    });
    const redPaths = allRedPaths.filter((path) =>
      path.some((_, i) => {
        if (i >= path.length - 1) return false;
        const a = path[i]!; const b = path[i + 1]!;
        return !purpleSegmentKeys.has(segKey(a, b)) && !purpleSegmentKeys.has(segKey(b, a));
      })
    );

    const completionPaths = validPredictions.flatMap((prediction) => prediction.completionPaths ?? []);
    const permutations = validPredictions.reduce((sum, prediction) => {
      const fallbackCount = (prediction.redPath ? 1 : 0) + (prediction.completionPaths?.length ?? 0);
      return sum + (Number.isFinite(prediction.permutationCount) ? prediction.permutationCount : fallbackCount);
    }, 0);
    const bestConfidence = validPredictions.reduce<number | null>((best, prediction) => {
      if (prediction.confidence == null) return best;
      return best == null ? prediction.confidence : Math.max(best, prediction.confidence);
    }, null);
    const remainingHops = validPredictions.reduce<number | null>((best, prediction) => {
      if (prediction.remainingHops == null) return best;
      return best == null ? prediction.remainingHops : Math.max(best, prediction.remainingHops);
    }, null);

    setBetaPacketPaths(purplePaths);
    setBetaLowConfidencePaths(redPaths);
    setBetaLowConfidenceSegments(redSegments);
    setBetaCompletionPaths(completionPaths);
    setBetaPathConfidence(bestConfidence);
    setBetaPermutationCount(permutations);
    setBetaRemainingHops(remainingHops);

    recentPredictionsRef.current.set(packetHash, {
      purplePaths,
      redPaths,
      redSegments,
      completionPaths,
      confidence: bestConfidence,
      permutations,
      remainingHops,
      ts: Date.now(),
    });
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

  const packetObserverIds = useCallback((packet: AggregatedPacket | undefined): string[] => {
    if (!packet) return [];
    return Array.from(new Set([
      ...(packet.observerIds ?? []),
      ...(packet.rxNodeId ? [packet.rxNodeId] : []),
    ]));
  }, []);

  const buildRegularPacketPaths = useCallback((packet: AggregatedPacket | undefined, observerIds: string[]): [number, number][][] => {
    if (!packet || observerIds.length < 1 || (!packet.path?.length && !packet.srcNodeId)) return [];
    const src = packet.srcNodeId ? (nodes.get(packet.srcNodeId) ?? null) : null;
    const srcWithPos = hasCoords(src) ? src : null;
    return observerIds.flatMap((observerId) => {
      const rx = nodes.get(observerId);
      if (!hasCoords(rx)) return [];
      const waypoints = packet.path?.length
        ? resolvePathWaypoints(packet.path, srcWithPos, rx, nodes)
        : (srcWithPos ? [[srcWithPos.lat, srcWithPos.lon], [rx.lat, rx.lon]] as [number, number][] : []);
      return waypoints.length >= 2 ? [waypoints] : [];
    });
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

  const latestId = packets[0]?.id;
  useEffect(() => {
    if (pinnedPacketId !== null) return;
    stopPathTimers();

    const latest = packets[0];
    const observerIds = packetObserverIds(latest);

    if (filters.packetPaths && observerIds.length > 0 && latest && (latest.path?.length || latest.srcNodeId)) {
      setPacketPaths(buildRegularPacketPaths(latest, observerIds));
    } else {
      setPacketPaths([]);
    }

    if (filters.betaPaths && latest?.packetHash && latest.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      void Promise.all(observerIds.map((observerId) => resolvePrediction(latest.packetHash, network, observerId)))
        .then((predictions) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(latest.packetHash, predictions);
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(latest.packetHash, []);
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

    if (!filters.packetPaths && !filters.betaPaths) {
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
  }, [latestId, filters.packetPaths, filters.betaPaths, pinnedPacketId, network, observer, packets, packetObserverIds, resolvePrediction, stopPathTimers, clearPathState, applyServerPredictions, buildRegularPacketPaths]);

  const handlePacketPin = useCallback((packet: AggregatedPacket) => {
    if (pinnedPacketId === packet.id) {
      setPinnedPacketId(null);
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

    const pinnedPacket = packets.find((packet) => packet.id === pinnedPacketId);
    if (!pinnedPacket) return;

    const observerIds = packetObserverIds(pinnedPacket);
    const overlayKey = [
      pinnedPacket.id,
      pinnedPacket.packetHash ?? '',
      pinnedPacket.srcNodeId ?? '',
      pinnedPacket.path?.join(',') ?? '',
      observerIds.join(','),
      filters.packetPaths ? 'paths-on' : 'paths-off',
      filters.betaPaths ? 'beta-on' : 'beta-off',
      network ?? 'all',
      observer ?? 'all',
    ].join('|');

    if (overlayKey === pinnedOverlayKeyRef.current) return;
    pinnedOverlayKeyRef.current = overlayKey;

    if (filters.packetPaths) {
      setPacketPaths(buildRegularPacketPaths(pinnedPacket, observerIds));
    } else {
      setPacketPaths([]);
    }

    if (filters.betaPaths && pinnedPacket.packetHash && pinnedPacket.path?.length && observerIds.length > 0) {
      const reqSeq = ++activeReqSeqRef.current;
      void Promise.all(observerIds.map((observerId) => resolvePrediction(pinnedPacket.packetHash!, network, observerId)))
        .then((predictions) => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(pinnedPacket.packetHash!, predictions);
        })
        .catch(() => {
          if (reqSeq !== activeReqSeqRef.current) return;
          applyServerPredictions(pinnedPacket.packetHash!, []);
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
    filters.packetPaths,
    filters.betaPaths,
    network,
    observer,
    packetObserverIds,
    buildRegularPacketPaths,
    resolvePrediction,
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
