import { useCallback, useEffect, useRef, useState } from 'react';
import type { AggregatedPacket, MeshNode } from './useNodes.js';
import type { NodeCoverage } from './useCoverage.js';
import { hasCoords, resolvePathWaypoints } from '../utils/pathing.js';
import { buildNearestPrefixContinuation, resolveBetaPath, type LinkMetrics, type PathLearningModel } from '../utils/betaPathing.js';
import type { Filters } from '../components/FilterPanel/FilterPanel.js';

const PATH_TTL = 5_000;

type UsePacketPathOverlayParams = {
  packets: AggregatedPacket[];
  nodes: Map<string, MeshNode>;
  coverage: NodeCoverage[];
  linkPairs: Set<string>;
  linkMetrics: Map<string, LinkMetrics>;
  learningModel: PathLearningModel | null;
  filters: Filters;
};

type UsePacketPathOverlayResult = {
  packetPath: [number, number][] | null;
  betaPacketPath: [number, number][] | null;
  betaLowConfidencePath: [number, number][] | null;
  betaCompletionPaths: [number, number][][];
  betaPathConfidence: number | null;
  betaPermutationCount: number | null;
  pathOpacity: number;
  pinnedPacketId: string | null;
  handlePacketPin: (packet: AggregatedPacket) => void;
};

function distKm(a: MeshNode, b: MeshNode): number {
  const midLat = ((a.lat! + b.lat!) / 2) * (Math.PI / 180);
  const dlat = (a.lat! - b.lat!) * 111;
  const dlon = (a.lon! - b.lon!) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

function buildFallbackPrefixPath(
  hopHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  nodes: Map<string, MeshNode>,
  forceIncludeSource = false,
): [number, number][] | null {
  const repeaters = Array.from(nodes.values()).filter(
    (n) => hasCoords(n) && (n.role === undefined || n.role === 2) && !n.name?.includes('🚫'),
  );

  const pickedNearRx: MeshNode[] = [];
  const visited = new Set<string>([rx.node_id]);
  let prev = rx;
  for (const h of [...hopHashes].reverse()) {
    const prefix = h.slice(0, 2).toUpperCase();
    const candidates = repeaters
      .filter((n) => !visited.has(n.node_id) && n.node_id.slice(0, 2).toUpperCase() === prefix)
      .sort((a, b) => distKm(a, prev) - distKm(b, prev));
    const chosen = candidates[0];
    if (!chosen) continue;
    pickedNearRx.push(chosen);
    visited.add(chosen.node_id);
    prev = chosen;
  }

  const hopsFarToNear = [...pickedNearRx].reverse();
  const pathNodes: MeshNode[] = [...(hasCoords(src) && forceIncludeSource ? [src] : []), ...hopsFarToNear, rx];
  // Avoid +1 visual hop inflation in low-confidence fallback mode.
  if (!forceIncludeSource && hasCoords(src) && pathNodes.length >= 2 && pathNodes[0]?.node_id === src.node_id) {
    pathNodes.shift();
  }
  if (pathNodes.length < 2) return null;
  return pathNodes.map((n) => [n.lat!, n.lon!]);
}

function splitResolvedAndAlternatives(
  result: ReturnType<typeof resolveBetaPath>,
  hopHashes: string[],
  srcNodeId: string | undefined,
  forceIncludeSource: boolean,
  threshold: number,
  nodes: Map<string, MeshNode>,
): { resolvedPath: [number, number][] | null; lowPath: [number, number][] | null; completionPaths: [number, number][][] } {
  if (!result) return { resolvedPath: null, lowPath: null, completionPaths: [] };
  const seg = result.segmentConfidence;
  const firstLow = seg.findIndex((v) => v < threshold);
  if (firstLow < 0) return { resolvedPath: result.path, lowPath: null, completionPaths: [] };

  // Receiver-side confident suffix (contiguous from rx backwards).
  let suffixStartEdge = seg.length;
  for (let i = seg.length - 1; i >= 0; i--) {
    if (seg[i]! >= threshold) suffixStartEdge = i;
    else break;
  }

  const purpleFromStartEdges = Math.max(0, firstLow);
  const purpleFromEndEdges = suffixStartEdge < seg.length ? (seg.length - suffixStartEdge) : 0;
  const preferReceiverSide = purpleFromEndEdges > purpleFromStartEdges;

  if (preferReceiverSide && suffixStartEdge < seg.length) {
    const resolvedCandidate = result.path.slice(suffixStartEdge);
    const resolvedPath = resolvedCandidate.length >= 2 ? resolvedCandidate : null;
    const lowCandidate = result.path.slice(0, suffixStartEdge + 1);
    const lowPath = lowCandidate.length >= 2 ? lowCandidate : null;
    return { resolvedPath, lowPath, completionPaths: [] };
  }

  const resolvedCandidate = result.path.slice(0, firstLow + 1);
  const resolvedPath = resolvedCandidate.length >= 2 ? resolvedCandidate : null;
  const lowCandidate = result.path.slice(Math.max(0, firstLow));
  const lowPath = lowCandidate.length >= 2 ? lowCandidate : null;
  const stepsRemaining = result.path.length - 1 - firstLow;
  const startNodeId = result.nodeIds[firstLow];
  const endNodeId = result.nodeIds[result.nodeIds.length - 1];
  if (!startNodeId || !endNodeId || stepsRemaining <= 0) {
    return { resolvedPath, lowPath, completionPaths: [] };
  }

  const hasSource = Boolean(srcNodeId && result.nodeIds[0] === srcNodeId);
  const confidentRelayCount = Math.max(0, Math.min(
    hopHashes.length,
    (resolvedPath?.length ?? 0) - (hasSource ? 1 : 0),
  ));
  const remainingPrefixes = hopHashes.slice(confidentRelayCount);
  const nearestPrefixPath = buildNearestPrefixContinuation(
    startNodeId,
    remainingPrefixes,
    endNodeId,
    nodes,
    { dropStartIfNodeId: forceIncludeSource ? undefined : srcNodeId },
  );
  if (nearestPrefixPath && nearestPrefixPath.length >= 2) {
    return { resolvedPath, lowPath: nearestPrefixPath, completionPaths: [] };
  }
  return { resolvedPath, lowPath, completionPaths: [] };
}

export function usePacketPathOverlay({
  packets,
  nodes,
  coverage,
  linkPairs,
  linkMetrics,
  learningModel,
  filters,
}: UsePacketPathOverlayParams): UsePacketPathOverlayResult {
  const [packetPath, setPacketPath] = useState<[number, number][] | null>(null);
  const [betaPacketPath, setBetaPacketPath] = useState<[number, number][] | null>(null);
  const [betaLowConfidencePath, setBetaLowConfidencePath] = useState<[number, number][] | null>(null);
  const [betaCompletionPaths, setBetaCompletionPaths] = useState<[number, number][][]>([]);
  const [betaPathConfidence, setBetaPathConfidence] = useState<number | null>(null);
  const [betaPermutationCount, setBetaPermutationCount] = useState<number | null>(null);
  const [pinnedPacketId, setPinnedPacketId] = useState<string | null>(null);
  const [pathOpacity, setPathOpacity] = useState(0.75);

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeRef = useRef<number | null>(null);
  const recentPredictionsRef = useRef<Map<string, { path: [number, number][]; lowPath: [number, number][] | null; completionPaths: [number, number][][]; confidence: number | null; ts: number }>>(new Map());

  const stopPathTimers = useCallback(() => {
    if (pathTimerRef.current) {
      clearTimeout(pathTimerRef.current);
      pathTimerRef.current = null;
    }
    if (pathFadeRef.current !== null) {
      cancelAnimationFrame(pathFadeRef.current);
      pathFadeRef.current = null;
    }
  }, []);

  const clearPathState = useCallback(() => {
    setPacketPath(null);
    setBetaPacketPath(null);
    setBetaLowConfidencePath(null);
    setBetaCompletionPaths([]);
    setBetaPathConfidence(null);
    setBetaPermutationCount(null);
    setPathOpacity(0.75);
  }, []);

  const latestId = packets[0]?.id;
  useEffect(() => {
    if (pinnedPacketId !== null) return;
    stopPathTimers();

    const latest = packets[0];
    const rx = latest?.rxNodeId ? nodes.get(latest.rxNodeId) : undefined;

    if (filters.packetPaths && latest?.rxNodeId && (latest.path?.length || latest.srcNodeId) && hasCoords(rx)) {
      const src = latest.srcNodeId ? (nodes.get(latest.srcNodeId) ?? null) : null;
      const srcWithPos = hasCoords(src) ? src : null;
      const waypoints = latest.path?.length
        ? resolvePathWaypoints(latest.path, srcWithPos, rx, nodes)
        : [[srcWithPos!.lat!, srcWithPos!.lon!], [rx.lat, rx.lon]] as [number, number][];
      setPacketPath(waypoints.length >= 2 ? waypoints : null);
    } else {
      setPacketPath(null);
    }

    if (filters.betaPaths && latest?.rxNodeId && latest.path?.length && hasCoords(rx)) {
      const src = latest.srcNodeId ? (nodes.get(latest.srcNodeId) ?? null) : null;
      const forceIncludeSource = latest.packetType === 4; // Advert packets should stay anchored to their source repeater.
      const hops = latest.hopCount != null ? latest.path.slice(0, latest.hopCount) : latest.path;
      const pairKey = `${src?.node_id ?? 'unknown'}>${rx.node_id}`;
      const result = resolveBetaPath(
        hops,
        hasCoords(src) ? src : null,
        rx,
        nodes,
        coverage,
        linkPairs,
        linkMetrics,
        learningModel,
        { forceIncludeSource },
      );
      if (result) {
        const split = splitResolvedAndAlternatives(result, hops, src?.node_id, forceIncludeSource, filters.betaPathThreshold, nodes);
        recentPredictionsRef.current.set(pairKey, {
          path: split.resolvedPath ?? [],
          lowPath: split.lowPath,
          completionPaths: split.completionPaths,
          confidence: result.confidence,
          ts: Date.now(),
        });
        setBetaPacketPath(split.resolvedPath);
        setBetaLowConfidencePath(split.lowPath);
        setBetaCompletionPaths(split.completionPaths);
        setBetaPathConfidence(result.confidence);
        setBetaPermutationCount((split.lowPath ? 1 : 0) + split.completionPaths.length);
      } else {
        const fallback = buildFallbackPrefixPath(hops, hasCoords(src) ? src : null, rx, nodes, forceIncludeSource);
        if (fallback) {
          setBetaPacketPath(null);
          setBetaLowConfidencePath(fallback);
          setBetaCompletionPaths([]);
          setBetaPathConfidence(null);
          setBetaPermutationCount(1);
          recentPredictionsRef.current.set(pairKey, {
            path: [],
            lowPath: fallback,
            completionPaths: [],
            confidence: null,
            ts: Date.now(),
          });
        } else {
          const recent = recentPredictionsRef.current.get(pairKey);
          if (recent && Date.now() - recent.ts < 45_000) {
            setBetaPacketPath(recent.path.length > 1 ? recent.path : null);
            setBetaLowConfidencePath(recent.lowPath);
            setBetaCompletionPaths(recent.completionPaths);
            setBetaPathConfidence(recent.confidence);
            setBetaPermutationCount((recent.lowPath ? 1 : 0) + recent.completionPaths.length);
          } else {
            setBetaPacketPath(null);
            setBetaLowConfidencePath(null);
            setBetaCompletionPaths([]);
            setBetaPathConfidence(null);
            setBetaPermutationCount(null);
          }
        }
      }
    } else {
      setBetaPacketPath(null);
      setBetaLowConfidencePath(null);
      setBetaCompletionPaths([]);
      setBetaPathConfidence(null);
      setBetaPermutationCount(null);
    }

    if (!filters.packetPaths && !filters.betaPaths) { setPathOpacity(0.75); return; }
    if (!latest) { setPathOpacity(0.75); return; }

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
  }, [latestId, filters.packetPaths, filters.betaPaths, pinnedPacketId, linkMetrics, learningModel, filters.betaPathThreshold]);

  const handlePacketPin = useCallback((packet: AggregatedPacket) => {
    if (pinnedPacketId === packet.id) {
      setPinnedPacketId(null);
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

    const rx = packet.rxNodeId ? nodes.get(packet.rxNodeId) : undefined;

    setPacketPath(null);

    if (packet.rxNodeId && packet.path?.length && hasCoords(rx)) {
      const src = packet.srcNodeId ? (nodes.get(packet.srcNodeId) ?? null) : null;
      const forceIncludeSource = packet.packetType === 4; // Advert packets should stay anchored to their source repeater.
      const hops = packet.hopCount != null ? packet.path.slice(0, packet.hopCount) : packet.path;
      const pairKey = `${src?.node_id ?? 'unknown'}>${rx.node_id}`;
      const result = resolveBetaPath(
        hops,
        hasCoords(src) ? src : null,
        rx,
        nodes,
        coverage,
        linkPairs,
        linkMetrics,
        learningModel,
        { forceIncludeSource },
      );
      if (result) {
        const split = splitResolvedAndAlternatives(result, hops, src?.node_id, forceIncludeSource, filters.betaPathThreshold, nodes);
        recentPredictionsRef.current.set(pairKey, {
          path: split.resolvedPath ?? [],
          lowPath: split.lowPath,
          completionPaths: split.completionPaths,
          confidence: result.confidence,
          ts: Date.now(),
        });
        setBetaPacketPath(split.resolvedPath);
        setBetaLowConfidencePath(split.lowPath);
        setBetaCompletionPaths(split.completionPaths);
        setBetaPathConfidence(result.confidence);
        setBetaPermutationCount((split.lowPath ? 1 : 0) + split.completionPaths.length);
      } else {
        const fallback = buildFallbackPrefixPath(hops, hasCoords(src) ? src : null, rx, nodes, forceIncludeSource);
        if (fallback) {
          setBetaPacketPath(null);
          setBetaLowConfidencePath(fallback);
          setBetaCompletionPaths([]);
          setBetaPathConfidence(null);
          setBetaPermutationCount(1);
          recentPredictionsRef.current.set(pairKey, {
            path: [],
            lowPath: fallback,
            completionPaths: [],
            confidence: null,
            ts: Date.now(),
          });
        } else {
          const recent = recentPredictionsRef.current.get(pairKey);
          if (recent && Date.now() - recent.ts < 45_000) {
            setBetaPacketPath(recent.path.length > 1 ? recent.path : null);
            setBetaLowConfidencePath(recent.lowPath);
            setBetaCompletionPaths(recent.completionPaths);
            setBetaPathConfidence(recent.confidence);
            setBetaPermutationCount((recent.lowPath ? 1 : 0) + recent.completionPaths.length);
          } else {
            setBetaPacketPath(null);
            setBetaLowConfidencePath(null);
            setBetaCompletionPaths([]);
            setBetaPathConfidence(null);
            setBetaPermutationCount(null);
          }
        }
      }
    } else {
      setBetaPacketPath(null);
      setBetaLowConfidencePath(null);
      setBetaCompletionPaths([]);
      setBetaPathConfidence(null);
      setBetaPermutationCount(null);
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
          pinnedTimerRef.current = null;
        }
      };
      pathFadeRef.current = requestAnimationFrame(animate);
    }, 30_000);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pinnedPacketId, nodes, coverage, linkPairs, linkMetrics, learningModel, filters.betaPathThreshold, stopPathTimers, clearPathState]);

  useEffect(() => () => {
    stopPathTimers();
    if (pinnedTimerRef.current) clearTimeout(pinnedTimerRef.current);
  }, [stopPathTimers]);

  return {
    packetPath,
    betaPacketPath,
    betaLowConfidencePath,
    betaCompletionPaths,
    betaPathConfidence,
    betaPermutationCount,
    pathOpacity,
    pinnedPacketId,
    handlePacketPin,
  };
}
