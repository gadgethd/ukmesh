import { useCallback, useEffect, useRef, useState } from 'react';
import type { AggregatedPacket, MeshNode } from './useNodes.js';
import type { NodeCoverage } from './useCoverage.js';
import { hasCoords, resolvePathWaypoints } from '../utils/pathing.js';
import { resolveBetaPath, type LinkMetrics, type PathLearningModel } from '../utils/betaPathing.js';
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
  pathOpacity: number;
  pinnedPacketId: string | null;
  handlePacketPin: (packet: AggregatedPacket) => void;
};

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
  const [pinnedPacketId, setPinnedPacketId] = useState<string | null>(null);
  const [pathOpacity, setPathOpacity] = useState(0.75);

  const pinnedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeRef = useRef<number | null>(null);
  const recentPredictionsRef = useRef<Map<string, { path: [number, number][]; ts: number }>>(new Map());

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
      );
      if (result && result.confidence >= filters.betaPathThreshold) {
        recentPredictionsRef.current.set(pairKey, { path: result.path, ts: Date.now() });
        setBetaPacketPath(result.path);
      } else {
        const recent = recentPredictionsRef.current.get(pairKey);
        if (recent && Date.now() - recent.ts < 45_000) {
          setBetaPacketPath(recent.path);
        } else {
          setBetaPacketPath(null);
        }
      }
    } else {
      setBetaPacketPath(null);
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
      );
      if (result && result.confidence >= filters.betaPathThreshold) {
        recentPredictionsRef.current.set(pairKey, { path: result.path, ts: Date.now() });
        setBetaPacketPath(result.path);
      } else {
        const recent = recentPredictionsRef.current.get(pairKey);
        if (recent && Date.now() - recent.ts < 45_000) {
          setBetaPacketPath(recent.path);
        } else {
          setBetaPacketPath(null);
        }
      }
    } else {
      setBetaPacketPath(null);
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
    pathOpacity,
    pinnedPacketId,
    handlePacketPin,
  };
}
