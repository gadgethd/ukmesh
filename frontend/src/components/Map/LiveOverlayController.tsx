import React, { useEffect, useMemo } from 'react';
import type maplibregl from 'maplibre-gl';
import { DeckGLOverlay } from './DeckGLOverlay.js';
import { useNodeMap } from '../../hooks/useNodes.js';
import { usePacketPathOverlay } from '../../hooks/usePacketPathOverlay.js';
import type { Filters } from '../FilterPanel/FilterPanel.js';
import { buildHiddenCoordMask, hasCoords } from '../../utils/pathing.js';
import { useOverlayStore } from '../../store/overlayStore.js';

type PacketHistorySegment = {
  positions: [[number, number], [number, number]];
  count: number;
};

type LiveOverlayControllerProps = {
  map: maplibregl.Map | null;
  filters: Filters;
  network?: string;
  observer?: string;
  packetHistorySegments: PacketHistorySegment[];
};

export const LiveOverlayController: React.FC<LiveOverlayControllerProps> = ({
  map,
  filters,
  network,
  observer,
  packetHistorySegments,
}) => {
  const nodes = useNodeMap();
  const hiddenCoordMask = useMemo(() => buildHiddenCoordMask(nodes.values()), [nodes]);
  const setPathNodeIds = useOverlayStore((state) => state.setPathNodeIds);
  const setBetaMetrics = useOverlayStore((state) => state.setBetaMetrics);

  const {
    packetPaths,
    betaPacketPaths,
    betaLowConfidenceSegments,
    betaCompletionPaths,
    betaPathConfidence,
    betaPermutationCount,
    betaRemainingHops,
    pathFadingOut,
    pinnedPacketId,
    activePacketSnapshot,
  } = usePacketPathOverlay({
    filters,
    network,
    observer,
  });

  const renderedPaths = useMemo<[number, number][][]>(() => (
    betaPacketPaths.length > 0 ? betaPacketPaths : packetPaths
  ), [betaPacketPaths, packetPaths]);
  const showPathOnly = filters.betaPaths || pinnedPacketId !== null;

  const pathPointIndex = useMemo(() => {
    const index = new Map<string, Set<string>>();
    const pointKey = (lat: number, lon: number) => `${lat.toFixed(5)},${lon.toFixed(5)}`;
    for (const node of nodes.values()) {
      if (!hasCoords(node)) continue;
      const key = pointKey(node.lat, node.lon);
      const existing = index.get(key);
      if (existing) existing.add(node.node_id.toLowerCase());
      else index.set(key, new Set([node.node_id.toLowerCase()]));
    }
    return index;
  }, [nodes]);

  const pathNodeIdsPrevRef = React.useRef<Set<string> | null>(null);
  const pathNodeIds = useMemo<Set<string> | null>(() => {
    if (!showPathOnly) {
      if (pathNodeIdsPrevRef.current !== null) pathNodeIdsPrevRef.current = null;
      return null;
    }
    if (!activePacketSnapshot) {
      const empty = new Set<string>();
      pathNodeIdsPrevRef.current = empty;
      return empty;
    }
    const pointKey = (lat: number, lon: number) => `${lat.toFixed(5)},${lon.toFixed(5)}`;
    const ids = new Set<string>();

    const addPoint = (point: [number, number] | null | undefined) => {
      if (!point) return;
      const matches = pathPointIndex.get(pointKey(point[0], point[1]));
      if (!matches) return;
      for (const id of matches) ids.add(id);
    };

    for (const path of renderedPaths) {
      for (const point of path) addPoint(point);
    }
    for (const [a, b] of betaLowConfidenceSegments) {
      addPoint(a);
      addPoint(b);
    }
    for (const path of betaCompletionPaths) {
      for (const point of path) addPoint(point);
    }

    const result = ids;
    const prev = pathNodeIdsPrevRef.current;
    if (prev && result && prev.size === result.size && [...result].every((id) => prev.has(id))) {
      return prev;
    }
    pathNodeIdsPrevRef.current = result;
    return result;
  }, [showPathOnly, activePacketSnapshot, pathPointIndex, renderedPaths, betaLowConfidenceSegments, betaCompletionPaths]);

  useEffect(() => {
    setPathNodeIds(pathNodeIds);
  }, [pathNodeIds, setPathNodeIds]);

  useEffect(() => {
    setBetaMetrics({
      betaPathConfidence,
      betaPermutationCount,
      betaRemainingHops,
    });
  }, [betaPathConfidence, betaPermutationCount, betaRemainingHops, setBetaMetrics]);

  useEffect(() => () => {
    setPathNodeIds(null);
    setBetaMetrics({
      betaPathConfidence: null,
      betaPermutationCount: null,
      betaRemainingHops: null,
    });
  }, [setPathNodeIds, setBetaMetrics]);

  return (
    <DeckGLOverlay
      map={map}
      arcs={[]}
      showArcs={filters.livePackets}
      packetHistorySegments={packetHistorySegments}
      showPacketHistory={filters.packetHistory}
      betaPaths={renderedPaths}
      betaLowSegments={betaLowConfidenceSegments}
      betaCompletionPaths={betaCompletionPaths}
      showBetaPaths={filters.betaPaths || pinnedPacketId !== null}
      pathFadingOut={pathFadingOut}
      hiddenCoordMask={hiddenCoordMask}
    />
  );
};
