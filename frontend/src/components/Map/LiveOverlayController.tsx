import React, { useEffect, useMemo } from 'react';
import type maplibregl from 'maplibre-gl';
import { DeckGLOverlay } from './DeckGLOverlay.js';
import { AnimatedPathOverlay, type AerialPath } from './AnimatedPathOverlay.js';
import { useArcs, useNodeMap } from '../../hooks/useNodes.js';
import { usePacketPathOverlay } from '../../hooks/usePacketPathOverlay.js';
import { packetObserverIds } from '../../hooks/packetPathOverlayUtils.js';
import type { ResolvedPathRoute } from '../../hooks/packetPathOverlayUtils.js';
import type { Filters } from '../FilterPanel/FilterPanel.js';
import { buildHiddenCoordMask, hasCoords, maskNodePoint, maskPoint } from '../../utils/pathing.js';
import { useOverlayStore } from '../../store/overlayStore.js';

type PacketHistorySegment = {
  positions: [[number, number], [number, number]];
  count: number;
};

function positionKey(lat: number, lon: number): string {
  return `${Math.round(lat * 1_000_000) / 1_000_000},${Math.round(lon * 1_000_000) / 1_000_000}`;
}

type LiveOverlayControllerProps = {
  map: maplibregl.Map | null;
  filters: Filters;
  network?: string;
  observer?: string;
  packetHistorySegments: PacketHistorySegment[];
  packetArcsEnabled: boolean;
  heatmapEnabled: boolean;
};

export function buildResolvedAerialPaths(
  packetHash: string | null,
  routes: ResolvedPathRoute[],
  hiddenCoordMask: ReturnType<typeof buildHiddenCoordMask>,
): AerialPath[] {
  if (!packetHash) return [];
  return routes.map((route) => ({
    id: `main-live-path:${packetHash}:resolved`,
    packetHash,
    confidence: route.confidence,
    nodes: route.nodes.map((node) => {
      const [lat, lon] = maskPoint([node.lat, node.lon], hiddenCoordMask);
      return {
        position: [lon, lat] as [number, number],
        nodeId: node.nodeId ?? undefined,
        name: node.name ?? undefined,
        confidence: node.confidence,
      };
    }),
  }));
}

export const LiveOverlayController: React.FC<LiveOverlayControllerProps> = ({
  map,
  filters,
  network,
  observer,
  packetHistorySegments,
  packetArcsEnabled,
  heatmapEnabled,
}) => {
  const losProfilesByNodeId = useOverlayStore((state) => state.losProfilesByNodeId);
  const customLosSegments = useOverlayStore((state) => state.customLosSegments);
  const customLosStart = useOverlayStore((state) => state.customLosStart);
  const clashPathLines = useOverlayStore((state) => state.clashPathLines);
  const losProfilesKey = useMemo(() => Object.entries(losProfilesByNodeId)
    .map(([nodeId, profiles]) => `${nodeId}:${profiles.length}:${profiles.map((profile) => `${profile.peer_id}:${profile.profile.length}:${profile.itm_viable ? 1 : 0}`).join(',')}`)
    .sort()
    .join('|'), [losProfilesByNodeId]);
  const losProfiles = useMemo(
    () => Object.values(losProfilesByNodeId).flat(),
    // The key avoids flattening again when Zustand produces an equivalent
    // record while unrelated overlay state changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [losProfilesKey],
  );
  const nodes = useNodeMap();
  const arcs = useArcs();
  const nodeCoordinateKey = useMemo(() => Array.from(nodes.values())
    .map((node) => `${node.node_id}:${node.lat ?? ''}:${node.lon ?? ''}:${node.name?.includes('🚫') ? 1 : 0}`)
    .sort()
    .join('|'), [nodes]);
  const nodeElevationKey = useMemo(() => Array.from(nodes.values())
    .map((node) => `${node.node_id}:${node.lat ?? ''}:${node.lon ?? ''}:${node.elevation_m ?? 0}`)
    .sort()
    .join('|'), [nodes]);
  const hiddenCoordMask = useMemo(
    () => buildHiddenCoordMask(nodes.values()),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nodeCoordinateKey],
  );
  const positionElevations = useMemo(() => {
    const elevations = new Map<string, number>();
    for (const node of nodes.values()) {
      if (!hasCoords(node)) continue;
      const elevation = node.elevation_m ?? 0;
      elevations.set(positionKey(node.lat, node.lon), elevation);
      const [maskedLat, maskedLon] = maskNodePoint(node, hiddenCoordMask);
      elevations.set(positionKey(maskedLat, maskedLon), elevation);
    }
    return elevations;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hiddenCoordMask, nodeElevationKey]);
  const setPathNodeIds = useOverlayStore((state) => state.setPathNodeIds);
  const setBetaMetrics = useOverlayStore((state) => state.setBetaMetrics);
  const pathExplanation = useOverlayStore((state) => state.pathExplanation);

  const {
    packetPaths,
    betaPacketPaths,
    betaPathPacketHash,
    betaPathRoutes,
    betaObserverIds,
    betaPathConfidence,
    betaPermutationCount,
    betaRemainingHops,
    pinnedPacketId,
    activePacketSnapshot,
  } = usePacketPathOverlay({
    filters,
    network,
    observer,
  });

  const renderedPaths = useMemo<[number, number][][]>(() => (
    filters.betaPaths ? betaPacketPaths : packetPaths
  ), [betaPacketPaths, filters.betaPaths, packetPaths]);
  const showPathOnly = filters.betaPaths || pinnedPacketId !== null;
  const liveAerialPaths = useMemo<AerialPath[]>(() => {
    if (!showPathOnly) return [];
    const nodesFor = (path: [number, number][]) => path.map((point) => {
      const [lat, lon] = maskPoint(point, hiddenCoordMask);
      return { position: [lon, lat] as [number, number] };
    });
    if (filters.betaPaths) {
      // The routes and their scope come from the same resolved DTO. During the
      // render where packet B becomes active, packet A's still-committed routes
      // therefore cannot be registered under B and replayed as new segments.
      return buildResolvedAerialPaths(betaPathPacketHash, betaPathRoutes, hiddenCoordMask);
    }
    const packetKey = activePacketSnapshot?.packetHash ?? activePacketSnapshot?.id ?? 'live';
    return renderedPaths.map((path) => ({
      id: `main-live-path:${packetKey}:observed`,
      packetHash: activePacketSnapshot?.packetHash ?? activePacketSnapshot?.id ?? null,
      confidence: 1,
      nodes: nodesFor(path),
    })).filter((path) => path.nodes.length > 1);
  }, [activePacketSnapshot?.id, activePacketSnapshot?.packetHash, betaPathPacketHash, betaPathRoutes, filters.betaPaths,
    hiddenCoordMask, renderedPaths, showPathOnly]);

  const observerIdsForOverlay = useMemo(() => {
    if (!showPathOnly) return [];
    const ids = filters.betaPaths && betaObserverIds.length > 0
      ? betaObserverIds
      : packetObserverIds(activePacketSnapshot ?? undefined);
    return Array.from(new Set(ids));
  }, [activePacketSnapshot, betaObserverIds, filters.betaPaths, showPathOnly]);

  const observerNodes = useMemo(() => observerIdsForOverlay.flatMap((observerId) => {
    const node = nodes.get(observerId);
    if (!hasCoords(node)) return [];
    const [lat, lon] = maskNodePoint(node, hiddenCoordMask);
    return [{
      position: [lon, lat] as [number, number],
      nodeId: node.node_id,
      name: node.name ?? undefined,
      isObserver: true,
    }];
  }), [hiddenCoordMask, nodes, observerIdsForOverlay]);

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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeCoordinateKey]);

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
    for (const node of observerNodes) {
      addPoint([node.position[1], node.position[0]]);
    }

    const result = ids;
    const prev = pathNodeIdsPrevRef.current;
    if (prev && result && prev.size === result.size && [...result].every((id) => prev.has(id))) {
      return prev;
    }
    pathNodeIdsPrevRef.current = result;
    return result;
  }, [showPathOnly, activePacketSnapshot, pathPointIndex, renderedPaths, observerNodes]);

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
    <>
      <DeckGLOverlay
      map={map}
      arcs={packetArcsEnabled ? arcs : []}
      showArcs={packetArcsEnabled && filters.livePackets}
      packetHistorySegments={packetHistorySegments}
      showPacketHistory={filters.packetHistory}
      showHeatmap={heatmapEnabled && filters.heatmap}
      betaPaths={renderedPaths}
      clashPathLines={clashPathLines}
      showBetaPaths={filters.betaPaths || pinnedPacketId !== null}
      betaConfidence={betaPathConfidence}
      pathObserverCount={observerNodes.length}
      pathAlternatives={pathExplanation?.alternativesConsidered ?? betaPermutationCount ?? 0}
      pathSummary={pathExplanation?.summary ?? null}
      hiddenCoordMask={hiddenCoordMask}
      positionElevations={positionElevations}
      useTerrainElevation={filters.terrain}
      losProfiles={losProfiles}
      customLosSegments={customLosSegments}
      customLosStart={customLosStart}
      />
      <AnimatedPathOverlay
        map={map}
        paths={liveAerialPaths}
        observerNodes={observerNodes}
        active={showPathOnly}
        terrainEnabled={filters.terrain}
      />
    </>
  );
};
