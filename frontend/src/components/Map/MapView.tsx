import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, useMap, Pane, Polygon, Polyline } from 'react-leaflet';
import type { LatLngExpression, Map as LeafletMap } from 'leaflet';
import type { MeshNode, PacketArc } from '../../hooks/useNodes.js';
import type { NodeCoverage } from '../../hooks/useCoverage.js';
import { hasCoords } from '../../utils/pathing.js';
import type { LinkMetrics } from '../../utils/pathing.js';
import { NodeMarker } from './NodeMarker.js';
import { PacketArcLayer } from './PacketArcLayer.js';
import { NodeSearch } from './NodeSearch.js';

// Sync Leaflet view state with deck.gl view state
interface SyncerProps {
  onViewStateChange: (vs: DeckViewState) => void;
}

interface DeckViewState {
  longitude: number;
  latitude:  number;
  zoom:      number;
  pitch:     number;
  bearing:   number;
}

type ViewBounds = {
  north: number;
  south: number;
  east: number;
  west: number;
};

// Leaflet→deck.gl sync component
const LeafletDeckSyncer: React.FC<SyncerProps> = ({ onViewStateChange }) => {
  const map = useMap();

  React.useEffect(() => {
    const sync = () => {
      const center = map.getCenter();
      onViewStateChange({
        longitude: center.lng,
        latitude:  center.lat,
        // deck.gl uses 512px tiles (mapbox convention); Leaflet uses 256px.
        // One zoom level difference = factor of 2 in scale.
        zoom:      map.getZoom() - 1,
        pitch:     0,
        bearing:   0,
      });
    };

    map.on('move', sync);
    map.on('zoom', sync);
    sync();
    return () => { map.off('move', sync); map.off('zoom', sync); };
  }, [map, onViewStateChange]);

  return null;
};

function ringToLatLng(ring: number[][]): LatLngExpression[] {
  return ring.map(([lon, lat]) => [lat, lon] as LatLngExpression);
}

function geomToRings(geom: { type: string; coordinates: unknown } | null | undefined): LatLngExpression[][] {
  if (!geom) return [];
  if (geom.type === 'Polygon') return [ringToLatLng((geom.coordinates as number[][][])[0])];
  if (geom.type === 'MultiPolygon') return (geom.coordinates as number[][][][]).map((poly) => ringToLatLng(poly[0]));
  return [];
}

function isHiddenMapNode(node: MeshNode | null | undefined): boolean {
  return !hasCoords(node) || Boolean(node?.name?.includes('🚫'));
}

// Raw outer rings from each coverage polygon — used for the green coverage display.
// Using raw rings (not a union) with fillRule:'nonzero' means:
//   - overlapping viewsheds: winding numbers add (+1 per CCW ring) → always filled ✓
//   - no opacity stacking: single SVG <path> element, one fill pass ✓
function useCoverageDisplayRings(coverage: NodeCoverage[]): LatLngExpression[][] {
  return useMemo(() => {
    const rings: LatLngExpression[][] = [];
    for (const c of coverage) {
      rings.push(...geomToRings(c.geom));
    }
    return rings;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [coverage]);
}

// Dash pattern cycle length in px — must match dashArray below ('6 9' = 15px).
const DASH_CYCLE = 15;
// Pixels to advance per animation frame (~60fps → ~18px/s ≈ 1.2 cycles/s).
const DASH_STEP  = 0.3;

interface MapViewProps {
  nodes:           Map<string, MeshNode>;
  arcs:            PacketArc[];
  activeNodes:     Set<string>;
  coverage:        NodeCoverage[];
  showPackets:     boolean;
  showCoverage:    boolean;
  showClientNodes: boolean;
  showLinks:       boolean;
  showHexClashes:  boolean;
  maxHexClashHops: number;
  viablePairsArr:  [string, string][];
  linkMetrics:     Map<string, LinkMetrics>;
  packetPaths:     [number, number][][];
  betaPaths:       [number, number][][];
  betaLowPaths:    [number, number][][];
  betaLowSegments: [[number, number], [number, number]][];
  betaCompletionPaths: [number, number][][];
  showBetaPaths:   boolean;
  pathOpacity:     number;
  onMapReady?:     (m: LeafletMap) => void;
}

// Default UK centre (Teesside area)
const DEFAULT_CENTER: [number, number] = [54.57, -1.23];
const DEFAULT_ZOOM = 11;

export const MapView: React.FC<MapViewProps> = ({
  nodes, arcs, activeNodes, coverage, showPackets, showCoverage, showClientNodes,
  showLinks, showHexClashes, maxHexClashHops, viablePairsArr, linkMetrics, packetPaths, betaPaths, betaLowSegments, betaCompletionPaths, showBetaPaths, pathOpacity, onMapReady,
}) => {
  const [map, setMap] = useState<LeafletMap | null>(null);
  const [viewBounds, setViewBounds] = useState<ViewBounds | null>(null);
  const [focusedPrefix, setFocusedPrefix] = useState<string | null>(null);
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  const [focusedPrefixNodeIds, setFocusedPrefixNodeIds] = useState<Set<string> | null>(null);
  const [focusHidePhase, setFocusHidePhase] = useState<'idle' | 'hide' | 'fade'>('idle');
  const hideTimerRef = useRef<number | null>(null);
  const fadeTimerRef = useRef<number | null>(null);

  const clearFocusTimers = useCallback(() => {
    if (hideTimerRef.current !== null) {
      window.clearTimeout(hideTimerRef.current);
      hideTimerRef.current = null;
    }
    if (fadeTimerRef.current !== null) {
      window.clearTimeout(fadeTimerRef.current);
      fadeTimerRef.current = null;
    }
  }, []);

  useEffect(() => () => clearFocusTimers(), [clearFocusTimers]);

  useEffect(() => {
    if (map && onMapReady) onMapReady(map);
  }, [map, onMapReady]);

  useEffect(() => {
    if (!map) return;
    const syncBounds = () => {
      const bounds = map.getBounds().pad(0.2);
      setViewBounds({
        north: bounds.getNorth(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        west: bounds.getWest(),
      });
    };
    map.on('moveend', syncBounds);
    map.on('zoomend', syncBounds);
    syncBounds();
    return () => {
      map.off('moveend', syncBounds);
      map.off('zoomend', syncBounds);
    };
  }, [map]);

  const [deckViewState, setDeckViewState] = useState<DeckViewState>({
    longitude: DEFAULT_CENTER[1],
    latitude:  DEFAULT_CENTER[0],
    zoom:      DEFAULT_ZOOM,
    pitch:     0,
    bearing:   0,
  });

  const handleViewStateChange = useCallback((vs: unknown) => {
    setDeckViewState(vs as DeckViewState);
  }, []);
  const [isMobileViewport, setIsMobileViewport] = useState(
    () => (typeof window !== 'undefined' ? window.matchMedia('(max-width: 640px)').matches : false),
  );

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const mq = window.matchMedia('(max-width: 640px)');
    const update = () => setIsMobileViewport(mq.matches);
    update();
    mq.addEventListener('change', update);
    return () => mq.removeEventListener('change', update);
  }, []);

  const aniFrameRef    = useRef<number | null>(null);

  // Animate marching dashes by incrementing stroke-dashoffset directly on the
  // Leaflet SVG path element. CSS animation is unreliable here because Leaflet
  // calls _updateStyle (setAttribute) on every prop change, which can interrupt
  // CSS keyframe animations. Direct DOM manipulation in an rAF loop is stable.
  const hasRegular = packetPaths.length > 0;
  const hasBeta = Boolean(showBetaPaths && (betaLowSegments.length > 0 || betaPaths.length > 0));

  useEffect(() => {
    if (!hasRegular && !hasBeta) {
      if (aniFrameRef.current !== null) {
        cancelAnimationFrame(aniFrameRef.current);
        aniFrameRef.current = null;
      }
      return;
    }

      let offset = 0;
      const tick = () => {
        offset = (offset + DASH_STEP) % DASH_CYCLE;
        const val = String(-offset);
        const paths = map?.getContainer().querySelectorAll<SVGPathElement>(
          '.packet-path-overlay, .beta-red-path-overlay, .beta-purple-path-overlay',
        );
        paths?.forEach((path) => path.setAttribute('stroke-dashoffset', val));
        aniFrameRef.current = requestAnimationFrame(tick);
      };

    aniFrameRef.current = requestAnimationFrame(tick);
    return () => {
      if (aniFrameRef.current !== null) {
        cancelAnimationFrame(aniFrameRef.current);
        aniFrameRef.current = null;
      }
    };
  }, [hasRegular, hasBeta, map]); // eslint-disable-line react-hooks/exhaustive-deps

  const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
  const allNodesWithPos = useMemo(() => Array.from(nodes.values()).filter(
    (n) => hasCoords(n)
      && (Date.now() - new Date(n.last_seen).getTime()) < FOURTEEN_DAYS_MS
  ), [nodes]); // eslint-disable-line react-hooks/exhaustive-deps
  const nodesWithPos   = useMemo(() => allNodesWithPos.filter((n) => n.role === undefined || n.role === 2), [allNodesWithPos]);
  const clientNodesArr = useMemo(() => allNodesWithPos.filter((n) => n.role === 1 || n.role === 3), [allNodesWithPos]);
  const repeaterPrefixIds = useMemo(() => {
    const prefixMap = new Map<string, string[]>();
    for (const n of nodesWithPos) {
      const prefix = n.node_id.slice(0, 2).toUpperCase();
      const existing = prefixMap.get(prefix);
      if (existing) existing.push(n.node_id);
      else prefixMap.set(prefix, [n.node_id]);
    }
    return prefixMap;
  }, [nodesWithPos]);

  const handleToggleSamePrefix = useCallback((nodeId: string, enabled: boolean) => {
    if (!enabled) {
      clearFocusTimers();
      setFocusedPrefix(null);
      setFocusedNodeId(null);
      setFocusedPrefixNodeIds(null);
      setFocusHidePhase('idle');
      return;
    }
    const prefix = nodeId.slice(0, 2).toUpperCase();
    const ids = repeaterPrefixIds.get(prefix) ?? [nodeId];
    const idSet = new Set(ids);
    clearFocusTimers();
    setFocusedPrefix(prefix);
    setFocusedNodeId(nodeId);
    setFocusedPrefixNodeIds(idSet);
    setFocusHidePhase('hide');
    hideTimerRef.current = window.setTimeout(() => {
      setFocusHidePhase('fade');
      fadeTimerRef.current = window.setTimeout(() => {
        setFocusedPrefix(null);
        setFocusedNodeId(null);
        setFocusedPrefixNodeIds(null);
        setFocusHidePhase('idle');
      }, 1200);
    }, 10_000);
  }, [clearFocusTimers, repeaterPrefixIds]);

  const coverageRings = useCoverageDisplayRings(coverage);
  const coverageByNodeId = useMemo(() => {
    const m = new Map<string, NodeCoverage>();
    for (const c of coverage) m.set(c.node_id, c);
    return m;
  }, [coverage]);

  const distKm = useCallback((a: MeshNode, b: MeshNode) => {
    if (!hasCoords(a) || !hasCoords(b)) return Number.POSITIVE_INFINITY;
    const midLat = ((a.lat + b.lat) / 2) * (Math.PI / 180);
    const dlat = (a.lat - b.lat) * 111;
    const dlon = (a.lon - b.lon) * 111 * Math.cos(midLat);
    return Math.hypot(dlat, dlon);
  }, []);

  const nodeRangeKm = useCallback((nodeId: string) => {
    const cov = coverageByNodeId.get(nodeId);
    if (!cov?.radius_m) return 50;
    return Math.min(80, Math.max(50, cov.radius_m / 1000));
  }, [coverageByNodeId]);

  const pairInReceiveRange = useCallback((a: MeshNode, b: MeshNode) => {
    const d = distKm(a, b);
    const range = Math.max(nodeRangeKm(a.node_id), nodeRangeKm(b.node_id));
    return d <= range;
  }, [distKm, nodeRangeKm]);


  const clashLinePositions = useCallback((a: MeshNode, b: MeshNode): [number, number][] => {
    const d = distKm(a, b);
    if (d > 0.02) return [[a.lat!, a.lon!], [b.lat!, b.lon!]];
    const off = 0.0018;
    return [[a.lat!, a.lon!], [b.lat! + off, b.lon! + off]];
  }, [distKm]);

  const linkKey = (a: string, b: string) => (a < b ? `${a}:${b}` : `${b}:${a}`);

  const inView = useCallback((lat: number, lon: number) => {
    if (!viewBounds) return true;
    return lat <= viewBounds.north
      && lat >= viewBounds.south
      && lon <= viewBounds.east
      && lon >= viewBounds.west;
  }, [viewBounds]);

  const lineInView = useCallback((positions: [number, number][]) => {
    if (!viewBounds || positions.length < 1) return true;
    let minLat = Number.POSITIVE_INFINITY;
    let maxLat = Number.NEGATIVE_INFINITY;
    let minLon = Number.POSITIVE_INFINITY;
    let maxLon = Number.NEGATIVE_INFINITY;
    for (const [lat, lon] of positions) {
      if (inView(lat, lon)) return true;
      minLat = Math.min(minLat, lat);
      maxLat = Math.max(maxLat, lat);
      minLon = Math.min(minLon, lon);
      maxLon = Math.max(maxLon, lon);
    }
    return !(maxLat < viewBounds.south
      || minLat > viewBounds.north
      || maxLon < viewBounds.west
      || minLon > viewBounds.east);
  }, [inView, viewBounds]);

  const linkColor = (pathLossDb: number | null | undefined) => {
    if (pathLossDb == null) return '#d1d5db';
    if (pathLossDb <= 120) return '#22c55e';
    if (pathLossDb <= 135) return '#fbbf24';
    return '#ef4444';
  };

  const clashAdjacency = useMemo(() => {
    const adj = new Map<string, Set<string>>();
    for (const [aId, bId] of viablePairsArr) {
      const a = nodes.get(aId);
      const b = nodes.get(bId);
      if (!hasCoords(a) || !hasCoords(b)) continue;
      const key = linkKey(aId, bId);
      // Require computed dB (weak or above) for every edge used in clash-hop routing.
      const pathLoss = linkMetrics.get(key)?.itm_path_loss_db;
      if (pathLoss == null) continue;
      if (!pairInReceiveRange(a, b)) continue;
      if (!adj.has(aId)) adj.set(aId, new Set());
      if (!adj.has(bId)) adj.set(bId, new Set());
      adj.get(aId)!.add(bId);
      adj.get(bId)!.add(aId);
    }
    return adj;
  }, [viablePairsArr, linkMetrics, nodes, pairInReceiveRange]);

  const shortestPathWithinRelayHops = useCallback((fromId: string, toId: string, maxRelayHops: number) => {
    if (fromId === toId) return [fromId];
    const maxEdges = Math.max(1, Math.floor(maxRelayHops) + 1);
    const visited = new Set<string>([fromId]);
    const prev = new Map<string, string>();
    const queue: Array<{ id: string; edges: number }> = [{ id: fromId, edges: 0 }];
    while (queue.length > 0) {
      const cur = queue.shift()!;
      if (cur.edges >= maxEdges) continue;
      for (const next of (clashAdjacency.get(cur.id) ?? [])) {
        if (visited.has(next)) continue;
        visited.add(next);
        prev.set(next, cur.id);
        const nextEdges = cur.edges + 1;
        if (next === toId) {
          const path = [toId];
          let p = toId;
          while (prev.has(p)) {
            p = prev.get(p)!;
            path.unshift(p);
          }
          return path;
        }
        queue.push({ id: next, edges: nextEdges });
      }
    }
    return null;
  }, [clashAdjacency]);

  type ClashPath = { key: string; nodeIds: string[]; offenderA: string; offenderB: string };

  const clashPaths = useMemo(() => {
    const paths: ClashPath[] = [];
    for (const [, ids] of repeaterPrefixIds) {
      if (ids.length < 2) continue;
      for (let i = 0; i < ids.length - 1; i++) {
        for (let j = i + 1; j < ids.length; j++) {
          const fromId = ids[i]!;
          const toId = ids[j]!;
          const path = shortestPathWithinRelayHops(fromId, toId, maxHexClashHops);
          if (!path || path.length < 2) continue;
          paths.push({
            key: `clash-${fromId.slice(0, 8)}-${toId.slice(0, 8)}-${path.length}`,
            nodeIds: path,
            offenderA: fromId,
            offenderB: toId,
          });
        }
      }
    }
    return paths;
  }, [repeaterPrefixIds, shortestPathWithinRelayHops, maxHexClashHops]);

  const focusedClashPaths = useMemo(() => {
    if (!focusedNodeId || !focusedPrefixNodeIds || focusedPrefixNodeIds.size < 2) return [];
    const paths: ClashPath[] = [];
    for (const targetId of focusedPrefixNodeIds) {
      if (targetId === focusedNodeId) continue;
      const path = shortestPathWithinRelayHops(focusedNodeId, targetId, maxHexClashHops);
      if (!path || path.length < 2) continue;
      paths.push({
        key: `focus-${focusedNodeId.slice(0, 8)}-${targetId.slice(0, 8)}-${path.length}`,
        nodeIds: path,
        offenderA: focusedNodeId,
        offenderB: targetId,
      });
    }
    return paths;
  }, [focusedNodeId, focusedPrefixNodeIds, shortestPathWithinRelayHops, maxHexClashHops]);

  const clashPathLines = useMemo(() => {
    const chosen = showHexClashes ? clashPaths : focusedClashPaths;
    const lines: Array<{ key: string; positions: [number, number][] }> = [];
    const edgeKeys = new Set<string>();
    for (const path of chosen) {
      for (let i = 0; i < path.nodeIds.length - 1; i++) {
        const a = nodes.get(path.nodeIds[i]!);
        const b = nodes.get(path.nodeIds[i + 1]!);
        if (!hasCoords(a) || !hasCoords(b)) continue;
        const edgeKey = linkKey(a.node_id, b.node_id);
        if (edgeKeys.has(edgeKey)) continue;
        edgeKeys.add(edgeKey);
        lines.push({ key: `${path.key}-${edgeKey}`, positions: clashLinePositions(a, b) });
      }
    }
    return lines;
  }, [showHexClashes, clashPaths, focusedClashPaths, nodes, clashLinePositions]);

  const clashOffenderNodeIds = useMemo(() => {
    const ids = new Set<string>();
    const chosen = showHexClashes ? clashPaths : focusedClashPaths;
    for (const path of chosen) {
      ids.add(path.offenderA);
      ids.add(path.offenderB);
    }
    return ids;
  }, [showHexClashes, clashPaths, focusedClashPaths]);

  const clashVisibleNodeIds = useMemo(() => {
    const ids = new Set<string>();
    const chosen = showHexClashes ? clashPaths : focusedClashPaths;
    for (const path of chosen) {
      for (const id of path.nodeIds) ids.add(id);
    }
    return ids;
  }, [showHexClashes, clashPaths, focusedClashPaths]);

  const clashModeActive = showHexClashes || !!focusedPrefixNodeIds;
  const effectiveShowCoverage = showCoverage && !clashModeActive;
  const effectiveShowLinks = showLinks && !clashModeActive;

  // Stable fingerprint of linked nodes' position/role/last_seen — only changes when
  // data relevant to link rendering changes, not on every node_update heartbeat.
  const linkedNodesKey = useMemo(() => {
    const ids = new Set(viablePairsArr.flatMap(([a, b]) => [a, b]));
    return Array.from(ids).sort().map((id) => {
      const n = nodes.get(id);
      return n ? `${id}=${n.lat ?? ''},${n.lon ?? ''},${n.role ?? ''},${n.last_seen.slice(0, 13)}` : id;
    }).join(';');
  }, [viablePairsArr, nodes]);

  // Resolve viable link pairs to lat/lon polyline positions
  const linkLines = useMemo(() => {
    if (!showLinks || viablePairsArr.length === 0) return [];
    const lines: Array<{
      key: string;
      positions: [number, number][];
      observedCount: number;
      pathLossDb: number | null | undefined;
      countAToB: number;
      countBToA: number;
    }> = [];
    for (const [aId, bId] of viablePairsArr) {
      const a = nodes.get(aId);
      const b = nodes.get(bId);
      if (
        hasCoords(a)
        && hasCoords(b)
        && (Date.now() - new Date(a.last_seen).getTime()) < FOURTEEN_DAYS_MS
        && (Date.now() - new Date(b.last_seen).getTime()) < FOURTEEN_DAYS_MS
        && (a.role === undefined || a.role === 2)
        && (b.role === undefined || b.role === 2)
      ) {
        const key = linkKey(aId, bId);
        const metrics = linkMetrics.get(key);
        lines.push({
          key,
          positions: [[a.lat, a.lon], [b.lat, b.lon]],
          observedCount: metrics?.observed_count ?? 0,
          pathLossDb: metrics?.itm_path_loss_db,
          countAToB: metrics?.count_a_to_b ?? 0,
          countBToA: metrics?.count_b_to_a ?? 0,
        });
      }
    }
    const zoom = map?.getZoom() ?? DEFAULT_ZOOM;
    const visibleLines = lines.filter((line) => lineInView(line.positions));
    if (zoom >= 9 || visibleLines.length <= 400) return visibleLines;
    return visibleLines
      .sort((a, b) => (b.observedCount - a.observedCount))
      .slice(0, 400);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showLinks, viablePairsArr, linkedNodesKey, linkMetrics, map, lineInView]);

  const visibleClashPathLines = useMemo(
    () => clashPathLines.filter((line) => lineInView(line.positions)),
    [clashPathLines, lineInView],
  );

  const visibleRepeaterNodes = useMemo(
    () => nodesWithPos.filter((node) => hasCoords(node) && inView(node.lat, node.lon)),
    [nodesWithPos, inView],
  );

  const visibleClientNodes = useMemo(
    () => clientNodesArr.filter((node) => hasCoords(node) && inView(node.lat, node.lon)),
    [clientNodesArr, inView],
  );

  const markerSize = useMemo(() => {
    const leafletZoom = deckViewState.zoom + 1;
    let size = 12;
    if (leafletZoom <= 6) size = 5;
    else if (leafletZoom <= 7) size = 6;
    else if (leafletZoom <= 8) size = 7;
    else if (leafletZoom <= 9) size = 8;
    else if (leafletZoom <= 10) size = 9;
    else if (leafletZoom <= 11) size = 10;
    else if (leafletZoom <= 12) size = 11;
    if (isMobileViewport) size -= 1;
    return Math.max(4, size);
  }, [deckViewState.zoom, isMobileViewport]);

  return (
    <div className="map-area">
      <NodeSearch nodes={nodes} map={map} />
      <MapContainer
        ref={setMap}
        center={DEFAULT_CENTER}
        zoom={DEFAULT_ZOOM}
        style={{ width: '100%', height: '100%' }}
        zoomControl={false}
        attributionControl={false}
      >
        {/* CartoDB Dark Matter tiles */}
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
          subdomains="abcd"
          maxZoom={19}
          keepBuffer={6}
          updateWhenIdle={false}
        />

        {/* Sync Leaflet map position to deck.gl */}
        <LeafletDeckSyncer onViewStateChange={handleViewStateChange} />

        {/* Coverage — raw outer rings from each viewshed, fillRule:'nonzero'.
            nonzero means overlapping CCW rings sum winding numbers (+1 each)
            so all covered areas fill regardless of how many viewsheds overlap. */}
        {effectiveShowCoverage && coverageRings.length > 0 && (
          <Pane name="coveragePane" style={{ zIndex: 350 }}>
            <Polygon
              positions={coverageRings as LatLngExpression[][]}
              pathOptions={{
                fillColor: '#22c55e',
                fillOpacity: 0.18,
                weight: 0,
                fillRule: 'nonzero',
              }}
              interactive={false}
            />
          </Pane>
        )}

        {/* Confirmed link lines — ITM-viable node pairs */}
        {effectiveShowLinks && linkLines.length > 0 && (
          <Pane name="linksPane" style={{ zIndex: 400 }}>
            {linkLines.map((line) => {
              const obs = Math.max(1, line.observedCount);
              const strength = Math.log10(obs + 1);
              const opacity = Math.min(0.85, 0.35 + strength * 0.22);
              const weight = Math.min(3.2, 1.0 + strength * 1.1);
              return (
              <Polyline
                key={line.key}
                positions={line.positions}
                pathOptions={{
                  color: linkColor(line.pathLossDb),
                  weight,
                  opacity,
                }}
                interactive={false}
              />
              );
            })}
          </Pane>
        )}

        {clashModeActive && (
          <Pane name="hexClashPane" style={{ zIndex: 405 }}>
            {visibleClashPathLines.map((line) => (
              <Polyline
                key={line.key}
                positions={line.positions}
                pathOptions={{
                  color: '#f97316',
                  weight: 2.2,
                  opacity: 0.9,
                }}
                interactive={false}
              />
            ))}
          </Pane>
        )}

        {/* Repeater markers — Leaflet default marker pane at zIndex 600 */}
        {visibleRepeaterNodes.map((node) => {
          if (isHiddenMapNode(node)) return null;
          if (showHexClashes && !clashVisibleNodeIds.has(node.node_id)) return null;
          const isFocusVisible = clashVisibleNodeIds.has(node.node_id) || (focusedPrefixNodeIds?.has(node.node_id) ?? false);
          if (focusedPrefixNodeIds && focusHidePhase === 'hide' && !isFocusVisible) return null;
          return (
            <NodeMarker
              key={node.node_id}
              node={node}
              isActive={activeNodes.has(node.node_id)}
              isHighlighted={!!focusedPrefix && node.node_id.slice(0, 2).toUpperCase() === focusedPrefix}
              isRestoring={!!focusedPrefixNodeIds && focusHidePhase === 'fade' && !isFocusVisible}
              hexClashState={clashModeActive ? (clashOffenderNodeIds.has(node.node_id) ? 'offender' : clashVisibleNodeIds.has(node.node_id) ? 'clear' : undefined) : undefined}
              samePrefixRepeaterCount={repeaterPrefixIds.get(node.node_id.slice(0, 2).toUpperCase())?.length ?? 1}
              samePrefixActive={!!focusedPrefix && node.node_id.slice(0, 2).toUpperCase() === focusedPrefix}
              onToggleSamePrefix={handleToggleSamePrefix}
              nodeCoverage={coverageByNodeId.get(node.node_id)}
              markerSize={markerSize}
            />
          );
        })}

        {/* Companion radio + room server markers (toggled via filter) */}
        {showClientNodes && !showHexClashes && visibleClientNodes.map((node) => {
          if (isHiddenMapNode(node)) return null;
          const isFocusVisible = clashVisibleNodeIds.has(node.node_id);
          if (focusedPrefixNodeIds && focusHidePhase === 'hide' && !isFocusVisible) return null;
          return (
            <NodeMarker
              key={node.node_id}
              node={node}
              isActive={activeNodes.has(node.node_id)}
              isRestoring={!!focusedPrefixNodeIds && focusHidePhase === 'fade' && !isFocusVisible}
              nodeCoverage={coverageByNodeId.get(node.node_id)}
              markerSize={markerSize}
            />
          );
        })}

        {/* Live packet path — marching dashes from source → observer */}
        {packetPaths.map((path, idx) => (
          <Polyline
            key={`packet-path-${idx}`}
            positions={path}
            className="packet-path-overlay"
            pathOptions={{
              color:     '#00c4ff',
              weight:    2,
              dashArray: '6 9',
              opacity:   pathOpacity,
            }}
          />
        ))}

        {/* Beta path — uncertain (red) drawn first so confident (purple) always renders on top.
            Renders individual filtered segments so purple-covered edges are never drawn in red. */}
        {showBetaPaths && betaLowSegments.map(([a, b], idx) => (
          <Polyline
            key={`beta-low-seg-${idx}`}
            positions={[a, b]}
            className="beta-red-path-overlay"
            pathOptions={{
              color:     '#ef4444',
              weight:    2.6,
              dashArray: '6 9',
              opacity:   Math.min(0.9, pathOpacity),
            }}
            interactive={false}
          />
        ))}

        {/* Purple confident portion rendered last — highest z-order so it is never obscured by the red uncertain portion */}
        {showBetaPaths && betaPaths.map((path, idx) => (
          <Polyline
            key={`beta-purple-${idx}`}
            positions={path}
            className="beta-purple-path-overlay"
            pathOptions={{
              color:     '#a855f7',
              weight:    2.8,
              dashArray: '6 9',
              opacity:   pathOpacity,
            }}
          />
        ))}

        {showBetaPaths && betaCompletionPaths.length > 0 && (
          <Pane name="betaCompletionsPane" style={{ zIndex: 520 }}>
            {betaCompletionPaths.map((path, idx) => (
              <Polyline
                key={`beta-completion-${idx}`}
                positions={path}
                pathOptions={{
                  color: '#ef4444',
                  weight: 1.8,
                  dashArray: '4 7',
                  opacity: Math.min(0.78, pathOpacity * 0.95),
                }}
                interactive={false}
              />
            ))}
          </Pane>
        )}
      </MapContainer>

      {/* deck.gl overlay — arcs only */}
      <PacketArcLayer
        arcs={arcs}
        showArcs={showPackets}
        viewState={deckViewState}
      />
    </div>
  );
};
