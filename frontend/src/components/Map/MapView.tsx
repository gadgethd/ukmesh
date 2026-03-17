import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, useMap, Pane, Polygon, Polyline, Circle, Popup } from 'react-leaflet';
import type { LatLngExpression, Map as LeafletMap, LeafletMouseEvent } from 'leaflet';
import type { MeshNode } from '../../hooks/useNodes.js';
import type { NodeCoverage } from '../../hooks/useCoverage.js';
import { hasCoords, maskCircleCenter, maskNodePoint, isProhibitedMapNode, HIDDEN_NODE_MASK_RADIUS_METERS } from '../../utils/pathing.js';
import type { HiddenMaskGeometry, LinkMetrics } from '../../utils/pathing.js';
import { NodeMarker } from './NodeMarker.js';
import { NodeSearch } from './NodeSearch.js';

export interface DeckViewState {
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

interface SyncerProps { onViewStateChange: (vs: DeckViewState) => void; }

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

// ── GPU popup helpers ──────────────────────────────────────────────────────────

interface NodeLink {
  peer_id: string; peer_name: string | null; observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number; count_peer_to_this: number;
}

const GPU_ROLE_LABELS: Record<number, string> = {
  1: 'Companion Radio', 2: 'Repeater', 3: 'Room Server', 4: 'Sensor',
};

function gpuTimeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60)    return `${secs}s ago`;
  if (secs < 3600)  return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

function gpuIsRepeater(role: number | undefined): boolean {
  return role === undefined || role === 2;
}

// Registers a single map.on('click') listener; uses refs so the handler is never
// re-registered when node data updates — avoiding any click-listener churn.
interface GPUClickHandlerProps {
  allNodes: MeshNode[];
  hiddenCoordMask: Map<string, HiddenMaskGeometry>;
  onNodeClick: (node: MeshNode, lat: number, lon: number) => void;
}
const GPUClickHandler: React.FC<GPUClickHandlerProps> = ({ allNodes, hiddenCoordMask, onNodeClick }) => {
  const map = useMap();
  const nodesRef = useRef(allNodes);
  const maskRef  = useRef(hiddenCoordMask);
  const cbRef    = useRef(onNodeClick);

  useEffect(() => { nodesRef.current = allNodes; },         [allNodes]);
  useEffect(() => { maskRef.current  = hiddenCoordMask; },  [hiddenCoordMask]);
  useEffect(() => { cbRef.current    = onNodeClick; },      [onNodeClick]);

  useEffect(() => {
    const handler = (e: LeafletMouseEvent) => {
      const clickPt = map.latLngToContainerPoint(e.latlng);
      let nearest: MeshNode | null = null;
      let nearestDist = 16; // pixel threshold
      for (const node of nodesRef.current) {
        if (!hasCoords(node)) continue;
        const masked = maskNodePoint(node as MeshNode & { lat: number; lon: number }, maskRef.current);
        const lat = masked[0];
        const lon = masked[1];
        const pt = map.latLngToContainerPoint([lat, lon]);
        const d = Math.hypot(clickPt.x - pt.x, clickPt.y - pt.y);
        if (d < nearestDist) { nearestDist = d; nearest = node; }
      }
      if (nearest) {
        // nearest is guaranteed to have coords (hasCoords check above)
        const masked = maskNodePoint(nearest as MeshNode & { lat: number; lon: number }, maskRef.current);
        cbRef.current(nearest, masked[0], masked[1]);
      }
    };
    map.on('click', handler);
    return () => { map.off('click', handler); };
  }, [map]);

  return null;
};

// ──────────────────────────────────────────────────────────────────────────────

function ringToLatLng(ring: number[][] | undefined): LatLngExpression[] {
  if (!ring) return [];
  return ring.map(([lon, lat]) => [lat, lon] as LatLngExpression);
}

function geomToRings(geom: { type: string; coordinates: unknown } | null | undefined): LatLngExpression[][] {
  if (!geom) return [];
  if (geom.type === 'Polygon') {
    const ring = (geom.coordinates as number[][][])[0];
    return ring ? [ringToLatLng(ring)] : [];
  }
  if (geom.type === 'MultiPolygon') {
    return (geom.coordinates as number[][][][]).flatMap((poly) => {
      const ring = poly[0];
      return ring ? [ringToLatLng(ring)] : [];
    });
  }
  return [];
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


interface MapViewProps {
  nodes:           Map<string, MeshNode>;
  inferredNodes:   MeshNode[];
  inferredActiveNodeIds: Set<string>;
  activeNodes:     Set<string>;
  coverage:        NodeCoverage[];
  onDeckViewStateChange: (vs: DeckViewState) => void;
  showCoverage:    boolean;
  showClientNodes: boolean;
  showHexClashes:  boolean;
  maxHexClashHops: number;
  viablePairsArr:  [string, string][];
  linkMetrics:     Map<string, LinkMetrics>;
  hiddenCoordMask: Map<string, HiddenMaskGeometry>;
  pathNodeIds:     Set<string> | null;
  onMapReady?:     (m: LeafletMap) => void;
  /** Called when prefix-focus mode activates/deactivates, so DeckGLOverlay can hide GPU nodes
   *  during the focus animation (which relies on Leaflet markers for show/hide transitions). */
  onPrefixFocusActiveChange?: (active: boolean) => void;
}

// Default UK centre (Teesside area)
const DEFAULT_CENTER: [number, number] = [54.57, -1.23];
const DEFAULT_ZOOM = 11;
const STALE_MARKER_MS = 7 * 24 * 60 * 60 * 1000;

// Custom comparison — only props that affect Leaflet SVG/marker rendering.
// GPU overlay props (packet history, beta paths) are handled by DeckGLOverlay.
function propsAreEqual(prev: MapViewProps, next: MapViewProps): boolean {
  if (prev.nodes !== next.nodes) return false;
  if (prev.coverage !== next.coverage) return false;
  if (prev.activeNodes !== next.activeNodes) return false;
  if (prev.viablePairsArr !== next.viablePairsArr) return false;
  if (prev.linkMetrics !== next.linkMetrics) return false;
  if (prev.inferredNodes !== next.inferredNodes) return false;
  if (prev.inferredActiveNodeIds !== next.inferredActiveNodeIds) return false;
  if (prev.hiddenCoordMask !== next.hiddenCoordMask) return false;
  if (prev.showCoverage !== next.showCoverage) return false;
  if (prev.showClientNodes !== next.showClientNodes) return false;
  if (prev.showHexClashes !== next.showHexClashes) return false;
  if (prev.maxHexClashHops !== next.maxHexClashHops) return false;
  if (prev.pathNodeIds !== next.pathNodeIds) return false;
  if (prev.onPrefixFocusActiveChange !== next.onPrefixFocusActiveChange) return false;
  return true;
}

export const MapView = React.memo(({
  nodes, inferredNodes, inferredActiveNodeIds, activeNodes, coverage, showCoverage, showClientNodes,
  showHexClashes, maxHexClashHops, viablePairsArr, linkMetrics, hiddenCoordMask, pathNodeIds,
  onMapReady, onDeckViewStateChange, onPrefixFocusActiveChange,
}) => {
  const [map, setMap] = useState<LeafletMap | null>(null);
  const [viewBounds, setViewBounds] = useState<ViewBounds | null>(null);
  const [focusedPrefix, setFocusedPrefix] = useState<string | null>(null);
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  const [focusedPrefixNodeIds, setFocusedPrefixNodeIds] = useState<Set<string> | null>(null);
  const [focusHidePhase, setFocusHidePhase] = useState<'idle' | 'hide' | 'fade'>('idle');
  const hideTimerRef = useRef<number | null>(null);
  const fadeTimerRef = useRef<number | null>(null);

  // GPU popup — one popup at a time driven by click handler
  const [gpuPopupNode, setGpuPopupNode] = useState<MeshNode | null>(null);
  const [gpuPopupLat,  setGpuPopupLat]  = useState<number>(0);
  const [gpuPopupLon,  setGpuPopupLon]  = useState<number>(0);
  const [gpuPopupLinks, setGpuPopupLinks] = useState<NodeLink[] | null>(null);

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

  // Fetch neighbour links when GPU popup opens for non-repeater nodes
  useEffect(() => {
    if (!gpuPopupNode || gpuIsRepeater(gpuPopupNode.role)) {
      setGpuPopupLinks(null);
      return;
    }
    setGpuPopupLinks(null);
    fetch(`/api/nodes/${gpuPopupNode.node_id}/links`)
      .then((r) => r.json())
      .then((data: NodeLink[]) => setGpuPopupLinks(data))
      .catch(() => setGpuPopupLinks([]));
  }, [gpuPopupNode]);

  const handleGpuNodeClick = useCallback((node: MeshNode, lat: number, lon: number) => {
    setGpuPopupNode(node);
    setGpuPopupLat(lat);
    setGpuPopupLon(lon);
  }, []);

  // Notify App.tsx when prefix-focus mode activates/deactivates so it can pause GPU node
  // rendering and let Leaflet handle the show/hide transition animation.
  useEffect(() => {
    onPrefixFocusActiveChange?.(focusHidePhase !== 'idle');
  }, [focusHidePhase, onPrefixFocusActiveChange]);

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
    longitude: DEFAULT_CENTER[1], latitude: DEFAULT_CENTER[0], zoom: DEFAULT_ZOOM, pitch: 0, bearing: 0,
  });
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

  // Marching dashes are animated via CSS @keyframes in globals.css — no JS rAF needed.

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

  // Stable callback: without this, an inline arrow in JSX would create a new function reference
  // on every render, causing LeafletDeckSyncer's effect to re-run and re-register the 'move'
  // event listener every frame during pan — effectively listener churn at 60fps.
  const handleViewStateChange = useCallback((vs: DeckViewState) => {
    setDeckViewState(vs);
    onDeckViewStateChange(vs);
  }, [onDeckViewStateChange]);

  const clashModeActive = showHexClashes || !!focusedPrefixNodeIds;
  const effectiveShowCoverage = showCoverage && !clashModeActive;

  // When GPU nodes are active, NodeMarkers are NOT rendered at all — zero React fibers for
  // individual nodes. DeckGLOverlay's ScatterplotLayer draws the dots on the GPU, and a single
  // GPUClickHandler does hit-testing on click. Fall back to full Leaflet markers during
  // hex-clash mode (needs clash colours) and prefix-focus animations (show/hide transitions).
  const gpuRendered = !clashModeActive && focusHidePhase === 'idle';

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

  const visibleInferredNodes = useMemo(
    () => inferredNodes.filter((node) => hasCoords(node) && inView(node.lat, node.lon)),
    [inferredNodes, inView],
  );

  // All visible nodes fed to the GPU click handler for hit-testing
  const allVisibleGpuNodes = useMemo(() => {
    const result = [...visibleRepeaterNodes, ...visibleInferredNodes];
    if (showClientNodes) result.push(...visibleClientNodes);
    return result;
  }, [visibleRepeaterNodes, visibleInferredNodes, visibleClientNodes, showClientNodes]);

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
          keepBuffer={10}
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
        {clashModeActive && (
          <Pane name="hexClashPane" style={{ zIndex: 660 }}>
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

        {/* ── GPU mode: zero NodeMarker fibers ─────────────────────────────────
            The ScatterplotLayer in DeckGLOverlay renders all node dots on the GPU.
            We only add Leaflet elements for nodes that need them:
            - Privacy circles for prohibited nodes (visual mask ring)
            - A single click handler that opens a popup for the nearest node
            - The popup itself                                                    */}
        {gpuRendered && visibleRepeaterNodes
          .filter((n) => isProhibitedMapNode(n) && hasCoords(n))
          .map((node) => {
            const cp = maskCircleCenter([node.lat!, node.lon!], hiddenCoordMask);
            return (
              <Circle
                key={`priv-${node.node_id}`}
                center={[cp?.[0] ?? node.lat!, cp?.[1] ?? node.lon!]}
                radius={HIDDEN_NODE_MASK_RADIUS_METERS}
                pathOptions={{ color: '#f59e0b', weight: 1.4, opacity: 0.55, fillColor: '#f59e0b', fillOpacity: 0.05, dashArray: '4 6' }}
                interactive={false}
              />
            );
          })}

        {gpuRendered && (
          <GPUClickHandler
            allNodes={allVisibleGpuNodes}
            hiddenCoordMask={hiddenCoordMask}
            onNodeClick={handleGpuNodeClick}
          />
        )}

        {gpuRendered && gpuPopupNode && (() => {
          const node = gpuPopupNode;
          const prohibited = isProhibitedMapNode(node);
          const fallbackName = GPU_ROLE_LABELS[node.role ?? 2] ?? 'Unknown Device';
          const displayName = prohibited ? `Redacted ${fallbackName}` : (node.name ?? `Unknown ${fallbackName}`);
          const ageMs = Date.now() - new Date(node.last_seen).getTime();
          const isStale = ageMs > STALE_MARKER_MS;
          const statusLabel = isStale ? 'STALE' : node.is_online ? 'ONLINE' : 'OFFLINE';
          const statusColor = isStale ? 'var(--danger)' : node.is_online ? 'var(--online)' : 'var(--offline)';
          const isRepeater = gpuIsRepeater(node.role);
          return (
            <Popup
              position={[gpuPopupLat, gpuPopupLon]}
              eventHandlers={{ remove: () => { setGpuPopupNode(null); setGpuPopupLinks(null); } }}
            >
              <div className="node-popup">
                <div className="node-popup__name">{displayName}</div>
                {node.public_key && (
                  <div className="node-popup__row">
                    <span>Public key</span>
                    <span className="node-popup__mono">{node.public_key}</span>
                  </div>
                )}
                {!isRepeater && node.role !== undefined && (
                  <div className="node-popup__row">
                    <span>Type</span>
                    <span>{GPU_ROLE_LABELS[node.role] ?? 'Unknown'}</span>
                  </div>
                )}
                <div className="node-popup__row">
                  <span>Status</span>
                  <span style={{ color: statusColor }}>{statusLabel}</span>
                </div>
                {node.hardware_model && (
                  <div className="node-popup__row">
                    <span>Hardware</span>
                    <span>{node.hardware_model}</span>
                  </div>
                )}
                <div className="node-popup__row">
                  <span>Last seen</span>
                  <span>{gpuTimeAgo(node.last_seen)}</span>
                </div>
                {node.advert_count !== undefined && (
                  <div className="node-popup__row">
                    <span>Times seen</span>
                    <span>{node.advert_count}</span>
                  </div>
                )}
                <div className="node-popup__row">
                  <span>Position</span>
                  <span>{prohibited ? 'Redacted' : `${gpuPopupLat.toFixed(5)}, ${gpuPopupLon.toFixed(5)}`}</span>
                </div>
                {prohibited && (
                  <div className="node-popup__row">
                    <span>Location</span>
                    <span>Redacted within 1 mile radius</span>
                  </div>
                )}
                {node.elevation_m !== undefined && node.elevation_m !== null && (
                  <div className="node-popup__row">
                    <span>Elevation</span>
                    <span>{Math.round(node.elevation_m)} m ASL</span>
                  </div>
                )}
                {!isRepeater && gpuPopupLinks === null && (
                  <div className="node-popup__neighbours-loading">Loading neighbours…</div>
                )}
                {!isRepeater && gpuPopupLinks !== null && gpuPopupLinks.length > 0 && (
                  <div className="node-popup__neighbours">
                    <div className="node-popup__neighbours-title">Confirmed neighbours</div>
                    {gpuPopupLinks.map((lk) => {
                      const tx = lk.count_this_to_peer > 0;
                      const rx = lk.count_peer_to_this > 0;
                      const arrow = tx && rx ? '↔' : tx ? '→' : '←';
                      return (
                        <div key={lk.peer_id} className="node-popup__neighbour-row">
                          <span className="node-popup__neighbour-name">{arrow} {lk.peer_name ?? lk.peer_id.slice(0, 8)}</span>
                          <span className="node-popup__neighbour-meta">
                            {lk.observed_count}× seen
                            {lk.itm_path_loss_db != null && <> &middot; {Math.round(lk.itm_path_loss_db)} dB</>}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </Popup>
          );
        })()}

        {/* ── Non-GPU mode: full Leaflet NodeMarkers ──────────────────────────
            Active during hex-clash mode and prefix-focus animations, where we need
            per-node Leaflet SVG styling (clash colours, show/hide transitions).   */}
        {!gpuRendered && visibleRepeaterNodes.map((node) => {
          if (!hasCoords(node)) return null;
          if (pathNodeIds && !pathNodeIds.has(node.node_id.toLowerCase())) return null;
          if (showHexClashes && !clashVisibleNodeIds.has(node.node_id)) return null;
          const isFocusVisible = clashVisibleNodeIds.has(node.node_id) || (focusedPrefixNodeIds?.has(node.node_id) ?? false);
          if (focusedPrefixNodeIds && focusHidePhase === 'hide' && !isFocusVisible) return null;
          const isStaleNode = (Date.now() - new Date(node.last_seen).getTime()) > STALE_MARKER_MS;
          const displayPosition = maskNodePoint(node, hiddenCoordMask);
          const circleCenterPosition = maskCircleCenter([node.lat, node.lon], hiddenCoordMask);
          return (
            <NodeMarker
              key={node.node_id}
              node={node}
              displayPosition={displayPosition}
              circleCenterPosition={circleCenterPosition}
              isActive={activeNodes.has(node.node_id)}
              isInferred={isStaleNode && inferredActiveNodeIds.has(node.node_id.toLowerCase()) && !activeNodes.has(node.node_id)}
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

        {!gpuRendered && !showHexClashes && visibleInferredNodes.map((node) => (
          <NodeMarker
            key={node.node_id}
            node={node}
            isActive={false}
            isInferred
            markerSize={Math.max(4, markerSize - 1)}
          />
        ))}

        {!gpuRendered && showClientNodes && !showHexClashes && visibleClientNodes.map((node) => {
          if (!hasCoords(node)) return null;
          const isFocusVisible = clashVisibleNodeIds.has(node.node_id);
          if (focusedPrefixNodeIds && focusHidePhase === 'hide' && !isFocusVisible) return null;
          return (
            <NodeMarker
              key={node.node_id}
              node={node}
              displayPosition={maskNodePoint(node, hiddenCoordMask)}
              circleCenterPosition={maskCircleCenter([node.lat, node.lon], hiddenCoordMask)}
              isActive={activeNodes.has(node.node_id)}
              isRestoring={!!focusedPrefixNodeIds && focusHidePhase === 'fade' && !isFocusVisible}
              nodeCoverage={coverageByNodeId.get(node.node_id)}
              markerSize={markerSize}
            />
          );
        })}

      </MapContainer>
    </div>
  );
}, propsAreEqual);
