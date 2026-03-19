/**
 * MapLibreMap — replaces MapView (Leaflet).
 *
 * Node dots are rendered as a MapLibre GeoJSON circle layer (GPU, no React fibers).
 * Pan/zoom is pure GPU — zero JS work on move events.
 * Coverage, hex-clash lines, and privacy rings are also GeoJSON layers.
 * Click hit-testing uses MapLibre's built-in R-tree spatial index.
 * deck.gl overlays are integrated via @deck.gl/mapbox (MapboxOverlay).
 */
import React, { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import type { MeshNode } from '../../hooks/useNodes.js';
import { nodeStore } from '../../hooks/useNodes.js';
import { coverageStore, type NodeCoverage } from '../../hooks/useCoverage.js';
import { linkStateStore } from '../../hooks/useLinkState.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import {
  buildHiddenCoordMask,
  hasCoords,
  isProhibitedMapNode,
  maskNodePoint,
  maskCircleCenter,
  HIDDEN_NODE_MASK_RADIUS_METERS,
  linkKey,
} from '../../utils/pathing.js';
import { NodeSearch } from './NodeSearch.js';
import { useOverlayStore } from '../../store/overlayStore.js';

// ── Constants ─────────────────────────────────────────────────────────────────

const DEFAULT_CENTER: [number, number] = [54.57, -1.23]; // [lat, lon] Teesside
const DEFAULT_ZOOM = 11;
const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
const MAP_REFRESH_INTERVAL_MS = 100;

const EMPTY_FC: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] };

// CartoDB Dark Matter style definition for MapLibre
const MAP_STYLE: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    'carto-dark': {
      type: 'raster',
      tiles: [
        'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      ],
      tileSize: 256,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
      maxzoom: 19,
    },
  },
  layers: [
    { id: 'background', type: 'raster', source: 'carto-dark' },
  ],
};

// ── Types ─────────────────────────────────────────────────────────────────────

interface NodeLink {
  peer_id: string;
  peer_name: string | null;
  observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number;
  count_peer_to_this: number;
}

interface PopupState {
  nodeId: string;
  lngLat: maplibregl.LngLatLike;
}

// Properties stored in GeoJSON features (must be JSON-serialisable)
interface NodeFeatureProps {
  node_id: string;
  name: string | null;
  role: number;
  is_online: boolean;
  is_stale: boolean;
  is_prohibited: boolean;
  is_inferred: boolean;
  hex_clash_state: 'offender' | 'relay' | null;
  visible: boolean;
  last_seen: string;
  public_key: string | null;
  advert_count: number | null;
  elevation_m: number | null;
  hardware_model: string | null;
}

export interface MapLibreMapProps {
  inferredNodes: MeshNode[];
  inferredActiveNodeIds: Set<string>;
  showLinks: boolean;
  showCoverage: boolean;
  showClientNodes: boolean;
  showHexClashes: boolean;
  maxHexClashHops: number;
  onMapReady?: (map: maplibregl.Map) => void;
}

interface ClashComputation {
  clashOffenderNodeIds: Set<string>;
  clashRelayIds: Set<string>;
  clashPathLines: Array<{ key: string; positions: [number, number][] }>;
  clashModeActive: boolean;
}

// ── GeoJSON builders ──────────────────────────────────────────────────────────

function circleLineString(
  lat: number,
  lon: number,
  radiusMeters: number,
  steps = 48,
): GeoJSON.Feature<GeoJSON.LineString> {
  const latRad = lat * (Math.PI / 180);
  const coords: [number, number][] = [];
  for (let i = 0; i <= steps; i++) {
    const angle = (i / steps) * 2 * Math.PI;
    const dLat = (radiusMeters / 111320) * Math.cos(angle);
    const dLon = (radiusMeters / (111320 * Math.cos(latRad))) * Math.sin(angle);
    coords.push([lon + dLon, lat + dLat]);
  }
  return { type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: {} };
}

function buildNodeGeoJSON(
  nodes: Map<string, MeshNode>,
  inferredNodes: MeshNode[],
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
  showClientNodes: boolean,
  clashOffenderIds: Set<string>,
  clashRelayIds: Set<string>,
  showHexClashes: boolean,
  pathNodeIds: Set<string> | null,
): GeoJSON.FeatureCollection {
  const now = Date.now();
  const features: GeoJSON.Feature[] = [];

  const addNode = (node: MeshNode, isInferred: boolean) => {
    if (!hasCoords(node)) return;
    const ageMs = now - new Date(node.last_seen).getTime();
    if (ageMs > FOURTEEN_DAYS_MS) return;

    const isClientNode = node.role === 1 || node.role === 3;
    if (isClientNode && !showClientNodes) return;

    const isStale = ageMs > SEVEN_DAYS_MS;
    const isProhibited = isProhibitedMapNode(node);
    const masked = maskNodePoint(node as MeshNode & { lat: number; lon: number }, hiddenCoordMask);

    let hexClashState: NodeFeatureProps['hex_clash_state'] = null;
    if (showHexClashes) {
      hexClashState = clashOffenderIds.has(node.node_id)
        ? 'offender'
        : clashRelayIds.has(node.node_id)
          ? 'relay'
          : null;
    }

    // Determine visibility (for filter-based show/hide)
    let visible = true;
    if (showHexClashes && (clashOffenderIds.size > 0 || clashRelayIds.size > 0)) {
      visible = clashOffenderIds.has(node.node_id) || clashRelayIds.has(node.node_id);
    } else if (pathNodeIds !== null) {
      visible = pathNodeIds.has(node.node_id.toLowerCase());
    }

    const props: NodeFeatureProps = {
      node_id: node.node_id,
      name: node.name ?? null,
      role: node.role ?? 2,
      is_online: node.is_online,
      is_stale: isStale,
      is_prohibited: isProhibited,
      is_inferred: isInferred,
      hex_clash_state: hexClashState,
      visible,
      last_seen: node.last_seen,
      public_key: node.public_key ?? null,
      advert_count: node.advert_count ?? null,
      elevation_m: node.elevation_m ?? null,
      hardware_model: node.hardware_model ?? null,
    };

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [masked[1], masked[0]] }, // [lon, lat]
      properties: props,
    });
  };

  for (const node of nodes.values()) addNode(node, false);
  for (const node of inferredNodes) addNode(node, true);

  return { type: 'FeatureCollection', features };
}

function buildPrivacyRingsGeoJSON(
  nodes: Map<string, MeshNode>,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = [];
  for (const node of nodes.values()) {
    if (!hasCoords(node) || !isProhibitedMapNode(node)) continue;
    const center = maskCircleCenter([node.lat!, node.lon!], hiddenCoordMask);
    features.push(circleLineString(center[0], center[1], HIDDEN_NODE_MASK_RADIUS_METERS));
  }
  return { type: 'FeatureCollection', features };
}

function buildCoverageGeoJSON(coverage: NodeCoverage[]): GeoJSON.FeatureCollection {
  if (coverage.length === 0) return EMPTY_FC;
  const features: GeoJSON.Feature[] = [];
  for (const c of coverage) {
    if (c.geom.type === 'Polygon' || c.geom.type === 'MultiPolygon') {
      features.push({
        type: 'Feature',
        geometry: c.geom as GeoJSON.Geometry,
        properties: { node_id: c.node_id },
      });
    }
  }
  return { type: 'FeatureCollection', features };
}

function buildClashLinesGeoJSON(
  lines: { key: string; positions: [number, number][] }[],
): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: lines.map((line) => ({
      type: 'Feature',
      geometry: {
        type: 'LineString',
        // positions are [lat, lon] — convert to [lon, lat] for GeoJSON
        coordinates: line.positions.map(([lat, lon]) => [lon, lat]),
      },
      properties: { key: line.key },
    })),
  };
}

function buildLinksGeoJSON(
  nodes: Map<string, MeshNode>,
  viablePairsArr: [string, string][],
  linkMetrics: Map<string, { itm_path_loss_db?: number | null }>,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = [];
  const seen = new Set<string>();

  for (const [aId, bId] of viablePairsArr) {
    const edgeId = linkKey(aId, bId);
    if (seen.has(edgeId)) continue;
    seen.add(edgeId);

    const a = nodes.get(aId);
    const b = nodes.get(bId);
    if (!hasCoords(a) || !hasCoords(b)) continue;

    const aMasked = maskNodePoint(a, hiddenCoordMask);
    const bMasked = maskNodePoint(b, hiddenCoordMask);
    const pathLoss = linkMetrics.get(edgeId)?.itm_path_loss_db ?? null;
    const distance = distKm(a, b);
    const color = pathLoss == null
      ? '#d1d5db'
      : pathLoss <= 130
        ? '#22c55e'
        : pathLoss <= 138
          ? '#fbbf24'
          : '#ef4444';

    const coordinates = distance > 0.02
      ? [[aMasked[1], aMasked[0]], [bMasked[1], bMasked[0]]]
      : [[aMasked[1], aMasked[0]], [bMasked[1] + 0.0018, bMasked[0] + 0.0018]];

    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates },
      properties: {
        key: edgeId,
        color,
        width: pathLoss == null ? 1.2 : pathLoss <= 130 ? 2.2 : pathLoss <= 138 ? 1.8 : 1.4,
        opacity: pathLoss == null ? 0.38 : pathLoss <= 130 ? 0.72 : pathLoss <= 138 ? 0.62 : 0.5,
      },
    });
  }

  return { type: 'FeatureCollection', features };
}

function distKm(a: MeshNode, b: MeshNode): number {
  if (!hasCoords(a) || !hasCoords(b)) return Number.POSITIVE_INFINITY;
  const midLat = ((a.lat + b.lat) / 2) * (Math.PI / 180);
  const dlat = (a.lat - b.lat) * 111;
  const dlon = (a.lon - b.lon) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

function buildCoverageByNodeId(coverage: NodeCoverage[]): Map<string, NodeCoverage> {
  const coverageByNodeId = new Map<string, NodeCoverage>();
  for (const item of coverage) coverageByNodeId.set(item.node_id, item);
  return coverageByNodeId;
}

function nodeRangeKm(nodeId: string, coverageByNodeId: Map<string, NodeCoverage>): number {
  const coverage = coverageByNodeId.get(nodeId);
  if (!coverage?.radius_m) return 50;
  return Math.min(80, Math.max(50, coverage.radius_m / 1000));
}

function pairInReceiveRange(a: MeshNode, b: MeshNode, coverageByNodeId: Map<string, NodeCoverage>): boolean {
  const distance = distKm(a, b);
  const range = Math.max(nodeRangeKm(a.node_id, coverageByNodeId), nodeRangeKm(b.node_id, coverageByNodeId));
  return distance <= range;
}

function computeClashData(
  nodes: Map<string, MeshNode>,
  coverage: NodeCoverage[],
  viablePairsArr: [string, string][],
  linkMetrics: Map<string, { itm_path_loss_db?: number | null }>,
  showHexClashes: boolean,
  maxHexClashHops: number,
  focusedNodeId: string | null,
  focusedPrefixNodeIds: Set<string> | null,
): ClashComputation {
  const coverageByNodeId = buildCoverageByNodeId(coverage);
  const nodesWithPos = Array.from(nodes.values()).filter(
    (node) => hasCoords(node) && (node.role === undefined || node.role === 2)
      && (Date.now() - new Date(node.last_seen).getTime()) < FOURTEEN_DAYS_MS,
  );

  const repeaterPrefixIds = new Map<string, string[]>();
  for (const node of nodesWithPos) {
    const prefix = node.node_id.slice(0, 2).toUpperCase();
    const existing = repeaterPrefixIds.get(prefix);
    if (existing) existing.push(node.node_id);
    else repeaterPrefixIds.set(prefix, [node.node_id]);
  }

  const clashAdjacency = new Map<string, Set<string>>();
  for (const [aId, bId] of viablePairsArr) {
    const a = nodes.get(aId);
    const b = nodes.get(bId);
    if (!hasCoords(a) || !hasCoords(b)) continue;
    const edgeKey = linkKey(aId, bId);
    const pathLoss = linkMetrics.get(edgeKey)?.itm_path_loss_db;
    if (pathLoss == null) continue;
    if (!pairInReceiveRange(a, b, coverageByNodeId)) continue;
    if (!clashAdjacency.has(aId)) clashAdjacency.set(aId, new Set());
    if (!clashAdjacency.has(bId)) clashAdjacency.set(bId, new Set());
    clashAdjacency.get(aId)?.add(bId);
    clashAdjacency.get(bId)?.add(aId);
  }

  const shortestPathWithinRelayHops = (fromId: string, toId: string): string[] | null => {
    if (fromId === toId) return [fromId];
    const maxEdges = Math.max(1, Math.floor(maxHexClashHops) + 1);
    const visited = new Set<string>([fromId]);
    const previous = new Map<string, string>();
    const queue: Array<{ id: string; edges: number }> = [{ id: fromId, edges: 0 }];

    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) break;
      if (current.edges >= maxEdges) continue;
      for (const next of clashAdjacency.get(current.id) ?? []) {
        if (visited.has(next)) continue;
        visited.add(next);
        previous.set(next, current.id);
        if (next === toId) {
          const path = [toId];
          let cursor = toId;
          while (previous.has(cursor)) {
            cursor = previous.get(cursor)!;
            path.unshift(cursor);
          }
          return path;
        }
        queue.push({ id: next, edges: current.edges + 1 });
      }
    }

    return null;
  };

  const activePaths: Array<{ key: string; nodeIds: string[]; offenderA: string; offenderB: string }> = [];
  if (showHexClashes) {
    for (const ids of repeaterPrefixIds.values()) {
      if (ids.length < 2) continue;
      for (let i = 0; i < ids.length - 1; i += 1) {
        for (let j = i + 1; j < ids.length; j += 1) {
          const fromId = ids[i]!;
          const toId = ids[j]!;
          const path = shortestPathWithinRelayHops(fromId, toId);
          if (!path || path.length < 2) continue;
          activePaths.push({
            key: `clash-${fromId.slice(0, 8)}-${toId.slice(0, 8)}-${path.length}`,
            nodeIds: path,
            offenderA: fromId,
            offenderB: toId,
          });
        }
      }
    }
  } else if (focusedNodeId && focusedPrefixNodeIds && focusedPrefixNodeIds.size >= 2) {
    for (const targetId of focusedPrefixNodeIds) {
      if (targetId === focusedNodeId) continue;
      const path = shortestPathWithinRelayHops(focusedNodeId, targetId);
      if (!path || path.length < 2) continue;
      activePaths.push({
        key: `focus-${focusedNodeId.slice(0, 8)}-${targetId.slice(0, 8)}-${path.length}`,
        nodeIds: path,
        offenderA: focusedNodeId,
        offenderB: targetId,
      });
    }
  }

  const clashOffenderNodeIds = new Set<string>();
  const clashVisibleNodeIds = new Set<string>();
  for (const path of activePaths) {
    clashOffenderNodeIds.add(path.offenderA);
    clashOffenderNodeIds.add(path.offenderB);
    for (const nodeId of path.nodeIds) clashVisibleNodeIds.add(nodeId);
  }

  const clashRelayIds = new Set<string>();
  for (const nodeId of clashVisibleNodeIds) {
    if (!clashOffenderNodeIds.has(nodeId)) clashRelayIds.add(nodeId);
  }

  const clashPathLines: Array<{ key: string; positions: [number, number][] }> = [];
  const edgeKeys = new Set<string>();
  for (const path of activePaths) {
    for (let i = 0; i < path.nodeIds.length - 1; i += 1) {
      const a = nodes.get(path.nodeIds[i]!);
      const b = nodes.get(path.nodeIds[i + 1]!);
      if (!hasCoords(a) || !hasCoords(b)) continue;
      const edgeKey = linkKey(a.node_id, b.node_id);
      if (edgeKeys.has(edgeKey)) continue;
      edgeKeys.add(edgeKey);
      const distance = distKm(a, b);
      clashPathLines.push({
        key: `${path.key}-${edgeKey}`,
        positions: distance > 0.02
          ? [[a.lat!, a.lon!], [b.lat!, b.lon!]]
          : [[a.lat!, a.lon!], [b.lat! + 0.0018, b.lon! + 0.0018]],
      });
    }
  }

  return {
    clashOffenderNodeIds,
    clashRelayIds,
    clashPathLines,
    clashModeActive: showHexClashes || Boolean(focusedPrefixNodeIds),
  };
}

// ── Popup helpers ─────────────────────────────────────────────────────────────

const GPU_ROLE_LABELS: Record<number, string> = {
  1: 'Companion Radio', 2: 'Repeater', 3: 'Room Server', 4: 'Sensor',
};

function timeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

// ── NodePopupContent ──────────────────────────────────────────────────────────

const NodePopupContent: React.FC<{
  props: NodeFeatureProps;
  lat: number;
  lon: number;
  links: NodeLink[] | null;
  onFocusSamePrefix: (nodeId: string) => void;
  samePrefixCount: number;
}> = ({ props, lat, lon, links, onFocusSamePrefix, samePrefixCount }) => {
  const isRepeater = props.role === undefined || props.role === 2;
  const ageMs = Date.now() - new Date(props.last_seen).getTime();
  const isStale = ageMs > SEVEN_DAYS_MS;
  const statusLabel = isStale ? 'STALE' : props.is_online ? 'ONLINE' : 'OFFLINE';
  const statusColor = isStale ? 'var(--danger)' : props.is_online ? 'var(--online)' : 'var(--offline)';
  const fallbackName = GPU_ROLE_LABELS[props.role ?? 2] ?? 'Unknown Device';
  const displayName = props.is_prohibited
    ? `Redacted ${fallbackName}`
    : (props.name ?? `Unknown ${fallbackName}`);

  return (
    <div className="node-popup">
      <div className="node-popup__name">{displayName}</div>
      {props.public_key && (
        <div className="node-popup__row">
          <span>Public key</span>
          <span className="node-popup__mono">{props.public_key}</span>
        </div>
      )}
      {!isRepeater && props.role !== undefined && (
        <div className="node-popup__row">
          <span>Type</span>
          <span>{GPU_ROLE_LABELS[props.role] ?? 'Unknown'}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Status</span>
        <span style={{ color: statusColor }}>{statusLabel}</span>
      </div>
      {props.hardware_model && (
        <div className="node-popup__row">
          <span>Hardware</span>
          <span>{props.hardware_model}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Last seen</span>
        <span>{timeAgo(props.last_seen)}</span>
      </div>
      {props.advert_count !== null && props.advert_count !== undefined && (
        <div className="node-popup__row">
          <span>Times seen</span>
          <span>{props.advert_count}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Position</span>
        <span>{props.is_prohibited ? 'Redacted' : `${lat.toFixed(5)}, ${lon.toFixed(5)}`}</span>
      </div>
      {props.is_prohibited && (
        <div className="node-popup__row">
          <span>Location</span>
          <span>Redacted within 1 mile radius</span>
        </div>
      )}
      {props.elevation_m !== null && props.elevation_m !== undefined && (
        <div className="node-popup__row">
          <span>Elevation</span>
          <span>{Math.round(props.elevation_m)} m ASL</span>
        </div>
      )}
      {isRepeater && samePrefixCount > 1 && (
        <div className="node-popup__row" style={{ marginTop: 6 }}>
          <button
            type="button"
            className="node-popup__action-btn"
            onClick={() => onFocusSamePrefix(props.node_id)}
          >
            Focus same-prefix nodes
          </button>
        </div>
      )}
      {!isRepeater && links === null && (
        <div className="node-popup__neighbours-loading">Loading neighbours…</div>
      )}
      {!isRepeater && links !== null && links.length > 0 && (
        <div className="node-popup__neighbours">
          <div className="node-popup__neighbours-title">Confirmed neighbours</div>
          {links.map((lk) => {
            const tx = lk.count_this_to_peer > 0;
            const rx = lk.count_peer_to_this > 0;
            const arrow = tx && rx ? '↔' : tx ? '→' : '←';
            return (
              <div key={lk.peer_id} className="node-popup__neighbour-row">
                <span className="node-popup__neighbour-name">
                  {arrow} {lk.peer_name ?? lk.peer_id.slice(0, 8)}
                </span>
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
  );
};

// ── Main Component ────────────────────────────────────────────────────────────

export const MapLibreMap: React.FC<MapLibreMapProps> = ({
  inferredNodes,
  inferredActiveNodeIds: _inferredActiveNodeIds,
  showLinks,
  showCoverage,
  showClientNodes,
  showHexClashes,
  maxHexClashHops,
  onMapReady,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const mapLoadedRef = useRef(false);
  const mlPopupRef = useRef<maplibregl.Popup | null>(null);
  const popupContainerRef = useRef<HTMLDivElement>(document.createElement('div'));
  const nodesRef = useRef(nodeStore.getState().nodes);
  const coverageRef = useRef(coverageStore.getState().coverage);
  const viablePairsRef = useRef(linkStateStore.getState().viablePairsArr);
  const linkMetricsRef = useRef(linkStateStore.getState().linkMetrics);
  const inferredNodesRef = useRef(inferredNodes);
  const showLinksRef = useRef(showLinks);
  const showCoverageRef = useRef(showCoverage);
  const showClientNodesRef = useRef(showClientNodes);
  const showHexClashesRef = useRef(showHexClashes);
  const maxHexClashHopsRef = useRef(maxHexClashHops);
  const pathNodeIdsRef = useRef(useOverlayStore.getState().pathNodeIds);
  const hiddenCoordMaskRef = useRef<Map<string, HiddenMaskGeometry>>(new Map());
  const refreshTimerRef = useRef<number | null>(null);
  const popupStateRef = useRef<PopupState | null>(null);

  const [popupState, setPopupState] = useState<PopupState | null>(null);
  const [popupLinks, setPopupLinks] = useState<NodeLink[] | null>(null);
  const [focusedPrefix, setFocusedPrefix] = useState<string | null>(null);
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  const [focusedPrefixNodeIds, setFocusedPrefixNodeIds] = useState<Set<string> | null>(null);
  const [popupVersion, setPopupVersion] = useState(0);
  const focusTimerRef = useRef<number | null>(null);

  // -- Focus mode (same-prefix highlight) ------------------------------------

  const clearFocusTimer = useCallback(() => {
    if (focusTimerRef.current !== null) {
      window.clearTimeout(focusTimerRef.current);
      focusTimerRef.current = null;
    }
  }, []);

  const refreshMapSources = useCallback(() => {
    if (!mapLoadedRef.current || !mapRef.current) return;

    const nodes = nodesRef.current;
    const coverage = coverageRef.current;
    const viablePairsArr = viablePairsRef.current;
    const linkMetrics = linkMetricsRef.current;
    const currentPathNodeIds = pathNodeIdsRef.current;
    const currentHiddenCoordMask = buildHiddenCoordMask(nodes.values());
    hiddenCoordMaskRef.current = currentHiddenCoordMask;

    const clash = computeClashData(
      nodes,
      coverage,
      viablePairsArr,
      linkMetrics,
      showHexClashesRef.current,
      maxHexClashHopsRef.current,
      focusedNodeId,
      focusedPrefixNodeIds,
    );

    const nodeGeoJSON = buildNodeGeoJSON(
      nodes,
      inferredNodesRef.current,
      currentHiddenCoordMask,
      showClientNodesRef.current,
      clash.clashOffenderNodeIds,
      clash.clashRelayIds,
      clash.clashModeActive,
      clash.clashModeActive ? null : currentPathNodeIds,
    );
    (mapRef.current.getSource('nodes') as maplibregl.GeoJSONSource | undefined)?.setData(nodeGeoJSON);

    const privacyGeoJSON = buildPrivacyRingsGeoJSON(nodes, currentHiddenCoordMask);
    (mapRef.current.getSource('privacy-rings') as maplibregl.GeoJSONSource | undefined)?.setData(privacyGeoJSON);

    const linksGeoJSON = showLinksRef.current
      ? buildLinksGeoJSON(nodes, viablePairsArr, linkMetrics, currentHiddenCoordMask)
      : EMPTY_FC;
    (mapRef.current.getSource('viable-links') as maplibregl.GeoJSONSource | undefined)?.setData(linksGeoJSON);
    mapRef.current.setLayoutProperty('viable-links-layer', 'visibility', showLinksRef.current ? 'visible' : 'none');

    const coverageGeoJSON = showCoverageRef.current && !clash.clashModeActive
      ? buildCoverageGeoJSON(coverage)
      : EMPTY_FC;
    (mapRef.current.getSource('coverage') as maplibregl.GeoJSONSource | undefined)?.setData(coverageGeoJSON);
    mapRef.current.setLayoutProperty('coverage-fill', 'visibility',
      showCoverageRef.current && !clash.clashModeActive ? 'visible' : 'none');

    const clashGeoJSON = clash.clashModeActive && clash.clashPathLines.length > 0
      ? buildClashLinesGeoJSON(clash.clashPathLines)
      : EMPTY_FC;
    (mapRef.current.getSource('clash-lines') as maplibregl.GeoJSONSource | undefined)?.setData(clashGeoJSON);
    mapRef.current.setLayoutProperty('clash-lines-layer', 'visibility',
      clash.clashModeActive && clash.clashPathLines.length > 0 ? 'visible' : 'none');
  }, [focusedNodeId, focusedPrefixNodeIds]);

  const scheduleRefresh = useCallback(() => {
    if (refreshTimerRef.current !== null) return;
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null;
      refreshMapSources();
    }, MAP_REFRESH_INTERVAL_MS);
  }, [refreshMapSources]);

  const handleFocusSamePrefix = useCallback((nodeId: string) => {
    const prefix = nodeId.slice(0, 2).toUpperCase();
    const ids = Array.from(nodesRef.current.values())
      .filter((node) => hasCoords(node) && (node.role === undefined || node.role === 2))
      .filter((node) => node.node_id.slice(0, 2).toUpperCase() === prefix)
      .map((node) => node.node_id);
    clearFocusTimer();
    setFocusedPrefix(prefix);
    setFocusedNodeId(nodeId);
    setFocusedPrefixNodeIds(new Set(ids.length > 0 ? ids : [nodeId]));
    // Auto-clear after 10s
    focusTimerRef.current = window.setTimeout(() => {
      setFocusedPrefix(null);
      setFocusedNodeId(null);
      setFocusedPrefixNodeIds(null);
      focusTimerRef.current = null;
    }, 10_000);
  }, [clearFocusTimer]);

  useEffect(() => () => {
    clearFocusTimer();
    if (refreshTimerRef.current !== null) {
      window.clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
  }, [clearFocusTimer]);

  // -- Map initialisation (runs once on mount) --------------------------------

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: [DEFAULT_CENTER[1], DEFAULT_CENTER[0]], // [lon, lat]
      zoom: DEFAULT_ZOOM,
      attributionControl: false,
    });

    map.on('load', () => {
      mapLoadedRef.current = true;

      // ── Node dots source + layer ───────────────────────────────────────────
      map.addSource('nodes', { type: 'geojson', data: EMPTY_FC });

      map.addLayer({
        id: 'node-dots',
        type: 'circle',
        source: 'nodes',
        filter: ['==', ['get', 'visible'], true],
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 3, 9, 4, 11, 5, 13, 7, 16, 9,
          ],
          'circle-color': [
            'case',
            ['==', ['get', 'hex_clash_state'], 'offender'], '#ef4444',
            ['==', ['get', 'hex_clash_state'], 'relay'], '#22c55e',
            ['get', 'is_prohibited'], '#f59e0b',
            ['get', 'is_inferred'], '#7dd3fc',
            ['get', 'is_stale'], '#6b7280',
            ['!', ['get', 'is_online']], '#6b7280',
            ['==', ['get', 'role'], 1], '#ff9f43',
            ['==', ['get', 'role'], 3], '#a78bfa',
            ['==', ['get', 'role'], 4], '#34d399',
            '#00c4ff', // repeater (role 2 / default)
          ],
          'circle-opacity': [
            'case',
            ['get', 'is_stale'], 0.4,
            ['!', ['get', 'is_online']], 0.4,
            ['get', 'is_inferred'], 0.7,
            1.0,
          ],
          'circle-stroke-width': [
            'case',
            ['get', 'is_prohibited'], 1.4,
            0,
          ],
          'circle-stroke-color': '#f59e0b',
          'circle-stroke-opacity': 0.7,
        },
      });

      // ── Privacy rings source + layer ───────────────────────────────────────
      map.addSource('privacy-rings', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'privacy-rings-layer',
        type: 'line',
        source: 'privacy-rings',
        paint: {
          'line-color': '#f59e0b',
          'line-width': 1.4,
          'line-opacity': 0.55,
          'line-dasharray': [4, 6],
        },
      });

      // ── Viable links source + layer ───────────────────────────────────────
      map.addSource('viable-links', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'viable-links-layer',
        type: 'line',
        source: 'viable-links',
        layout: {
          visibility: 'none',
          'line-cap': 'round',
          'line-join': 'round',
        },
        paint: {
          'line-color': ['get', 'color'],
          'line-width': ['get', 'width'],
          'line-opacity': ['get', 'opacity'],
        },
      });

      // ── Coverage source + layer ────────────────────────────────────────────
      map.addSource('coverage', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'coverage-fill',
        type: 'fill',
        source: 'coverage',
        layout: { visibility: 'none' },
        paint: {
          'fill-color': '#22c55e',
          'fill-opacity': 0.18,
        },
      });

      // ── Clash lines source + layer ─────────────────────────────────────────
      map.addSource('clash-lines', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'clash-lines-layer',
        type: 'line',
        source: 'clash-lines',
        layout: { visibility: 'none' },
        paint: {
          'line-color': '#f97316',
          'line-width': 2.2,
          'line-opacity': 0.9,
        },
      });

      // ── Click handler ──────────────────────────────────────────────────────
      map.on('click', 'node-dots', (e) => {
        const feature = e.features?.[0];
        if (!feature) return;
        const props = feature.properties as NodeFeatureProps;
        // MapLibre serialises properties to JSON strings for non-primitive types,
        // but all our props are primitives so this is safe.
        const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
        setPopupLinks(null);
        setPopupState({ nodeId: props.node_id, lngLat: { lng: coords[0], lat: coords[1] } });
      });

      // Make cursor a pointer over node dots
      map.on('mouseenter', 'node-dots', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'node-dots', () => {
        map.getCanvas().style.cursor = '';
      });

      mapRef.current = map;
      onMapReady?.(map);
      refreshMapSources();
    });

    return () => {
      mapLoadedRef.current = false;
      map.remove();
      mapRef.current = null;
    };
  }, [onMapReady, refreshMapSources]);

  // -- Imperative source updates ---------------------------------------------

  useEffect(() => {
    inferredNodesRef.current = inferredNodes;
    scheduleRefresh();
  }, [inferredNodes, scheduleRefresh]);

  useEffect(() => {
    showLinksRef.current = showLinks;
    scheduleRefresh();
  }, [showLinks, scheduleRefresh]);

  useEffect(() => {
    showCoverageRef.current = showCoverage;
    scheduleRefresh();
  }, [showCoverage, scheduleRefresh]);

  useEffect(() => {
    showClientNodesRef.current = showClientNodes;
    scheduleRefresh();
  }, [showClientNodes, scheduleRefresh]);

  useEffect(() => {
    showHexClashesRef.current = showHexClashes;
    scheduleRefresh();
  }, [showHexClashes, scheduleRefresh]);

  useEffect(() => {
    maxHexClashHopsRef.current = maxHexClashHops;
    scheduleRefresh();
  }, [maxHexClashHops, scheduleRefresh]);

  useEffect(() => {
    popupStateRef.current = popupState;
  }, [popupState]);

  useEffect(() => {
    const unsubscribeNodes = nodeStore.subscribe(() => {
      nodesRef.current = nodeStore.getState().nodes;
      scheduleRefresh();
      if (popupStateRef.current) setPopupVersion((value) => value + 1);
    });
    const unsubscribeCoverage = coverageStore.subscribe(() => {
      coverageRef.current = coverageStore.getState().coverage;
      scheduleRefresh();
    });
    const unsubscribeLinks = linkStateStore.subscribe(() => {
      const linkState = linkStateStore.getState();
      viablePairsRef.current = linkState.viablePairsArr;
      linkMetricsRef.current = linkState.linkMetrics;
      scheduleRefresh();
    });
    const unsubscribeOverlay = useOverlayStore.subscribe((overlayState) => {
      if (overlayState.pathNodeIds === pathNodeIdsRef.current) return;
      pathNodeIdsRef.current = overlayState.pathNodeIds;
      scheduleRefresh();
    });

    return () => {
      unsubscribeNodes();
      unsubscribeCoverage();
      unsubscribeLinks();
      unsubscribeOverlay();
    };
  }, [scheduleRefresh]);

  useEffect(() => {
    scheduleRefresh();
  }, [focusedNodeId, focusedPrefixNodeIds, scheduleRefresh]);

  // -- Popup management ------------------------------------------------------

  // Find the full MeshNode from nodeId (checks nodes and inferredNodes)
  const getNode = useCallback((nodeId: string): MeshNode | undefined => {
    return nodesRef.current.get(nodeId) ?? inferredNodesRef.current.find((node) => node.node_id === nodeId);
  }, []);

  // Fetch neighbour links for non-repeater node popups
  useEffect(() => {
    if (!popupState) return;
    const node = getNode(popupState.nodeId);
    if (!node || node.role === undefined || node.role === 2) return;
    // Non-repeater — fetch neighbours
    setPopupLinks(null);
    fetch(`/api/nodes/${popupState.nodeId}/links`)
      .then((r) => r.json() as Promise<NodeLink[]>)
      .then(setPopupLinks)
      .catch(() => setPopupLinks([]));
  }, [popupState?.nodeId, getNode]); // eslint-disable-line react-hooks/exhaustive-deps

  // Show/update/close the MapLibre popup when popupState changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current) return;

    if (!popupState) {
      mlPopupRef.current?.remove();
      return;
    }

    if (!mlPopupRef.current) {
      mlPopupRef.current = new maplibregl.Popup({ maxWidth: '280px', closeOnClick: false })
        .setDOMContent(popupContainerRef.current)
        .on('close', () => setPopupState(null));
    }

    mlPopupRef.current.setLngLat(popupState.lngLat).addTo(map);
  }, [popupState]);

  // Resolve popup props from current nodes map
  const popupNodeProps = useMemo((): NodeFeatureProps | null => {
    if (!popupState) return null;
    const node = getNode(popupState.nodeId);
    if (!node || !hasCoords(node)) return null;
    const now = Date.now();
    const ageMs = now - new Date(node.last_seen).getTime();
    const masked = maskNodePoint(node as MeshNode & { lat: number; lon: number }, hiddenCoordMaskRef.current);
    return {
      node_id: node.node_id,
      name: node.name ?? null,
      role: node.role ?? 2,
      is_online: node.is_online,
      is_stale: ageMs > SEVEN_DAYS_MS,
      is_prohibited: isProhibitedMapNode(node),
      is_inferred: !!node.is_inferred,
      hex_clash_state: null,
      visible: true,
      last_seen: node.last_seen,
      public_key: node.public_key ?? null,
      advert_count: node.advert_count ?? null,
      elevation_m: node.elevation_m ?? null,
      hardware_model: node.hardware_model ?? null,
      // Store masked position temporarily in unused fields:
      // (we pass lat/lon separately to NodePopupContent)
      _maskedLat: masked[0],
      _maskedLon: masked[1],
    } as NodeFeatureProps & { _maskedLat: number; _maskedLon: number };
  }, [popupState, popupVersion, getNode]);

  const popupSamePrefixCount = useMemo(() => {
    if (!popupState) return 1;
    const prefix = popupState.nodeId.slice(0, 2).toUpperCase();
    return Array.from(nodesRef.current.values()).filter(
      (node) => hasCoords(node)
        && (node.role === undefined || node.role === 2)
        && node.node_id.slice(0, 2).toUpperCase() === prefix,
    ).length || 1;
  }, [popupState, popupVersion]);

  // -- Render ----------------------------------------------------------------

  return (
    <div className="map-area" style={{ position: 'relative', width: '100%', height: '100%' }}>
      <NodeSearch map={mapRef.current} />
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />

      {/* Popup content rendered into the MapLibre popup's DOM node via portal */}
      {popupState && popupNodeProps && createPortal(
        <NodePopupContent
          props={popupNodeProps}
          lat={(popupNodeProps as NodeFeatureProps & { _maskedLat: number })._maskedLat}
          lon={(popupNodeProps as NodeFeatureProps & { _maskedLon: number })._maskedLon}
          links={popupLinks}
          onFocusSamePrefix={handleFocusSamePrefix}
          samePrefixCount={popupSamePrefixCount}
        />,
        popupContainerRef.current,
      )}

      {/* Focus mode indicator */}
      {focusedPrefix && (
        <div
          style={{
            position: 'absolute', top: 10, left: '50%', transform: 'translateX(-50%)',
            background: 'rgba(0,0,0,0.75)', color: '#fff', padding: '4px 10px',
            borderRadius: 4, fontSize: 12, pointerEvents: 'none', zIndex: 10,
          }}
        >
          Showing {focusedPrefix}xx prefix nodes
        </div>
      )}
    </div>
  );
};
