import type { MeshNode } from '../../hooks/useNodes.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import {
  buildHiddenCoordMask,
  hasCoords,
  isProhibitedMapNode,
  maskCircleCenter,
  maskNodePoint,
  HIDDEN_NODE_MASK_RADIUS_METERS,
  linkKey,
} from '../../utils/pathing.js';
import {
  EMPTY_FC,
  NODE_HIDE_AFTER_MS,
  NODE_STALE_AFTER_MS,
  LINK_AMBER_THRESHOLD_DB,
  LINK_GREEN_THRESHOLD_DB,
  MAP_OVERLAY_COLORS,
} from './mapConfig.js';
import type { MapTheme } from './mapConfig.js';
import type { ClashComputation, NodeFeatureProps, PlannedRepeater } from './types.js';

export type MapSourceDirtyFlags = {
  nodes: boolean;
  privacy: boolean;
  links: boolean;
  clash: boolean;
  plannedLinks: boolean;
};

export const MAX_INFERRED_NODE_FEATURES = 2_000;

export const ALL_MAP_SOURCE_DIRTY_FLAGS: MapSourceDirtyFlags = {
  nodes: true,
  privacy: true,
  links: true,
  clash: true,
  plannedLinks: true,
};

export function mergeMapSourceDirtyFlags(
  current: MapSourceDirtyFlags,
  incoming: Partial<MapSourceDirtyFlags>,
): MapSourceDirtyFlags {
  return {
    nodes: current.nodes || !!incoming.nodes,
    privacy: current.privacy || !!incoming.privacy,
    links: current.links || !!incoming.links,
    clash: current.clash || !!incoming.clash,
    plannedLinks: current.plannedLinks || !!incoming.plannedLinks,
  };
}

function circleLineString(
  lat: number,
  lon: number,
  radiusMeters: number,
  steps = 48,
): GeoJSON.Feature<GeoJSON.LineString> {
  const latRad = lat * (Math.PI / 180);
  const coords: [number, number][] = [];
  for (let i = 0; i <= steps; i += 1) {
    const angle = (i / steps) * 2 * Math.PI;
    const dLat = (radiusMeters / 111320) * Math.cos(angle);
    const dLon = (radiusMeters / (111320 * Math.cos(latRad))) * Math.sin(angle);
    coords.push([lon + dLon, lat + dLat]);
  }
  return { type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: {} };
}

export function distKm(a: MeshNode, b: MeshNode): number {
  if (!hasCoords(a) || !hasCoords(b)) return Number.POSITIVE_INFINITY;
  const midLat = ((a.lat + b.lat) / 2) * (Math.PI / 180);
  const dlat = (a.lat - b.lat) * 111;
  const dlon = (a.lon - b.lon) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

export function buildNodeGeoJSON(
  nodes: Map<string, MeshNode>,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
  showClientNodes: boolean,
  showLinks: boolean,
  viableLinkNodeIds: Set<string>,
  clashOffenderIds: Set<string>,
  clashRelayIds: Set<string>,
  showHexClashes: boolean,
  pathNodeIds: Set<string> | null,
  replayNodeIds: Set<string> | null = null,
  staleCutoffMs = Date.now(),
  inferredNodes: readonly MeshNode[] = [],
  inferredActiveNodeIds: ReadonlySet<string> = new Set<string>(),
): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = [];
  const activeInferredIds = new Set(
    Array.from(inferredActiveNodeIds, (nodeId) => nodeId.trim().toUpperCase()),
  );

  const addNode = (node: MeshNode, explicitlyInferred = false) => {
    if (!hasCoords(node)) return;
    if (explicitlyInferred && isProhibitedMapNode(node)) return;
    const ageMs = staleCutoffMs - new Date(node.last_seen).getTime();
    const isLinkOnlyStale = ageMs > NODE_HIDE_AFTER_MS
      && showLinks
      && viableLinkNodeIds.has(node.node_id.toLowerCase());
    if (ageMs > NODE_HIDE_AFTER_MS && !isLinkOnlyStale) return;

    const isClientNode = node.role === 1 || node.role === 3;
    if (isClientNode && !showClientNodes) return;

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

    let visible = true;
    if (showHexClashes && (clashOffenderIds.size > 0 || clashRelayIds.size > 0)) {
      visible = clashOffenderIds.has(node.node_id) || clashRelayIds.has(node.node_id);
    } else if (pathNodeIds !== null) {
      // Live Path focus used to hide everything not on the active route, which
      // made the map feel empty. Keep all repeaters (role 2 / default) and
      // sensors on-map; still surface any non-repeater hop that is on the path.
      const role = node.role ?? 2;
      const keepAlways = role === 2 || role === 4;
      visible = keepAlways || pathNodeIds.size === 0 || pathNodeIds.has(node.node_id.toLowerCase());
    }

    const props: NodeFeatureProps = {
      node_id: node.node_id,
      name: node.name ?? null,
      role: node.role ?? 2,
      is_online: node.is_online,
      is_stale: ageMs > NODE_STALE_AFTER_MS,
      is_link_only_stale: isLinkOnlyStale,
      is_prohibited: isProhibited,
      is_inferred: explicitlyInferred || activeInferredIds.has(node.node_id.trim().toUpperCase()),
      replay_active: replayNodeIds?.has(node.node_id.toLowerCase()) ?? false,
      replay_mode: replayNodeIds !== null,
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
      geometry: { type: 'Point', coordinates: [masked[1], masked[0]] },
      properties: props,
    });
  };

  for (const node of nodes.values()) addNode(node);
  const existingIds = new Set(
    Array.from(nodes.values(), (node) => node.node_id.trim().toUpperCase()),
  );
  let addedInferred = 0;
  for (const node of inferredNodes) {
    if (addedInferred >= MAX_INFERRED_NODE_FEATURES) break;
    if (existingIds.has(node.node_id.trim().toUpperCase())) continue;
    const before = features.length;
    addNode(node, true);
    if (features.length > before) addedInferred += 1;
  }

  return { type: 'FeatureCollection', features };
}

export function buildPrivacyRingsGeoJSON(
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

/** GeoJSON for planned coverage polygons — rendered in teal/indigo/purple to distinguish from real coverage. */
export function buildPlannedCoverageGeoJSON(repeaters: PlannedRepeater[]): GeoJSON.FeatureCollection {
  const ready = repeaters.filter((r) => r.status === 'ready' && r.coverage);
  if (ready.length === 0) return EMPTY_FC;
  const features: GeoJSON.Feature[] = [];
  for (const repeater of ready) {
    const item = repeater.coverage!;
    const strengthGeoms = item.strength_geoms;
    if (strengthGeoms) {
      for (const band of ['red', 'amber', 'green'] as const) {
        const geom = strengthGeoms[band];
        if (!geom) continue;
        if (geom.type === 'Polygon' || geom.type === 'MultiPolygon') {
          features.push({
            type: 'Feature',
            geometry: geom as GeoJSON.Geometry,
            properties: { plan_id: repeater.id, band },
          });
        }
      }
      continue;
    }
    if (item.geom.type === 'Polygon' || item.geom.type === 'MultiPolygon') {
      features.push({
        type: 'Feature',
        geometry: item.geom as GeoJSON.Geometry,
        properties: { plan_id: repeater.id, band: 'green' },
      });
    }
  }
  return { type: 'FeatureCollection', features };
}

/** GeoJSON point features for planned repeater pins on the map. */
export function buildPlannedPinGeoJSON(repeaters: PlannedRepeater[]): GeoJSON.FeatureCollection {
  if (repeaters.length === 0) return EMPTY_FC;
  return {
    type: 'FeatureCollection',
    features: repeaters.map((r) => {
      const head = r.status === 'ready' ? 'Planned' : r.status === 'error' ? 'Failed' : 'Computing…';
      return {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [r.lon, r.lat] },
        properties: {
          plan_id: r.id,
          status: r.status,
          // Coordinates shown directly under the pin so the placement is visible at a glance.
          label: `${head}\n${r.lat.toFixed(5)}, ${r.lon.toFixed(5)}`,
        },
      };
    }),
  };
}

/**
 * GeoJSON link lines from planned repeaters to their predicted peer repeaters.
 * Peer endpoints are resolved (and privacy-masked) from the live node map so the
 * lines honour the same redaction rules as real links.
 */
export function buildPlannedLinksGeoJSON(
  repeaters: PlannedRepeater[],
  nodes: Map<string, MeshNode>,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
  theme: MapTheme = 'dark',
): GeoJSON.FeatureCollection {
  const colors = MAP_OVERLAY_COLORS[theme];
  const features: GeoJSON.Feature[] = [];
  for (const repeater of repeaters) {
    if (repeater.status !== 'ready') continue;
    const links = repeater.coverage?.predicted_links;
    if (!links || links.length === 0) continue;
    for (const link of links) {
      const peer = nodes.get(link.peer_id);
      if (!hasCoords(peer)) continue;
      const peerMasked = maskNodePoint(peer, hiddenCoordMask);
      const pathLoss = link.itm_path_loss_db;
      const color = pathLoss == null
        ? colors.linkUnknown
        : pathLoss <= LINK_GREEN_THRESHOLD_DB
          ? colors.linkGood
          : pathLoss <= LINK_AMBER_THRESHOLD_DB
            ? colors.linkMarginal
            : colors.linkPoor;
      features.push({
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [[repeater.lon, repeater.lat], [peerMasked[1], peerMasked[0]]],
        },
        properties: {
          key: `${repeater.id}:${link.peer_id}`,
          color,
          width: pathLoss == null ? 1.8 : pathLoss <= LINK_GREEN_THRESHOLD_DB ? 3 : pathLoss <= LINK_AMBER_THRESHOLD_DB ? 2.1 : 1.4,
        },
      });
    }
  }
  return features.length > 0 ? { type: 'FeatureCollection', features } : EMPTY_FC;
}

export function buildLinksGeoJSON(
  nodes: Map<string, MeshNode>,
  viablePairsArr: [string, string][],
  linkMetrics: Map<string, { itm_path_loss_db?: number | null }>,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
  theme: MapTheme = 'dark',
): GeoJSON.FeatureCollection {
  const colors = MAP_OVERLAY_COLORS[theme];
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
      ? colors.linkUnknown
      : pathLoss <= LINK_GREEN_THRESHOLD_DB
        ? colors.linkGood
        : pathLoss <= LINK_AMBER_THRESHOLD_DB
          ? colors.linkMarginal
          : colors.linkPoor;

    const coordinates = distance > 0.02
      ? [[aMasked[1], aMasked[0]], [bMasked[1], bMasked[0]]]
      : [[aMasked[1], aMasked[0]], [bMasked[1] + 0.0018, bMasked[0] + 0.0018]];

    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates },
      properties: {
        key: edgeId,
        color,
        width: pathLoss == null ? 1.8 : pathLoss <= LINK_GREEN_THRESHOLD_DB ? 3 : pathLoss <= LINK_AMBER_THRESHOLD_DB ? 2.1 : 1.4,
        opacity: pathLoss == null ? 0.75 : 0.9,
      },
    });
  }

  return { type: 'FeatureCollection', features };
}

function pairInReceiveRange(
  a: MeshNode,
  b: MeshNode,
): boolean {
  // Clash adjacency uses observed viable links and a fixed conservative
  // presentation bound. It must never recover range from rejected per-node
  // viewshed geometry.
  return distKm(a, b) <= 50;
}

export function computeClashData(
  nodes: Map<string, MeshNode>,
  viablePairsArr: [string, string][],
  linkMetrics: Map<string, { itm_path_loss_db?: number | null }>,
  showHexClashes: boolean,
  maxHexClashHops: number,
  focusedNodeId: string | null,
  focusedPrefixNodeIds: Set<string> | null,
  staleCutoffMs = Date.now(),
): ClashComputation {
  const nodesWithPos = Array.from(nodes.values()).filter(
    (node) => hasCoords(node)
      && (node.role === undefined || node.role === 2)
      && (staleCutoffMs - new Date(node.last_seen).getTime()) < NODE_STALE_AFTER_MS,
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
    if (!pairInReceiveRange(a, b)) continue;
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

export function buildHiddenMask(nodes: Map<string, MeshNode>): Map<string, HiddenMaskGeometry> {
  return buildHiddenCoordMask(nodes.values());
}
