import type { MeshNode } from '../../hooks/useNodes.js';
import type { NodeCoverage } from '../../hooks/useCoverage.js';
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
  FOURTEEN_DAYS_MS,
  LINK_AMBER_THRESHOLD_DB,
  LINK_GREEN_THRESHOLD_DB,
} from './mapConfig.js';
import type { ClashComputation, NodeFeatureProps } from './types.js';

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
  inferredNodes: MeshNode[],
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
  showClientNodes: boolean,
  showLinks: boolean,
  viableLinkNodeIds: Set<string>,
  clashOffenderIds: Set<string>,
  clashRelayIds: Set<string>,
  showHexClashes: boolean,
  pathNodeIds: Set<string> | null,
  staleCutoffMs = Date.now(),
): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = [];

  const addNode = (node: MeshNode, isInferred: boolean) => {
    if (!hasCoords(node)) return;
    const ageMs = staleCutoffMs - new Date(node.last_seen).getTime();
    const isLinkOnlyStale = ageMs > FOURTEEN_DAYS_MS
      && showLinks
      && viableLinkNodeIds.has(node.node_id.toLowerCase());
    if (ageMs > FOURTEEN_DAYS_MS && !isLinkOnlyStale) return;

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
      visible = pathNodeIds.has(node.node_id.toLowerCase());
    }

    const props: NodeFeatureProps = {
      node_id: node.node_id,
      name: node.name ?? null,
      role: node.role ?? 2,
      is_online: node.is_online,
      is_stale: ageMs > 7 * 24 * 60 * 60 * 1000,
      is_link_only_stale: isLinkOnlyStale,
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
      geometry: { type: 'Point', coordinates: [masked[1], masked[0]] },
      properties: props,
    });
  };

  for (const node of nodes.values()) addNode(node, false);
  for (const node of inferredNodes) addNode(node, true);

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

export function buildCoverageGeoJSON(coverage: NodeCoverage[]): GeoJSON.FeatureCollection {
  if (coverage.length === 0) return EMPTY_FC;
  const features: GeoJSON.Feature[] = [];
  for (const item of coverage) {
    const strengthGeoms = item.strength_geoms;
    if (strengthGeoms) {
      for (const band of ['red', 'amber', 'green'] as const) {
        const geom = strengthGeoms[band];
        if (!geom) continue;
        if (geom.type === 'Polygon' || geom.type === 'MultiPolygon') {
          features.push({
            type: 'Feature',
            geometry: geom as GeoJSON.Geometry,
            properties: { node_id: item.node_id, band },
          });
        }
      }
      continue;
    }

    if (item.geom.type === 'Polygon' || item.geom.type === 'MultiPolygon') {
      features.push({
        type: 'Feature',
        geometry: item.geom as GeoJSON.Geometry,
        properties: { node_id: item.node_id, band: 'green' },
      });
    }
  }
  return { type: 'FeatureCollection', features };
}

export function buildClashLinesGeoJSON(
  lines: { key: string; positions: [number, number][] }[],
): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: lines.map((line) => ({
      type: 'Feature',
      geometry: {
        type: 'LineString',
        coordinates: line.positions.map(([lat, lon]) => [lon, lat]),
      },
      properties: { key: line.key },
    })),
  };
}

export function buildLinksGeoJSON(
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
      : pathLoss <= LINK_GREEN_THRESHOLD_DB
        ? '#22c55e'
        : pathLoss <= LINK_AMBER_THRESHOLD_DB
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
        width: pathLoss == null ? 1.2 : pathLoss <= LINK_GREEN_THRESHOLD_DB ? 2.2 : pathLoss <= LINK_AMBER_THRESHOLD_DB ? 1.8 : 1.4,
        opacity: pathLoss == null ? 0.38 : pathLoss <= LINK_GREEN_THRESHOLD_DB ? 0.72 : pathLoss <= LINK_AMBER_THRESHOLD_DB ? 0.62 : 0.5,
      },
    });
  }

  return { type: 'FeatureCollection', features };
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

function pairInReceiveRange(
  a: MeshNode,
  b: MeshNode,
  coverageByNodeId: Map<string, NodeCoverage>,
): boolean {
  const distance = distKm(a, b);
  const range = Math.max(
    nodeRangeKm(a.node_id, coverageByNodeId),
    nodeRangeKm(b.node_id, coverageByNodeId),
  );
  return distance <= range;
}

export function computeClashData(
  nodes: Map<string, MeshNode>,
  coverage: NodeCoverage[],
  viablePairsArr: [string, string][],
  linkMetrics: Map<string, { itm_path_loss_db?: number | null }>,
  showHexClashes: boolean,
  maxHexClashHops: number,
  focusedNodeId: string | null,
  focusedPrefixNodeIds: Set<string> | null,
  staleCutoffMs = Date.now(),
): ClashComputation {
  const coverageByNodeId = buildCoverageByNodeId(coverage);
  const nodesWithPos = Array.from(nodes.values()).filter(
    (node) => hasCoords(node)
      && (node.role === undefined || node.role === 2)
      && (staleCutoffMs - new Date(node.last_seen).getTime()) < FOURTEEN_DAYS_MS,
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

export function buildHiddenMask(nodes: Map<string, MeshNode>): Map<string, HiddenMaskGeometry> {
  return buildHiddenCoordMask(nodes.values());
}
