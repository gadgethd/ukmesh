import type { MeshNode } from '../hooks/useNodes.js';

export const MIN_LINK_OBSERVATIONS = 5; // must match backend db/index.ts
export const HIDDEN_NODE_MASK_RADIUS_MILES = 1;
export const HIDDEN_NODE_MASK_RADIUS_METERS = HIDDEN_NODE_MASK_RADIUS_MILES * 1609.344;
const PROHIBITED_NODE_MARKER = '🚫';

export type LinkMetrics = {
  observed_count: number;
  itm_viable?: boolean | null;
  itm_path_loss_db?: number | null;
  count_a_to_b?: number;
  count_b_to_a?: number;
};

export function linkKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}

export function isValidMapCoord(lat: number | null | undefined, lon: number | null | undefined): boolean {
  if (typeof lat !== 'number' || typeof lon !== 'number') return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (Math.abs(lat) < 5 && Math.abs(lon) < 5) return false;
  return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

export function hasCoords(node: MeshNode | null | undefined): node is MeshNode & { lat: number; lon: number } {
  return isValidMapCoord(node?.lat, node?.lon);
}

export function isProhibitedMapNode(node: MeshNode | null | undefined): boolean {
  return Boolean(node?.name?.includes(PROHIBITED_NODE_MARKER));
}

function roundCoord(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function hiddenCoordKey(lat: number, lon: number): string {
  return `${roundCoord(lat)},${roundCoord(lon)}`;
}

function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededUnitPair(seed: string): [number, number] {
  const distanceUnit = hashSeed(`${seed}:distance`) / 0xffffffff;
  const bearingUnit = hashSeed(`${seed}:bearing`) / 0xffffffff;
  return [distanceUnit, bearingUnit];
}

function stablePointWithinMiles(
  lat: number,
  lon: number,
  seed: string,
  radiusMiles = HIDDEN_NODE_MASK_RADIUS_MILES,
): [number, number] {
  const radiusKm = radiusMiles * 1.609344;
  const [distanceUnit, bearingUnit] = seededUnitPair(seed);
  const distanceKm = Math.sqrt(distanceUnit) * radiusKm;
  const bearing = bearingUnit * Math.PI * 2;
  const latRad = lat * (Math.PI / 180);
  const dLat = (distanceKm / 111) * Math.cos(bearing);
  const lonScale = Math.max(0.01, Math.cos(latRad));
  const dLon = (distanceKm / (111 * lonScale)) * Math.sin(bearing);
  return [lat + dLat, lon + dLon];
}

export function buildHiddenCoordMask(nodes: Iterable<MeshNode>): Map<string, [number, number]> {
  const mask = new Map<string, [number, number]>();
  for (const node of nodes) {
    if (!hasCoords(node) || !isProhibitedMapNode(node)) continue;
    const activityKey = node.last_seen ?? 'unknown';
    const seed = `${node.node_id}|${activityKey}`;
    mask.set(hiddenCoordKey(node.lat, node.lon), stablePointWithinMiles(node.lat, node.lon, seed));
  }
  return mask;
}

export function maskPoint(
  point: [number, number],
  hiddenCoordMask?: Map<string, [number, number]>,
): [number, number] {
  if (!hiddenCoordMask || hiddenCoordMask.size < 1) return point;
  return hiddenCoordMask.get(hiddenCoordKey(point[0], point[1])) ?? point;
}

export function maskNodePoint(
  node: MeshNode & { lat: number; lon: number },
  hiddenCoordMask?: Map<string, [number, number]>,
): [number, number] {
  if (!isProhibitedMapNode(node)) return [node.lat, node.lon];
  return maskPoint([node.lat, node.lon], hiddenCoordMask);
}

export function resolvePathWaypoints(
  pathHashes: string[],
  src: (MeshNode & { lat: number; lon: number }) | null,
  rx: MeshNode & { lat: number; lon: number },
  allNodes: Map<string, MeshNode>,
  hiddenCoordMask?: Map<string, [number, number]>,
): [number, number][] {
  const waypoints: [number, number][] = src ? [maskNodePoint(src, hiddenCoordMask)] : [];
  const N = pathHashes.length;

  for (let i = 0; i < N; i++) {
    const prefix = pathHashes[i]!.toUpperCase();
    const candidates = Array.from(allNodes.values()).filter(
      (n): n is MeshNode & { lat: number; lon: number } => hasCoords(n) && n.node_id.toUpperCase().startsWith(prefix),
    );
    if (candidates.length === 0) continue;

    let best = candidates[0]!;
    if (candidates.length > 1) {
      if (src) {
        const t = (i + 1) / (N + 1);
        const expLat = src.lat! + t * (rx.lat! - src.lat!);
        const expLon = src.lon! + t * (rx.lon! - src.lon!);
        best = candidates.reduce((a, b) => {
          const da = Math.hypot(a.lat! - expLat, a.lon! - expLon);
          const db = Math.hypot(b.lat! - expLat, b.lon! - expLon);
          return da <= db ? a : b;
        });
      } else {
        const [anchorLat, anchorLon] = waypoints.length > 0
          ? waypoints[waypoints.length - 1]!
          : [rx.lat!, rx.lon!];
        best = candidates.reduce((a, b) => {
          const da = Math.hypot(a.lat! - anchorLat, a.lon! - anchorLon);
          const db = Math.hypot(b.lat! - anchorLat, b.lon! - anchorLon);
          return da <= db ? a : b;
        });
      }
    }
    waypoints.push(maskNodePoint(best, hiddenCoordMask));
  }

  waypoints.push(maskNodePoint(rx, hiddenCoordMask));
  return waypoints;
}
