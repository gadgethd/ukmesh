import type { MeshNode } from '../hooks/useNodes.js';

export const HIDDEN_NODE_MASK_RADIUS_MILES = 0;
export const HIDDEN_NODE_MASK_RADIUS_METERS = 0;
const PROHIBITED_NODE_MARKER = '🚫';
export type HiddenMaskGeometry = {
  center: [number, number];
  point: [number, number];
};

export type LinkMetrics = {
  observed_count: number;
  multibyte_observed_count?: number;
  neighbor_report_count?: number;
  neighbor_best_snr_db?: number | null;
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

export function buildHiddenCoordMask(_nodes: Iterable<MeshNode>): Map<string, HiddenMaskGeometry> {
  // Coordinates now arrive server-finalized. Keeping this empty compatibility
  // object prevents the browser from becoming a confidentiality boundary.
  return new Map();
}

export function maskPoint(
  point: [number, number],
  hiddenCoordMask?: Map<string, HiddenMaskGeometry>,
): [number, number] {
  if (!hiddenCoordMask || hiddenCoordMask.size < 1) return point;
  return point;
}

export function maskCircleCenter(
  point: [number, number],
  hiddenCoordMask?: Map<string, HiddenMaskGeometry>,
): [number, number] {
  if (!hiddenCoordMask || hiddenCoordMask.size < 1) return point;
  return point;
}

export function maskNodePoint(
  node: MeshNode & { lat: number; lon: number },
  hiddenCoordMask?: Map<string, HiddenMaskGeometry>,
): [number, number] {
  void hiddenCoordMask;
  if (!isProhibitedMapNode(node)) return [node.lat, node.lon];
  // Fail closed if an old or malformed backend returns exact private geometry.
  return [0, 0];
}

export function resolvePathWaypoints(
  pathHashes: string[],
  src: (MeshNode & { lat: number; lon: number }) | null,
  rx: MeshNode & { lat: number; lon: number },
  allNodes: Map<string, MeshNode>,
  hiddenCoordMask?: Map<string, HiddenMaskGeometry>,
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
