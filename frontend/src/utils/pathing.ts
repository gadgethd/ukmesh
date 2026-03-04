import type { MeshNode } from '../hooks/useNodes.js';

export const MIN_LINK_OBSERVATIONS = 5; // must match backend db/index.ts

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

export function hasCoords(node: MeshNode | null | undefined): node is MeshNode & { lat: number; lon: number } {
  return typeof node?.lat === 'number' && typeof node?.lon === 'number';
}

export function resolvePathWaypoints(
  pathHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  allNodes: Map<string, MeshNode>,
): [number, number][] {
  const waypoints: [number, number][] = src ? [[src.lat!, src.lon!]] : [];
  const N = pathHashes.length;

  for (let i = 0; i < N; i++) {
    const prefix = pathHashes[i]!.toUpperCase();
    const candidates = Array.from(allNodes.values()).filter(
      (n) => hasCoords(n) && !n.name?.includes('🚫') && n.node_id.toUpperCase().startsWith(prefix),
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
    waypoints.push([best.lat!, best.lon!]);
  }

  waypoints.push([rx.lat!, rx.lon!]);
  return waypoints;
}
