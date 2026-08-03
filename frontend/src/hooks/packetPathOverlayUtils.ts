import type { AggregatedPacket, MeshNode } from './useNodes.js';
import { buildHiddenCoordMask, hasCoords, maskNodePoint, resolvePathWaypoints } from '../utils/pathing.js';

export type ResolveMode = 'resolved' | 'none';

export type CanonicalPathNode = {
  position: number;
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  ambiguous: boolean;
  confidence: number | null;
};

export type PathObserver = {
  observerId: string;
};

/** The single-observer DTO, with only resolved and none modes. */
export type ServerBetaResponse = {
  ok: boolean;
  packetHash: string;
  network?: string;
  mode: ResolveMode;
  canonicalPath: CanonicalPathNode[];
  observers: PathObserver[];
  confidence: number | null;
  purplePath?: [number, number][] | null;
  extraPurplePaths?: [number, number][][];
};

/** The canonical paths and observer projections for a packet. */
export type MultiObserverBetaResponse = {
  ok?: boolean;
  packetHash: string;
  network: string;
  observerCount?: number;
  sharedPrefixLength?: number;
  canonicalPath: CanonicalPathNode[];
  observers: PathObserver[];
  confidence: number | null;
  results?: ServerBetaResponse[];
};

export type ResolvedPathNode = {
  lat: number;
  lon: number;
  nodeId: string | null;
  name: string | null;
  confidence: number | null;
};

export type ResolvedPathRoute = {
  nodes: ResolvedPathNode[];
  confidence: number | null;
};

export type AggregatedPredictionState = {
  canonicalPath: CanonicalPathNode[];
  routes: ResolvedPathRoute[];
  observerIds: string[];
  confidence: number | null;
  ts: number;
};

/**
 * Return only contiguous coordinate-bearing portions of the canonical route.
 * An unresolved hop therefore cannot accidentally create a straight-line
 * bridge between two known hops.
 */
export function canonicalPathRuns(canonicalPath: readonly CanonicalPathNode[]): CanonicalPathNode[][] {
  const runs: CanonicalPathNode[][] = [];
  let current: CanonicalPathNode[] = [];
  const flush = () => {
    if (current.length >= 2) runs.push(current);
    current = [];
  };

  for (const node of canonicalPath) {
    if (node.lat == null || node.lon == null || !Number.isFinite(node.lat) || !Number.isFinite(node.lon)) {
      flush();
      continue;
    }
    current.push(node);
  }
  flush();
  return runs;
}

export function canonicalPathCoordinates(canonicalPath: readonly CanonicalPathNode[]): [number, number][][] {
  return canonicalPathRuns(canonicalPath).map((run) => (
    run.map((node) => [node.lat!, node.lon!] as [number, number])
  ));
}

function coordinateKey(lat: number, lon: number): string {
  return `${lat.toFixed(6)},${lon.toFixed(6)}`;
}

/**
 * Project every observer-specific DTO path, falling back to the top-level
 * canonical path for older responses. Exact coordinate matches retain the
 * decoder's node metadata and per-hop confidence.
 */
export function multiObserverPathRoutes(
  response: MultiObserverBetaResponse | null | undefined,
): ResolvedPathRoute[] {
  if (!response || response.ok === false) return [];

  const canonicalNodes = new Map<string, CanonicalPathNode>();
  for (const canonicalPath of [
    response.canonicalPath,
    ...(response.results ?? []).map((result) => result.canonicalPath),
  ]) {
    for (const node of canonicalPath ?? []) {
      if (node.lat == null || node.lon == null
          || !Number.isFinite(node.lat) || !Number.isFinite(node.lon)) continue;
      canonicalNodes.set(coordinateKey(node.lat, node.lon), node);
    }
  }

  const routes: ResolvedPathRoute[] = [];
  const seen = new Set<string>();
  for (const result of response.results ?? []) {
    if (!result?.ok) continue;
    for (const path of [result.purplePath, ...(result.extraPurplePaths ?? [])]) {
      if (!path || path.length < 2) continue;
      const signature = path.map(([lat, lon]) => coordinateKey(lat, lon)).join('>');
      if (seen.has(signature)) continue;
      seen.add(signature);
      routes.push({
        confidence: result.confidence,
        nodes: path.map(([lat, lon]) => {
          const canonical = canonicalNodes.get(coordinateKey(lat, lon));
          return {
            lat,
            lon,
            nodeId: canonical?.nodeId ?? null,
            name: canonical?.name ?? null,
            confidence: canonical?.confidence ?? null,
          };
        }),
      });
    }
  }

  if (routes.length > 0) return routes;
  return canonicalPathRuns(response.canonicalPath).map((run) => ({
    confidence: response.confidence,
    nodes: run.map((node) => ({
      lat: node.lat!,
      lon: node.lon!,
      nodeId: node.nodeId,
      name: node.name,
      confidence: node.confidence,
    })),
  }));
}

export function canonicalPathObserverIds(response: MultiObserverBetaResponse | null | undefined): string[] {
  if (!response) return [];
  return Array.from(new Set(
    (response.observers ?? [])
      .map((observer) => observer.observerId?.trim())
      .filter((observerId): observerId is string => Boolean(observerId)),
  ));
}

export function aggregateCanonicalPath(
  response: MultiObserverBetaResponse | null | undefined,
): Omit<AggregatedPredictionState, 'ts'> | null {
  if (!response || response.ok === false) return null;
  return {
    canonicalPath: Array.isArray(response.canonicalPath) ? response.canonicalPath : [],
    routes: multiObserverPathRoutes(response),
    observerIds: canonicalPathObserverIds(response),
    confidence: response.confidence ?? null,
  };
}

export function packetObserverIds(packet: AggregatedPacket | undefined): string[] {
  if (!packet) return [];
  return Array.from(new Set([
    ...(packet.observerIds ?? []),
    ...(packet.rxNodeId ? [packet.rxNodeId] : []),
  ]));
}

export function buildRegularPacketPaths(
  packet: AggregatedPacket | undefined,
  observerIds: string[],
  nodes: Map<string, MeshNode>,
): [number, number][][] {
  if (!packet || observerIds.length < 1 || (!packet.path?.length && !packet.srcNodeId)) return [];
  const hiddenCoordMask = buildHiddenCoordMask(nodes.values());
  const src = packet.srcNodeId ? (nodes.get(packet.srcNodeId) ?? null) : null;
  const srcWithPos = hasCoords(src) ? src : null;
  return observerIds.flatMap((observerId) => {
    const rx = nodes.get(observerId);
    if (!hasCoords(rx)) return [];
    const waypoints = packet.path?.length
      ? resolvePathWaypoints(packet.path, srcWithPos, rx, nodes, hiddenCoordMask)
      : (srcWithPos ? [maskNodePoint(srcWithPos, hiddenCoordMask), maskNodePoint(rx, hiddenCoordMask)] as [number, number][] : []);
    return waypoints.length >= 2 ? [waypoints] : [];
  });
}
