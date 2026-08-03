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
};

/** The canonical path projected across all observers for a packet. */
export type MultiObserverBetaResponse = {
  ok?: boolean;
  packetHash: string;
  network: string;
  canonicalPath: CanonicalPathNode[];
  observers: PathObserver[];
  confidence: number | null;
};

export type AggregatedPredictionState = {
  canonicalPath: CanonicalPathNode[];
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
