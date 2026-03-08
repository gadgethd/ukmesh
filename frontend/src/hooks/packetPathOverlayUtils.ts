import type { Filters } from '../components/FilterPanel/FilterPanel.js';
import type { AggregatedPacket, MeshNode } from './useNodes.js';
import { hasCoords, resolvePathWaypoints } from '../utils/pathing.js';

export type PathSegment = [[number, number], [number, number]];

export type ServerBetaResponse = {
  ok: boolean;
  packetHash: string;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  permutationCount: number;
  remainingHops: number | null;
  purplePath: [number, number][] | null;
  extraPurplePaths: [number, number][][];
  redPath: [number, number][] | null;
  redSegments: PathSegment[];
  completionPaths: [number, number][][];
};

export type MultiObserverBetaResponse = {
  ok: boolean;
  packetHash: string;
  observerCount: number;
  sharedPrefixLength: number;
  results: ServerBetaResponse[];
};

export type AggregatedPredictionState = {
  purplePaths: [number, number][][];
  redPaths: [number, number][][];
  redSegments: PathSegment[];
  completionPaths: [number, number][][];
  confidence: number | null;
  permutations: number | null;
  remainingHops: number | null;
  ts: number;
};

export function segmentizePath(path: [number, number][] | null): PathSegment[] {
  if (!path || path.length < 2) return [];
  const segments: PathSegment[] = [];
  for (let i = 0; i < path.length - 1; i++) {
    const a = path[i];
    const b = path[i + 1];
    if (!a || !b) continue;
    segments.push([a, b]);
  }
  return segments;
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
  const src = packet.srcNodeId ? (nodes.get(packet.srcNodeId) ?? null) : null;
  const srcWithPos = hasCoords(src) ? src : null;
  return observerIds.flatMap((observerId) => {
    const rx = nodes.get(observerId);
    if (!hasCoords(rx)) return [];
    const waypoints = packet.path?.length
      ? resolvePathWaypoints(packet.path, srcWithPos, rx, nodes)
      : (srcWithPos ? [[srcWithPos.lat, srcWithPos.lon], [rx.lat, rx.lon]] as [number, number][] : []);
    return waypoints.length >= 2 ? [waypoints] : [];
  });
}

export function shouldAllowCompletionPaths(packet: AggregatedPacket | undefined, filters: Filters): boolean {
  return Boolean(filters.betaPaths && packet?.packetType === 4);
}

export function aggregateServerPredictions(
  predictions: Array<ServerBetaResponse | null>,
  options?: {
    allowCompletionPaths?: boolean;
    collapseUnanchoredAdvertPartials?: boolean;
  },
): Omit<AggregatedPredictionState, 'ts'> | null {
  let validPredictions = predictions.filter((prediction): prediction is ServerBetaResponse => Boolean(prediction?.ok));
  if (validPredictions.length < 1) return null;

  if (options?.collapseUnanchoredAdvertPartials && validPredictions.length > 1) {
    const hasSourceAnchoredPartial = validPredictions.some((prediction) => (prediction.extraPurplePaths?.length ?? 0) > 0);
    if (!hasSourceAnchoredPartial) {
      validPredictions = [...validPredictions]
        .sort((a, b) => {
          const aResolved = a.mode === 'resolved' ? 1 : 0;
          const bResolved = b.mode === 'resolved' ? 1 : 0;
          if (aResolved !== bResolved) return bResolved - aResolved;

          const aConfidence = a.confidence ?? -1;
          const bConfidence = b.confidence ?? -1;
          if (aConfidence !== bConfidence) return bConfidence - aConfidence;

          const aPurpleEdges = Math.max(0, (a.purplePath?.length ?? 0) - 1);
          const bPurpleEdges = Math.max(0, (b.purplePath?.length ?? 0) - 1);
          if (aPurpleEdges !== bPurpleEdges) return bPurpleEdges - aPurpleEdges;

          const aRedEdges = Math.max(0, (a.redPath?.length ?? 0) - 1);
          const bRedEdges = Math.max(0, (b.redPath?.length ?? 0) - 1);
          if (aRedEdges !== bRedEdges) return bRedEdges - aRedEdges;

          const aRemaining = a.remainingHops ?? Number.POSITIVE_INFINITY;
          const bRemaining = b.remainingHops ?? Number.POSITIVE_INFINITY;
          return aRemaining - bRemaining;
        })
        .slice(0, 1);
    }
  }

  const purplePaths = validPredictions.flatMap((prediction) => {
    const paths: [number, number][][] = [];
    if (prediction.purplePath && prediction.purplePath.length >= 2) paths.push(prediction.purplePath);
    for (const path of prediction.extraPurplePaths ?? []) {
      if (path.length >= 2) paths.push(path);
    }
    return paths;
  });
  const allRedPaths = validPredictions.flatMap((prediction) => (
    prediction.redPath && prediction.redPath.length >= 2 ? [prediction.redPath] : []
  ));
  const allRedSegments = validPredictions.flatMap((prediction) => (
    prediction.redSegments?.length ? prediction.redSegments : segmentizePath(prediction.redPath && prediction.redPath.length >= 2 ? prediction.redPath : null)
  ));

  const purpleSegmentKeys = new Set<string>();
  for (const path of purplePaths) {
    for (let i = 0; i < path.length - 1; i++) {
      const a = path[i]!;
      const b = path[i + 1]!;
      purpleSegmentKeys.add(`${a[0]},${a[1]}|${b[0]},${b[1]}`);
      purpleSegmentKeys.add(`${b[0]},${b[1]}|${a[0]},${a[1]}`);
    }
  }
  const segKey = (a: [number, number], b: [number, number]) => `${a[0]},${a[1]}|${b[0]},${b[1]}`;
  const seenRedKeys = new Set<string>();
  const redSegments = allRedSegments.filter(([a, b]) => {
    if (purpleSegmentKeys.has(segKey(a, b)) || purpleSegmentKeys.has(segKey(b, a))) return false;
    const key = segKey(a, b);
    const revKey = segKey(b, a);
    if (seenRedKeys.has(key) || seenRedKeys.has(revKey)) return false;
    seenRedKeys.add(key);
    return true;
  });
  const redPaths = allRedPaths.filter((path) =>
    path.some((_, i) => {
      if (i >= path.length - 1) return false;
      const a = path[i]!;
      const b = path[i + 1]!;
      return !purpleSegmentKeys.has(segKey(a, b)) && !purpleSegmentKeys.has(segKey(b, a));
    }),
  );

  const completionPaths = options?.allowCompletionPaths === false
    ? []
    : validPredictions.flatMap((prediction) => prediction.completionPaths ?? []);
  const permutations = validPredictions.reduce((sum, prediction) => {
    const fallbackCount = (prediction.redPath ? 1 : 0) + (prediction.completionPaths?.length ?? 0);
    return sum + (Number.isFinite(prediction.permutationCount) ? prediction.permutationCount : fallbackCount);
  }, 0);
  const confidence = validPredictions.reduce<number | null>((best, prediction) => {
    if (prediction.confidence == null) return best;
    return best == null ? prediction.confidence : Math.max(best, prediction.confidence);
  }, null);
  const remainingHops = validPredictions.reduce<number | null>((best, prediction) => {
    if (prediction.remainingHops == null) return best;
    return best == null ? prediction.remainingHops : Math.max(best, prediction.remainingHops);
  }, null);

  return {
    purplePaths,
    redPaths,
    redSegments,
    completionPaths,
    confidence,
    permutations,
    remainingHops,
  };
}
