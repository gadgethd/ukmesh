import { query, touchNodesPredictedOnline } from '../db/index.js';
import { expandResolverScope } from '../networks.js';
import {
  buildNodePathHashIndex,
  getNodesForPathHash,
  nodePathHash,
  normalizePathHash,
} from '../path-hash/utils.js';
import {
  BETA_PURPLE_THRESHOLD,
  CONTEXT_TTL_MS,
  MAX_BETA_HOPS,
  MAX_HOP_KM,
  MAX_PERMUTATION_HOP_KM,
  MAX_PERMUTATION_STATES,
  MAX_RENDER_PERMUTATIONS,
  MODEL_LIMIT,
  OBSERVER_HOP_WEIGHT_CONFIRMED,
  OBSERVER_HOP_WEIGHT_REACHABLE,
  PREFIX_AMBIGUITY_FLOOR_KM,
  WEAK_LINK_PATHLOSS_MAX_DB,
} from './constants.js';
import {
  canReach,
  clamp,
  distKm,
  hasCoords,
  hasLoS,
  linkKey,
  nodeRange,
  sourceProgressScore,
} from './geometry.js';
import {
  compareFallbackCandidates,
  fallbackEdgeAllowed,
  isImpossibleLink,
  isWeakOrBetter,
  retargetRedPathStart,
  segmentizePath,
  softFallbackCandidateAllowed,
  trimPathBetweenStitches,
  trimRedToPurpleStitch,
} from './fallback.js';
import { buildNeighborAffinityAdjacency, buildNeighborAffinityMap, neighborAffinityPreference } from './affinity.js';
import type { BetaResolveContext, LinkMetrics, MeshNode, MlPrefixScore, NodeCoverage, ObserverHopHint, PathLearningModel, PathPacket } from './types.js';

const contextCache = new Map<string, BetaResolveContext>();
const MAX_TRELLIS_CANDIDATES_PER_HOP = 24;
const TRUSTED_PATH_MAX_KM = 150;
const ML_PREFIX_SCORE_MIN = 0.80;
const ML_PREFIX_DOMINANT_SCORE = 0.85;
const DIRECT_ANCHOR_MAX_KM = 150;
const PATH_MULTI_HISTORY_WINDOW_HOURS = Math.min(
  24 * 30,
  Math.max(1, Number(process.env['PATH_MULTI_HISTORY_WINDOW_HOURS'] ?? 168) || 168),
);
const PATH_MULTI_MAX_SCAN_ROWS = Math.min(
  10_000,
  Math.max(32, Number(process.env['PATH_MULTI_MAX_SCAN_ROWS'] ?? 2_048) || 2_048),
);
const PATH_MULTI_MAX_OBSERVERS = Math.min(
  1_000,
  Math.max(1, Number(process.env['PATH_MULTI_MAX_OBSERVERS'] ?? 128) || 128),
);

type DirectObserverAnchor = {
  observerNode: MeshNode;
  observerId: string;
};

function currentHourBucket(bucketHours: number): number {
  const now = new Date();
  return Math.floor(now.getUTCHours() / bucketHours);
}

function edgeKey(receiverRegion: string, bucket: number, fromId: string, toId: string): string {
  return `${receiverRegion}|${bucket}|${fromId}|${toId}`;
}

function motifKey(receiverRegion: string, bucket: number, nodeIds: string[]): string {
  return `${receiverRegion}|${bucket}|${nodeIds.length}|${nodeIds.join('>')}`;
}

function addUndirectedNeighbor(adjacency: Map<string, Set<string>>, aId: string, bId: string): void {
  if (!adjacency.has(aId)) adjacency.set(aId, new Set());
  if (!adjacency.has(bId)) adjacency.set(bId, new Set());
  adjacency.get(aId)!.add(bId);
  adjacency.get(bId)!.add(aId);
}

function minimumDirectionalSupport(observedCount: number): number {
  if (observedCount >= 120) return 0.45;
  if (observedCount >= 70) return 0.42;
  if (observedCount >= 35) return 0.38;
  if (observedCount >= 20) return 0.34;
  if (observedCount >= 10) return 0.30;
  return 0.26;
}

function directionalSupport(meta: LinkMetrics, fromId: string, toId: string): number {
  const key = linkKey(fromId, toId);
  const [aId] = key.split(':');
  const ab = Number(meta.count_a_to_b ?? 0);
  const ba = Number(meta.count_b_to_a ?? 0);
  const total = ab + ba;
  if (total <= 0) return 0;
  const fromTo = fromId === aId ? ab : ba;
  return fromTo / total;
}

function observerHopPrior(candidate: MeshNode, prevNode: MeshNode, hints: ObserverHopHint[], typicalHopKm = 25): number {
  if (hints.length < 1) return 0;
  let weighted = 0;
  let totalWeight = 0;
  for (const hint of hints) {
    const observer = hint.observerNode;
    if (!hasCoords(observer) || hint.hopDelta === 0) continue;
    const prevDist = distKm(prevNode, observer);
    const candidateDist = distKm(candidate, observer);
    // Normalise by typical hop spacing so the hint stays meaningful across
    // both dense (5 km) and wide-area (40 km) networks instead of saturating at ±1.
    const towardObserver = clamp((prevDist - candidateDist) / typicalHopKm, -1, 1);
    const weight = clamp(Math.abs(hint.hopDelta) / 4, 0.2, 1);
    const contribution = hint.hopDelta < 0
      ? towardObserver
      : -towardObserver * 0.55;
    weighted += contribution * weight;
    totalWeight += weight;
  }
  if (totalWeight <= 0) return 0;
  return clamp(weighted / totalWeight, -1, 1);
}

function linkColorPreference(meta: LinkMetrics | undefined): number {
  if (!meta) return -0.08;
  const pathLoss = meta.itm_path_loss_db;
  if (pathLoss == null) return -0.04;
  if (pathLoss <= 121.5) return 0.28; // green
  if (pathLoss <= 129.5) return 0.14; // yellow/amber
  return 0.02; // red
}

function radioNeighborPreference(meta: LinkMetrics | undefined): number {
  if (!meta) return 0;
  const reports = Number(meta.neighbor_report_count ?? 0);
  const snr = meta.neighbor_best_snr_db;
  if (reports <= 0 || snr == null || !Number.isFinite(snr)) return 0;
  const snrScore = snr >= 8 ? 1.0
    : snr >= 2 ? 0.62
      : snr >= 0 ? 0.40
        : snr >= -4 ? 0.20
          : 0.08;
  const reportScore = Math.min(1, Math.log2(1 + reports) / 4);
  return 0.10 + snrScore * 0.18 + reportScore * 0.08;
}

function observedEvidenceCount(meta: LinkMetrics | undefined): number {
  if (!meta) return 0;
  return Math.max(
    Number(meta.observed_count ?? 0),
    Number(meta.multibyte_observed_count ?? 0),
  );
}

function multibytePathPreference(meta: LinkMetrics | undefined): number {
  if (!meta || meta.itm_viable !== true) return 0;
  const multibyteObserved = Number(meta.multibyte_observed_count ?? 0);
  if (multibyteObserved <= 0) return 0;
  return Math.min(0.20, 0.06 + Math.log10(1 + multibyteObserved) * 0.045);
}

function confirmedLinkConfidence(
  meta: LinkMetrics | undefined,
  fromId: string,
  toId: string,
  prior?: { prefix?: number; transition?: number; motif?: number; edge?: number; ambiguity?: number; affinity?: number },
): number {
  if (!meta) return 0;

  const observed = observedEvidenceCount(meta);
  const pathLoss = meta.itm_path_loss_db;
  let base: number;
  if (pathLoss == null) {
    base = observed >= 60 ? 0.68 : observed >= 30 ? 0.56 : 0.34;
  } else if (pathLoss <= 121.5) {
    base = 0.95;
  } else if (pathLoss <= 129.5) {
    base = 0.9;
  } else if (pathLoss <= 137.5) {
    base = 0.84;
  } else if (pathLoss <= 140) {
    base = 0.7;
  } else {
    base = 0.56;
  }

  const direction = directionalSupport(meta, fromId, toId);
  const minDir = minimumDirectionalSupport(observed);
  const dirScale = direction >= minDir
    ? 1 + (direction - minDir) * 0.38
    : Math.max(0.45, 1 - (minDir - direction) * 1.8);

  const confidence = base * Math.min(1.25, Math.max(0.45, dirScale));
  const priorBoost = Number(prior?.prefix ?? 0)
    + Number(prior?.transition ?? 0)
    + Number(prior?.motif ?? 0)
    + Number(prior?.edge ?? 0)
    + Number(prior?.ambiguity ?? 0)
    + Number(prior?.affinity ?? 0)
    + linkColorPreference(meta)
    + radioNeighborPreference(meta)
    + multibytePathPreference(meta);
  return clamp(confidence + priorBoost, 0, 1);
}

function buildClashAdjacency(
  candidates: MeshNode[],
  linkPairs: Set<string>,
  linkMetrics: Map<string, LinkMetrics>,
): Map<string, Set<string>> {
  const byId = new Map<string, MeshNode>();
  for (const node of candidates) byId.set(node.node_id, node);

  const adjacency = new Map<string, Set<string>>();
  for (const key of linkPairs) {
    const [aId, bId] = key.split(':');
    if (!aId || !bId) continue;
    if (!byId.has(aId) || !byId.has(bId)) continue;
    const meta = linkMetrics.get(key);
    const pathLoss = meta?.itm_path_loss_db;
    if (pathLoss == null || pathLoss > WEAK_LINK_PATHLOSS_MAX_DB) continue;
    if (!adjacency.has(aId)) adjacency.set(aId, new Set());
    if (!adjacency.has(bId)) adjacency.set(bId, new Set());
    adjacency.get(aId)!.add(bId);
    adjacency.get(bId)!.add(aId);
  }
  return adjacency;
}

function strongConfirmedFloor(meta: LinkMetrics | undefined): number {
  if (!meta) return 0;
  const observed = observedEvidenceCount(meta);
  const pathLoss = meta.itm_path_loss_db;
  if (pathLoss == null) return 0;
  const radioBoost = radioNeighborPreference(meta) + multibytePathPreference(meta) * 0.5;
  if (pathLoss <= 121.5) {
    if (observed >= 120) return Math.min(0.99, 0.95 + radioBoost * 0.25);
    if (observed >= 70) return Math.min(0.98, 0.90 + radioBoost * 0.22);
    if (observed >= 35) return Math.min(0.96, 0.84 + radioBoost * 0.18);
  }
  if (pathLoss <= 129.5) {
    if (observed >= 120) return Math.min(0.95, 0.86 + radioBoost * 0.22);
    if (observed >= 70) return Math.min(0.90, 0.80 + radioBoost * 0.20);
    if (observed >= 35) return Math.min(0.84, 0.72 + radioBoost * 0.18);
  }
  if (radioBoost > 0.18 && pathLoss <= 137.5) return Math.min(0.78, 0.52 + radioBoost * 0.4);
  if (observed >= 120) return 0.68;
  if (observed >= 70) return 0.62;
  if (observed >= 35) return 0.56;
  return 0;
}

function attachSrcToPath(
  lowPath: [number, number][] | null,
  purplePath: [number, number][] | null,
  src: MeshNode | null,
  forceIncludeSource: boolean,
): [number, number][] | null {
  if (!forceIncludeSource || !src || typeof src.lat !== 'number' || typeof src.lon !== 'number') return lowPath;
  const srcPt: [number, number] = [src.lat, src.lon];
  const anchor = lowPath?.[0] ?? purplePath?.[0];
  if (!anchor) return lowPath;
  if (Math.abs(anchor[0] - srcPt[0]) <= 0.0001 && Math.abs(anchor[1] - srcPt[1]) <= 0.0001) return lowPath;
  return lowPath ? ([srcPt, ...lowPath] as [number, number][]) : [srcPt, anchor];
}

function edgeMetricConfidence(fromId: string, toId: string, linkMetrics: Map<string, LinkMetrics>): number {
  const meta = linkMetrics.get(linkKey(fromId, toId));
  if (!meta) return 0;
  const observed = observedEvidenceCount(meta);
  const pathLoss = meta.itm_path_loss_db;
  let base: number;
  if (pathLoss == null) base = observed >= 60 ? 0.72 : observed >= 30 ? 0.62 : 0.45;
  else if (observed >= 120 && pathLoss <= 121.5) base = 0.86;
  else if (observed >= 70 && pathLoss <= 129.5) base = 0.80;
  else if (observed >= 35 && pathLoss <= 137.5) base = 0.74;
  else if (pathLoss <= 137.5) base = Math.min(0.72, 0.48 + Math.log10(1 + observed) * 0.10);
  else base = Math.min(0.58, 0.40 + Math.log10(1 + observed) * 0.08);

  const radioBoost = radioNeighborPreference(meta) + multibytePathPreference(meta);
  if (observed >= 120) return Math.max(base + linkColorPreference(meta) + radioBoost, 0.82);
  if (observed >= 70) return Math.max(base + linkColorPreference(meta) + radioBoost, 0.74);
  if (observed >= 35) return Math.max(base + linkColorPreference(meta) + radioBoost, 0.62);
  if (observed >= 20) return Math.max(base + linkColorPreference(meta) + radioBoost, 0.56);
  return clamp(base + linkColorPreference(meta) + radioBoost, 0, 1);
}

function purpleEdgeAllowed(
  fromId: string,
  toId: string,
  nodesById: Map<string, MeshNode>,
  coverageByNode: Map<string, number>,
  linkPairs: Set<string>,
  linkMetrics: Map<string, LinkMetrics>,
): boolean {
  const from = nodesById.get(fromId);
  const to = nodesById.get(toId);
  if (!hasCoords(from) || !hasCoords(to)) return false;

  const distance = distKm(from, to);
  if (distance > MAX_HOP_KM) return false;

  const key = linkKey(fromId, toId);
  if (!linkPairs.has(key)) return false;

  const meta = linkMetrics.get(key);
  if (isImpossibleLink(meta)) return false;

  // A strongly-confirmed observed link is accepted as a lenient substitute for
  // strict geometric LoS, because it is direct real-world evidence.
  if (strongConfirmedFloor(meta) >= 0.70) return true;

  if (!hasLoS(from, to)) return false;

  return canReach(from, to, coverageByNode) || isWeakOrBetter(meta);
}

function splitResolvedAndAlternatives(
  result: { path: [number, number][]; segmentConfidence: number[]; nodeIds: string[] },
  threshold: number,
  nodesById: Map<string, MeshNode>,
  coverageByNode: Map<string, number>,
  linkPairs: Set<string>,
  linkMetrics: Map<string, LinkMetrics>,
): { purplePath: [number, number][] | null; redPath: [number, number][] | null; remainingHops: number } {
  const seg = result.segmentConfidence.map((v, i) => {
    const fromId = result.nodeIds[i];
    const toId = result.nodeIds[i + 1];
    if (!fromId || !toId) return v;
    if (!purpleEdgeAllowed(fromId, toId, nodesById, coverageByNode, linkPairs, linkMetrics)) return 0;
    // Only boost with edgeMetricConfidence for links with real observed traffic.
    // Unobserved ITM-viable links keep the Viterbi confidence as-is (capped below purple).
    const observed = observedEvidenceCount(linkMetrics.get(linkKey(fromId, toId)));
    return observed > 0 ? Math.max(v, edgeMetricConfidence(fromId, toId, linkMetrics)) : v;
  });

  let splitIdx = -1;
  for (let i = seg.length - 1; i >= 0; i--) {
    if (seg[i]! < threshold) {
      splitIdx = i;
      break;
    }
  }

  if (splitIdx < 0) {
  return { purplePath: result.path, redPath: null, remainingHops: 0 };
  }

  const purpleSlice = result.path.slice(splitIdx + 1);
  const redSlice = result.path.slice(0, splitIdx + 2);
  const purplePath = purpleSlice.length >= 2 ? purpleSlice : null;
  const redPath = redSlice.length >= 2 ? redSlice : null;
  return { purplePath, redPath, remainingHops: splitIdx + 1 };
}

function splitResolvedFromSource(
  result: { path: [number, number][]; segmentConfidence: number[]; nodeIds: string[] },
  threshold: number,
  nodesById: Map<string, MeshNode>,
  coverageByNode: Map<string, number>,
  linkPairs: Set<string>,
  linkMetrics: Map<string, LinkMetrics>,
): { purplePath: [number, number][] | null; remainingHops: number } {
  const seg = result.segmentConfidence.map((v, i) => {
    const fromId = result.nodeIds[i];
    const toId = result.nodeIds[i + 1];
    if (!fromId || !toId) return v;
    if (!purpleEdgeAllowed(fromId, toId, nodesById, coverageByNode, linkPairs, linkMetrics)) return 0;
    const observed = observedEvidenceCount(linkMetrics.get(linkKey(fromId, toId)));
    return observed > 0 ? Math.max(v, edgeMetricConfidence(fromId, toId, linkMetrics)) : v;
  });
  let keepEdges = 0;
  for (let i = 0; i < seg.length; i++) {
    if (seg[i]! < threshold) break;
    keepEdges += 1;
  }
  if (keepEdges < 1) return { purplePath: null, remainingHops: result.path.length - 1 };
  const purplePath = result.path.slice(0, keepEdges + 1);
  return {
    purplePath: purplePath.length >= 2 ? purplePath : null,
    remainingHops: Math.max(0, seg.length - keepEdges),
  };
}

function buildFallbackPrefixPath(
  hopHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  nodesById: Map<string, MeshNode>,
  coverageByNode: Map<string, number>,
  linkMetrics: Map<string, LinkMetrics>,
  forceIncludeSource = false,
  observerHopHints: ObserverHopHint[] = [],
  cachedPathHashIndex?: Map<string, MeshNode[]>,
): { path: [number, number][]; nodeIds: string[] } | null {
  const pathHashIndex = cachedPathHashIndex ?? buildNodePathHashIndex(
    Array.from(nodesById.values()).filter(
      (n) => hasCoords(n) && (n.role === null || n.role === 2),
    ),
  );

  const pickedNearRx: MeshNode[] = [];
  const visited = new Set<string>([rx.node_id]);
  let prev = rx;
  let nextTowardRx: MeshNode | null = null;
  for (const h of [...hopHashes].reverse()) {
    const prefix = normalizePathHash(h);
    const observerOwnPrefix = rx.role === 2 ? nodePathHash(rx.node_id, prefix) : null;
    const compareCandidates = (a: MeshNode, b: MeshNode): number => {
      const hopBias = observerHopPrior(b, prev, observerHopHints) - observerHopPrior(a, prev, observerHopHints);
      const base = compareFallbackCandidates(a, b, prev, src, nextTowardRx, coverageByNode, linkMetrics);
      return base + hopBias * 0.6;
    };
    let candidates = getNodesForPathHash(pathHashIndex, prefix)
      .filter((n) => {
        if (visited.has(n.node_id)) return false;
        if (!fallbackEdgeAllowed(n, prev, coverageByNode, linkMetrics)) return false;
        if (prev.node_id === rx.node_id && observerOwnPrefix === prefix && n.node_id !== rx.node_id) {
          const meta = linkMetrics.get(linkKey(n.node_id, rx.node_id));
          const observed = observedEvidenceCount(meta);
          const pathLoss = meta?.itm_path_loss_db;
          return observed >= 120 && pathLoss != null && pathLoss <= 125;
        }
        return true;
      })
      .sort(compareCandidates);
    if (candidates.length < 1) {
      candidates = getNodesForPathHash(pathHashIndex, prefix)
        .filter((n) => {
          if (visited.has(n.node_id)) return false;
          if (!softFallbackCandidateAllowed(n, prev, src, nextTowardRx)) return false;
          if (prev.node_id === rx.node_id && observerOwnPrefix === prefix && n.node_id !== rx.node_id) {
            const meta = linkMetrics.get(linkKey(n.node_id, rx.node_id));
            const observed = observedEvidenceCount(meta);
            const pathLoss = meta?.itm_path_loss_db;
            return observed >= 120 && pathLoss != null && pathLoss <= 125;
          }
          return true;
        })
        .sort(compareCandidates);
    }
    const chosen = candidates[0];
    if (!chosen) continue;
    pickedNearRx.push(chosen);
    visited.add(chosen.node_id);
    nextTowardRx = prev;
    prev = chosen;
  }

  const hopsFarToNear = [...pickedNearRx].reverse();
  const pathNodes: MeshNode[] = [...(hasCoords(src) && forceIncludeSource ? [src] : []), ...hopsFarToNear, rx];
  if (!forceIncludeSource && hasCoords(src) && pathNodes.length >= 2 && pathNodes[0]?.node_id === src.node_id) {
    pathNodes.shift();
  }
  for (let i = 0; i < pathNodes.length - 1; i++) {
    const from = pathNodes[i];
    const to = pathNodes[i + 1];
    if (!from || !to) return null;
    if (!fallbackEdgeAllowed(from, to, coverageByNode, linkMetrics)) return null;
  }
  if (pathNodes.length < 2) return null;
  return { path: pathNodes.map((n) => [n.lat!, n.lon!]), nodeIds: pathNodes.map((n) => n.node_id) };
}

function enumeratePrefixContinuations(
  startNodeId: string,
  remainingPrefixes: string[],
  endNodeId: string,
  nodesById: Map<string, MeshNode>,
  coverageByNode: Map<string, number>,
  linkMetrics: Map<string, LinkMetrics>,
  options?: {
    dropStartIfNodeId?: string;
    maxRenderPaths?: number;
    maxSearchStates?: number;
    blockedNodeIds?: string[];
    pathHashIndex?: Map<string, MeshNode[]>;
  },
): { paths: [number, number][][]; totalCount: number; truncated: boolean; longestPrefixDepth: number } {
  const maxRenderPaths = Math.max(1, options?.maxRenderPaths ?? MAX_RENDER_PERMUTATIONS);
  const maxSearchStates = Math.max(1000, options?.maxSearchStates ?? MAX_PERMUTATION_STATES);
  const blocked = new Set(options?.blockedNodeIds ?? []);
  const pathHashIndex = blocked.size === 0 && options?.pathHashIndex
    ? options.pathHashIndex
    : buildNodePathHashIndex(Array.from(nodesById.values()).filter(
      (n) => hasCoords(n) && (n.role === null || n.role === 2),
    ));

  const start = nodesById.get(startNodeId);
  const end = nodesById.get(endNodeId);
  if (!start || !end || !hasCoords(start) || !hasCoords(end)) {
    return { paths: [], totalCount: 0, truncated: false, longestPrefixDepth: 0 };
  }
  if (blocked.has(start.node_id) || blocked.has(end.node_id)) {
    return { paths: [], totalCount: 0, truncated: false, longestPrefixDepth: 0 };
  }

  const discovered: string[][] = [];
  const partialDiscovered: string[][] = [];
  let totalCount = 0;
  let partialCount = 0;
  let states = 0;
  let truncated = false;
  let longestPrefixDepth = 0;
  let bestPartialDepth = 0;

  const recordPartial = (depth: number, path: string[]) => {
    if (path.length < 2 || depth <= 0) return;
    if (depth > bestPartialDepth) {
      bestPartialDepth = depth;
      partialDiscovered.length = 0;
      partialCount = 0;
    }
    if (depth === bestPartialDepth) {
      partialCount += 1;
      if (partialDiscovered.length < maxRenderPaths) partialDiscovered.push([...path]);
    }
  };

  const dfs = (idx: number, current: MeshNode, path: string[], visited: Set<string>) => {
    if (idx > longestPrefixDepth) longestPrefixDepth = idx;
    if (states++ >= maxSearchStates) {
      truncated = true;
      return;
    }

    if (idx >= remainingPrefixes.length) {
      if (current.node_id !== end.node_id) {
        if (visited.has(end.node_id)) {
          recordPartial(idx, path);
          return;
        }
        if (!fallbackEdgeAllowed(current, end, coverageByNode, linkMetrics)) {
          recordPartial(idx, path);
          return;
        }
        path.push(end.node_id);
      }
      totalCount += 1;
      if (discovered.length < maxRenderPaths) discovered.push([...path]);
      if (current.node_id !== end.node_id) path.pop();
      return;
    }

    const prefix = normalizePathHash(remainingPrefixes[idx]);
    const nodesForPrefix = getNodesForPathHash(pathHashIndex, prefix)
      .filter((n) => (
        !visited.has(n.node_id)
        && !blocked.has(n.node_id)
        && n.node_id !== end.node_id
        && distKm(n, current) <= MAX_PERMUTATION_HOP_KM
        && fallbackEdgeAllowed(n, current, coverageByNode, linkMetrics)
      ))
      .sort((a, b) => compareFallbackCandidates(a, b, current, end, null, coverageByNode, linkMetrics));
    if (nodesForPrefix.length < 1) {
      recordPartial(idx, path);
      return;
    }
    for (const next of nodesForPrefix) {
      visited.add(next.node_id);
      path.push(next.node_id);
      dfs(idx + 1, next, path, visited);
      path.pop();
      visited.delete(next.node_id);
      if (states >= maxSearchStates) {
        truncated = true;
        return;
      }
    }
  };

  const visited = new Set<string>([start.node_id]);
  dfs(0, start, [start.node_id], visited);

  const renderSource = totalCount > 0 ? discovered : partialDiscovered;
  const paths = renderSource
    .map((ids) => {
      const renderIds = (options?.dropStartIfNodeId && ids[0] === options.dropStartIfNodeId) ? ids.slice(1) : ids;
      const nodes = renderIds.map((id) => nodesById.get(id)).filter((n): n is MeshNode => Boolean(n && hasCoords(n)));
      if (new Set(nodes.map((n) => n.node_id)).size !== nodes.length) return null;
      if (nodes.length < 2) return null;
      return nodes.map((n) => [n.lat!, n.lon!]) as [number, number][];
    })
    .filter((p): p is [number, number][] => Array.isArray(p));

  return {
    paths,
    totalCount: totalCount > 0 ? totalCount : partialCount,
    truncated,
    longestPrefixDepth,
  };
}

function reverseResolvedPath(
  result: { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null,
): { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null {
  if (!result) return null;
  return {
    path: [...result.path].reverse(),
    confidence: result.confidence,
    segmentConfidence: [...result.segmentConfidence].reverse(),
    nodeIds: [...result.nodeIds].reverse(),
  };
}

/**
 * Builds a hop-index → MeshNode anchor map by checking whether any candidate MQTT node's
 * node_id prefix exactly matches a hash in the hops array. A definitive hash match means
 * that node was unambiguously at that hop position, giving us a hard location anchor.
 * Only anchors hops where exactly one candidate matches (avoids ambiguous collisions).
 */
function buildHashMatchedAnchors(
  hops: string[],
  candidates: MeshNode[],
  excludeNodeIds: Set<string>,
): Map<number, MeshNode> {
  const anchors = new Map<number, MeshNode>();
  for (let i = 0; i < hops.length; i++) {
    const hash = normalizePathHash(hops[i]);
    if (!hash) continue;
    const matches = candidates.filter(
      (n) => !excludeNodeIds.has(n.node_id) && hasCoords(n) && nodePathHash(n.node_id, hash) === hash,
    );
    if (matches.length === 1) anchors.set(i, matches[0]!);
  }
  return anchors;
}

function directAnchorKey(position: number, hash: string): string {
  return `${position}:${normalizePathHash(hash)}`;
}

function buildDirectObserverAnchorIndex(
  observations: Array<{ observerId: string; rx: MeshNode | null; hops: string[]; hopCount: number | null | undefined }>,
): Map<string, DirectObserverAnchor[]> {
  const index = new Map<string, DirectObserverAnchor[]>();
  for (const obs of observations) {
    if (!hasCoords(obs.rx)) continue;
    const hopCount = Number(obs.hopCount ?? 0);
    if (!Number.isFinite(hopCount) || hopCount < 1) continue;
    const position = hopCount - 1;
    if (position < 0 || position >= obs.hops.length) continue;
    const hash = normalizePathHash(obs.hops[position]);
    if (!hash) continue;
    const key = directAnchorKey(position, hash);
    const anchors = index.get(key) ?? [];
    if (!anchors.some((a) => a.observerId === obs.observerId)) {
      anchors.push({ observerId: obs.observerId, observerNode: obs.rx });
    }
    index.set(key, anchors);
  }
  return index;
}

function directAnchorsForHops(
  hops: string[],
  index: Map<string, DirectObserverAnchor[]>,
): Map<number, DirectObserverAnchor[]> {
  const anchors = new Map<number, DirectObserverAnchor[]>();
  for (let i = 0; i < hops.length; i++) {
    const hash = normalizePathHash(hops[i]);
    if (!hash) continue;
    const directAnchors = index.get(directAnchorKey(i, hash));
    if (directAnchors && directAnchors.length > 0) anchors.set(i, directAnchors);
  }
  return anchors;
}

function buildResolvableMultibyteAnchors(
  hops: string[],
  candidates: MeshNode[],
  excludeNodeIds: Set<string>,
): Map<number, MeshNode> {
  const anchors = new Map<number, MeshNode>();
  if (hops.length === 0 || candidates.length === 0) return anchors;

  const normalizedLengths = Array.from(new Set(
    hops
      .map((hash) => normalizePathHash(hash))
      .filter((hash): hash is string => Boolean(hash) && hash.length >= 4)
      .map((hash) => hash.length),
  ));
  if (normalizedLengths.length === 0) return anchors;

  const eligibleCandidates = candidates.filter(
    (n) => !excludeNodeIds.has(n.node_id) && hasCoords(n) && (n.role === null || n.role === 2),
  );
  if (eligibleCandidates.length === 0) return anchors;

  const pathHashIndex = buildNodePathHashIndex(eligibleCandidates, normalizedLengths);
  for (let i = 0; i < hops.length; i++) {
    const hash = normalizePathHash(hops[i]);
    if (!hash || hash.length < 4) continue;
    const matches = getNodesForPathHash(pathHashIndex, hash);
    if (matches.length === 1) anchors.set(i, matches[0]!);
  }
  return anchors;
}

function trimObserverTerminalHop(hops: string[], rx: MeshNode | null | undefined): string[] {
  if (!rx || rx.role !== 2 || hops.length <= 1) return hops;
  const terminal = normalizePathHash(hops[hops.length - 1]);
  if (!terminal) return hops;
  return nodePathHash(rx.node_id, terminal) === terminal
    ? hops.slice(0, -1)
    : hops;
}

function matchesObserverPathHash(rx: MeshNode | null | undefined, hash: string | null | undefined): boolean {
  if (!rx || !hash) return false;
  const normalized = normalizePathHash(hash);
  if (!normalized) return false;
  return nodePathHash(rx.node_id, normalized) === normalized;
}

function isObserverSelfEchoLoop(rawHops: string[], rx: MeshNode | null | undefined): boolean {
  if (!rx || rx.role !== 2 || rawHops.length < 3) return false;
  return matchesObserverPathHash(rx, rawHops[0]) && matchesObserverPathHash(rx, rawHops[rawHops.length - 1]);
}

type PreparedPacketObservation = {
  packet: PathPacket;
  rx: MeshNode | null;
  hashes: string[];
  rawHops: string[];
  hops: string[];
  ignoreForPathing: boolean;
};

function preparePacketObservation(packet: PathPacket, rx: MeshNode | null): PreparedPacketObservation {
  const hashes = packet.path_hashes ?? [];
  const expectedHexLen = packet.path_hash_size_bytes != null ? packet.path_hash_size_bytes * 2 : null;
  const validatedHashes = expectedHexLen != null
    ? hashes.filter((h) => h.length === expectedHexLen)
    : hashes;
  const rawHops = packet.hop_count != null
    ? validatedHashes.slice(0, Math.max(0, packet.hop_count))
    : validatedHashes;
  const ignoreForPathing = isObserverSelfEchoLoop(rawHops, rx);
  const hops = trimObserverTerminalHop(rawHops, rx);
  return { packet, rx, hashes, rawHops, hops, ignoreForPathing };
}

function compareCanonicalObserverObservation(a: PreparedPacketObservation, b: PreparedPacketObservation): number {
  return b.hops.length - a.hops.length
    || b.rawHops.length - a.rawHops.length
    || Number(Boolean(b.packet.path_hash_size_bytes)) - Number(Boolean(a.packet.path_hash_size_bytes))
    || Number(Boolean(b.packet.src_node_id)) - Number(Boolean(a.packet.src_node_id))
    || Number(b.packet.hop_count ?? 0) - Number(a.packet.hop_count ?? 0);
}

function comparePreferredResolvedObservation(a: PreparedPacketObservation, b: PreparedPacketObservation): number {
  return b.hops.length - a.hops.length
    || Number(Boolean(b.packet.path_hash_size_bytes)) - Number(Boolean(a.packet.path_hash_size_bytes))
    || Number(Boolean(b.packet.src_node_id)) - Number(Boolean(a.packet.src_node_id))
    || a.rawHops.length - b.rawHops.length
    || Number(a.packet.hop_count ?? Number.MAX_SAFE_INTEGER) - Number(b.packet.hop_count ?? Number.MAX_SAFE_INTEGER);
}

function resolveExactMultibyteChain(
  pathHashes: string[],
  context: BetaResolveContext,
): { path: [number, number][]; nodeIds: string[] } | null {
  if (pathHashes.length < 2) return null;
  const pathHashIndex = buildNodePathHashIndex(context.repeaterNodes, [pathHashes[0]?.length ?? 0]);
  const nodes: MeshNode[] = [];
  const visited = new Set<string>();

  for (const rawHash of pathHashes) {
    const pathHash = normalizePathHash(rawHash);
    if (!pathHash) return null;
    const matches = getNodesForPathHash(pathHashIndex, pathHash)
      .filter((n) => hasCoords(n) && (n.role === null || n.role === 2));
    if (matches.length !== 1) return null;
    const node = matches[0]!;
    if (visited.has(node.node_id)) return null;
    visited.add(node.node_id);
    nodes.push(node);
  }

  if (nodes.length < 2) return null;
  return {
    path: nodes.map((n) => [n.lat!, n.lon!]),
    nodeIds: nodes.map((n) => n.node_id),
  };
}

function resolveBetaPath(
  pathHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  context: BetaResolveContext,
  options?: {
    forceIncludeSource?: boolean;
    disableSourcePrepend?: boolean;
    blockedNodeIds?: string[];
    observerHopHints?: ObserverHopHint[];
    anchorNodes?: Map<number, MeshNode>;
    directObserverAnchors?: Map<number, DirectObserverAnchor[]>;
    extraCorridorTargets?: MeshNode[];
    /** Age of sticky anchors as fraction of their TTL (0=fresh, 1=expired). Used to decay confidence. */
    stickyAgeFraction?: number;
  },
): { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null {
  const normalizedHashes = pathHashes.map(normalizePathHash).filter(Boolean);
  if (!hasCoords(rx) || normalizedHashes.length === 0) return null;
  if (normalizedHashes.length >= MAX_BETA_HOPS) return null;

  type HopResult = { node: MeshNode; conf: number };
  const blockedNodeIds = new Set(options?.blockedNodeIds ?? []);
  const hasBlockedNodes = blockedNodeIds.size > 0;
  const candidatesPool = hasBlockedNodes
    ? context.repeaterNodes.filter((node) => !blockedNodeIds.has(node.node_id))
    : context.repeaterNodes;
  const pathHashIndex = hasBlockedNodes
    ? buildNodePathHashIndex(candidatesPool)
    : context.repeaterPathHashIndex;

  const totalDist = hasCoords(src) ? distKm(src, rx) : 0;
  // Typical per-hop spacing used to normalise observer hint directionality.
  // Clamped 5–40 km so hints remain meaningful in both dense and sparse networks.
  const typicalHopKm = normalizedHashes.length > 0
    ? Math.max(5, Math.min(40, totalDist / normalizedHashes.length))
    : 25;
  const receiverRegion = rx.iata ?? 'unknown';
  const bucketHours = context.learningModel.bucketHours ?? 6;
  const hourBucket = currentHourBucket(bucketHours);
  const activeObserverHopHints = options?.observerHopHints ?? [];
  const anchorNodes = options?.anchorNodes;
  const directObserverAnchors = options?.directObserverAnchors;

  function prefixPrior(prefix: string, prevPrefix: string, nodeId: string): number {
    const exactKey = `${receiverRegion}|${prefix}|${prevPrefix}|${nodeId}`;
    const regionOnlyKey = `unknown|${prefix}|${prevPrefix}|${nodeId}`;
    const noPrevKey = `${receiverRegion}|${prefix}||${nodeId}`;
    const noPrevFallbackKey = `unknown|${prefix}||${nodeId}`;
    return context.learningModel.prefixProbabilities.get(exactKey)
      ?? context.learningModel.prefixProbabilities.get(regionOnlyKey)
      ?? context.learningModel.prefixProbabilities.get(noPrevKey)
      ?? context.learningModel.prefixProbabilities.get(noPrevFallbackKey)
      ?? 0;
  }

  function transitionPrior(fromId: string, toId: string): number {
    const key = `${receiverRegion}|${fromId}|${toId}`;
    const fallback = `unknown|${fromId}|${toId}`;
    return context.learningModel.transitionProbabilities.get(key)
      ?? context.learningModel.transitionProbabilities.get(fallback)
      ?? 0;
  }

  function edgePrior(fromId: string, toId: string): number {
    const exact = edgeKey(receiverRegion, hourBucket, fromId, toId);
    const regionFallback = edgeKey(receiverRegion, -1, fromId, toId);
    const unknownExact = edgeKey('unknown', hourBucket, fromId, toId);
    const unknownFallback = edgeKey('unknown', -1, fromId, toId);
    return context.learningModel.edgeScores.get(exact)
      ?? context.learningModel.edgeScores.get(regionFallback)
      ?? context.learningModel.edgeScores.get(unknownExact)
      ?? context.learningModel.edgeScores.get(unknownFallback)
      ?? 0;
  }

  function mlPrefixScore(prefix: string, nodeId: string): MlPrefixScore | null {
    const hash2char = normalizePathHash(prefix).slice(0, 2);
    if (!hash2char) return null;
    return context.mlPrefixScores.get(hash2char)?.get(nodeId) ?? null;
  }

  function directAnchorScore(candidate: MeshNode, hopIdx: number): number {
    const anchors = directObserverAnchors?.get(hopIdx);
    if (!anchors || anchors.length < 1) return 0;
    let best = 0;
    for (const anchor of anchors) {
      const observer = anchor.observerNode;
      if (!hasCoords(observer)) continue;
      const d = distKm(candidate, observer);
      if (d > DIRECT_ANCHOR_MAX_KM) continue;
      best = Math.max(best, 1 - d / DIRECT_ANCHOR_MAX_KM);
    }
    return best;
  }

  function motifPrior(nodeIds: string[]): number {
    if (nodeIds.length !== 2 && nodeIds.length !== 3) return 0;
    const exact = motifKey(receiverRegion, hourBucket, nodeIds);
    const regionFallback = motifKey(receiverRegion, -1, nodeIds);
    const unknownExact = motifKey('unknown', hourBucket, nodeIds);
    const unknownFallback = motifKey('unknown', -1, nodeIds);
    return context.learningModel.motifProbabilities.get(exact)
      ?? context.learningModel.motifProbabilities.get(regionFallback)
      ?? context.learningModel.motifProbabilities.get(unknownExact)
      ?? context.learningModel.motifProbabilities.get(unknownFallback)
      ?? 0;
  }

  function distanceElevationPrior(a: MeshNode, b: MeshNode): number {
    const d = distKm(a, b);
    const distScore = Math.exp(-d / 22);
    const elevA = a.elevation_m ?? 0;
    const elevB = b.elevation_m ?? 0;
    const elevScore = Math.min(1, Math.max(0, (Math.min(elevA, elevB) + 60) / 320));
    return 0.65 * distScore + 0.35 * elevScore;
  }

  const clashAdjacency = hasBlockedNodes
    ? buildClashAdjacency(candidatesPool, context.linkPairs, context.linkMetrics)
    : context.clashAdjacency;
  const hopCache = new Map<string, 1 | 2 | null>();

  function twoHopDistance(aId: string, bId: string): 1 | 2 | null {
    if (aId === bId) return null;
    const key = aId < bId ? `${aId}:${bId}` : `${bId}:${aId}`;
    const cached = hopCache.get(key);
    if (cached !== undefined) return cached;
    const neighbors = clashAdjacency.get(aId);
    if (!neighbors || neighbors.size === 0) {
      hopCache.set(key, null);
      return null;
    }
    if (neighbors.has(bId)) {
      hopCache.set(key, 1);
      return 1;
    }
    for (const mid of neighbors) {
      if (clashAdjacency.get(mid)?.has(bId)) {
        hopCache.set(key, 2);
        return 2;
      }
    }
    hopCache.set(key, null);
    return null;
  }

  function localPrefixAmbiguityPenalty(candidate: MeshNode, prevNode: MeshNode, pathHash: string): number {
    const peers = getNodesForPathHash(pathHashIndex, pathHash);
    if (peers.length <= 1) return 0;
    if (pathHash.length >= 6 && peers.length <= 2) return 0;
    if (pathHash.length >= 4 && peers.length <= 2) return 0.01;

    const inRangeKm = Math.max(PREFIX_AMBIGUITY_FLOOR_KM, nodeRange(candidate.node_id, context.coverageByNode), nodeRange(prevNode.node_id, context.coverageByNode));
    const candidateDist = distKm(candidate, prevNode);
    let localRaw = 0;
    let hopRaw = 0;
    for (const peer of peers) {
      if (peer.node_id === candidate.node_id) continue;
      const peerDist = distKm(peer, prevNode);
      if (peerDist > inRangeKm) continue;
      const distanceSimilarity = clamp(1 - Math.abs(peerDist - candidateDist) / inRangeKm, 0, 1);
      const proximity = clamp(1 - peerDist / inRangeKm, 0, 1);
      localRaw += distanceSimilarity * proximity;
      const hopDistance = twoHopDistance(candidate.node_id, peer.node_id);
      if (hopDistance === 1) hopRaw += 1.0 * distanceSimilarity;
      else if (hopDistance === 2) hopRaw += 0.5 * distanceSimilarity;
    }
    const localPenalty = clamp(localRaw * 0.08, 0, 0.14);
    const hopPenalty = clamp(hopRaw * 0.07, 0, 0.16);
    return clamp(localPenalty + hopPenalty, 0, 0.30);
  }

  /**
   * Returns a confidence boost when a candidate is the sole match for a multi-byte hash.
   * Longer hashes with a single match are near-definitive identifications.
   */
  function hashUniquenessBoost(pathHash: string): number {
    const matchCount = getNodesForPathHash(pathHashIndex, pathHash).length;
    if (matchCount !== 1) return 0;
    if (pathHash.length >= 6) return 0.42; // 3-byte hash, single global match: essentially definitive
    if (pathHash.length >= 4) return 0.26; // 2-byte hash, single global match: very strong evidence
    return 0;
  }

  function multibyteConfidenceFloor(pathHash: string): number {
    const matchCount = getNodesForPathHash(pathHashIndex, pathHash).length;
    if (matchCount !== 1) return 0;
    if (pathHash.length >= 6) return 0.985;
    if (pathHash.length >= 4) return 0.93;
    return 0;
  }

  // --- Viterbi HMM decoder ---
  // Replaces the budget-limited DFS. Finds the globally optimal node assignment
  // across all hop positions simultaneously in O(K²·N) time.

  const N = normalizedHashes.length;

  // Precompute total hash-match counts for ambiguity scoring (per hop)
  const hashMatchCounts = normalizedHashes.map((h) => getNodesForPathHash(pathHashIndex, h).length);

  // Build trellis: trellis[hopIdx] = candidate nodes at that hop.
  // hopIdx N-1 is adjacent to rx; hopIdx 0 is adjacent to src.
  function trustedNeighborMatchesPrefix(nodeId: string, prefix: string): boolean {
    const neighbors = context.trustedPathNeighbors.get(nodeId);
    if (!neighbors || neighbors.size < 1) return false;
    for (const neighborId of neighbors) {
      if (blockedNodeIds.has(neighborId)) continue;
      if (nodePathHash(neighborId, prefix) === prefix) return true;
    }
    return false;
  }

  function candidateColumnScore(candidate: MeshNode, hopIdx: number, prefix: string, allMatches: MeshNode[]): number {
    const prevPrefix = hopIdx < N - 1 ? normalizedHashes[hopIdx + 1]! : '';
    const nextPrefix = hopIdx > 0 ? normalizedHashes[hopIdx - 1]! : '';
    const terminalKey = linkKey(candidate.node_id, rx.node_id);
    const terminalTrusted = hopIdx === N - 1 && context.trustedPathPairs.has(terminalKey);
    const towardRxTrusted = prevPrefix ? trustedNeighborMatchesPrefix(candidate.node_id, prevPrefix) : false;
    const towardSrcTrusted = nextPrefix ? trustedNeighborMatchesPrefix(candidate.node_id, nextPrefix) : false;
    const sourceTrusted = hasCoords(src) && context.trustedPathPairs.has(linkKey(candidate.node_id, src.node_id));
    const uniqueMultibyte = prefix.length >= 4 && allMatches.length === 1;
    const prefixScore = prefixPrior(prefix, prevPrefix, candidate.node_id);
    const observerScore = observerHopPrior(candidate, rx, activeObserverHopHints, typicalHopKm);
    const anchorScore = directAnchorScore(candidate, hopIdx);
    const mlScore = mlPrefixScore(prefix, candidate.node_id)?.score ?? 0;
    const sourceProgress = sourceProgressScore(candidate, rx, src);
    const distancePenalty = distKm(candidate, rx) / 260;

    return (terminalTrusted ? 8 : 0)
      + (towardRxTrusted ? 5 : 0)
      + (towardSrcTrusted ? 2 : 0)
      + (sourceTrusted ? 1.5 : 0)
      + (uniqueMultibyte ? 10 : 0)
      + (mlScore >= ML_PREFIX_DOMINANT_SCORE ? mlScore * 4 : mlScore * 1.2)
      + anchorScore * 3
      + prefixScore * 2
      + clamp(observerScore, -1, 1) * 0.8
      + clamp(sourceProgress, -1, 1) * 0.45
      - distancePenalty;
  }

  function buildColumn(hopIdx: number): MeshNode[] {
    const prefix = normalizedHashes[hopIdx]!;
    const anchor = anchorNodes?.get(hopIdx);
    if (anchor && hasCoords(anchor) && !blockedNodeIds.has(anchor.node_id)) {
      if (nodePathHash(anchor.node_id, prefix) === prefix) return [anchor];
    }
    const matches = getNodesForPathHash(pathHashIndex, prefix).filter((n) => !blockedNodeIds.has(n.node_id));
    if (matches.length <= MAX_TRELLIS_CANDIDATES_PER_HOP) return matches;

    const ranked = matches
      .map((node) => ({
        node,
        score: candidateColumnScore(node, hopIdx, prefix, matches),
        trusted: context.trustedPathNeighbors.has(node.node_id)
          || context.trustedPathPairs.has(linkKey(node.node_id, rx.node_id)),
        directAnchored: directAnchorScore(node, hopIdx) > 0,
        mlDominant: (mlPrefixScore(prefix, node.node_id)?.score ?? 0) >= ML_PREFIX_DOMINANT_SCORE,
        priorBacked: prefixPrior(prefix, hopIdx < N - 1 ? normalizedHashes[hopIdx + 1]! : '', node.node_id) > 0,
        protected: prefix.length >= 4 && matches.length === 1,
      }))
      .sort((a, b) => b.score - a.score);

    const selected = new Map<string, MeshNode>();
    for (const predicate of [
      (item: typeof ranked[number]) => item.protected,
      (item: typeof ranked[number]) => item.trusted,
      (item: typeof ranked[number]) => item.directAnchored && (item.trusted || item.mlDominant || item.priorBacked || item.score >= 1.5),
      (item: typeof ranked[number]) => item.mlDominant,
      (item: typeof ranked[number]) => item.priorBacked,
    ]) {
      for (const item of ranked) {
        if (!predicate(item)) continue;
        selected.set(item.node.node_id, item.node);
        if (selected.size >= MAX_TRELLIS_CANDIDATES_PER_HOP) return Array.from(selected.values());
      }
    }
    for (const item of ranked) {
      if (!item.protected && item.score < 1 && selected.size >= Math.floor(MAX_TRELLIS_CANDIDATES_PER_HOP / 2)) continue;
      selected.set(item.node.node_id, item.node);
      if (selected.size >= MAX_TRELLIS_CANDIDATES_PER_HOP) break;
    }
    if (selected.size < Math.min(MAX_TRELLIS_CANDIDATES_PER_HOP, ranked.length)) {
      for (const item of ranked) {
        selected.set(item.node.node_id, item.node);
        if (selected.size >= MAX_TRELLIS_CANDIDATES_PER_HOP) break;
      }
    }
    return Array.from(selected.values());
  }

  const trellis: MeshNode[][] = [];
  for (let h = N - 1; h >= 0; h--) trellis[h] = buildColumn(h);

  // Compute the confidence score for assigning `candidate` to hopIdx,
  // given that the next-closer-to-rx node is `prevNode`.
  // Returns -Infinity when the transition is physically impossible.
  //
  // Only links present in context.linkPairs (itm_viable OR force_viable from the link worker)
  // are valid hops. Everything else is impossible regardless of geometry.
  function hopScore(
    candidate: MeshNode,
    prevNode: MeshNode,
    hopIdx: number,
  ): number {
    const mKey = linkKey(candidate.node_id, prevNode.node_id);

    // Link worker verdict is the sole gate: only ITM-viable links are valid hops.
    if (!context.linkPairs.has(mKey)) return -Infinity;

    const meta = context.linkMetrics.get(mKey);
    if (isImpossibleLink(meta)) return -Infinity;

    const prefix = normalizedHashes[hopIdx]!;
    const prevPrefix = hopIdx < N - 1 ? normalizedHashes[hopIdx + 1]! : '';

    // Terminal-collision guard: when prevNode is rx and the hash matches rx's own prefix,
    // only allow this candidate if the link has sufficient confirmed observations.
    if (prevNode.node_id === rx.node_id && candidate.node_id !== rx.node_id) {
      const observerOwnPrefix = rx.role === 2 ? nodePathHash(rx.node_id, prefix) : null;
      if (observerOwnPrefix === prefix) {
        const observed = observedEvidenceCount(meta);
        const pathLoss = meta?.itm_path_loss_db;
        if (!(observed >= 120 && pathLoss != null && pathLoss <= 125)) return -Infinity;
      }
    }

    const multibyteFloor = multibyteConfidenceFloor(prefix);
    const ambiguityPenalty = localPrefixAmbiguityPenalty(candidate, prevNode, prefix);
    const uniquenessBoost = hashUniquenessBoost(prefix);
    const dirBoost = sourceProgressScore(candidate, prevNode, src) * 0.75;
    const obsBoost = observerHopPrior(candidate, prevNode, activeObserverHopHints, typicalHopKm);
    const prior = distanceElevationPrior(candidate, prevNode);
    const prefixBoost = prefixPrior(prefix, prevPrefix, candidate.node_id);
    const transBoost = transitionPrior(candidate.node_id, prevNode.node_id);
    const motifBoost = motifPrior([candidate.node_id, prevNode.node_id]);
    const edgeBoost = edgePrior(candidate.node_id, prevNode.node_id);
    const mlScore = mlPrefixScore(prefix, candidate.node_id)?.score ?? 0;
    const mlBoost = mlScore >= ML_PREFIX_DOMINANT_SCORE ? Math.min(0.18, mlScore * 0.16) : Math.min(0.06, mlScore * 0.06);
    const trustedBoost = context.trustedPathPairs.has(mKey) ? 0.10 : 0;
    const hasAnchorEvidence = mlBoost > 0 || prefixBoost > 0 || edgeBoost > 0 || trustedBoost > 0 || context.observedLinkPairs.has(mKey);
    const anchorBoost = hasAnchorEvidence ? directAnchorScore(candidate, hopIdx) * 0.12 : 0;
    const affinityBoost = neighborAffinityPreference(
      context.neighborAffinity,
      context.neighborAffinityNeighbors,
      candidate.node_id,
      prevNode.node_id,
    );

    // --- Confirmed tier: link has real observed packet traffic ---
    if (context.observedLinkPairs.has(mKey)) {
      const confirmedFloor = strongConfirmedFloor(meta);
      const baseConf = confirmedLinkConfidence(meta, candidate.node_id, prevNode.node_id, {
        prefix: prefixBoost * 0.2 + clamp(dirBoost, -1, 1) * 0.08 + clamp(obsBoost, -1, 1) * OBSERVER_HOP_WEIGHT_CONFIRMED + uniquenessBoost + mlBoost + anchorBoost + 0.16,
        transition: transBoost * 0.24,
        motif: motifBoost * 0.2,
        edge: edgeBoost * 0.3,
        affinity: affinityBoost * 0.55,
        ambiguity: -ambiguityPenalty,
      });
      return Math.max(baseConf, confirmedFloor, multibyteFloor);
    }

    // --- ITM-viable but unobserved: valid link, lower confidence ---
    // linkColorPreference encodes path-loss quality: green (+0.28) > amber (+0.14) > red (+0.02),
    // so the Viterbi naturally prefers paths through lower-loss (stronger) links.
    const dist = distKm(candidate, prevNode);
    const distancePenalty = Math.min(0.12, dist / 120);
    const linkQuality = linkColorPreference(meta) + radioNeighborPreference(meta) + multibytePathPreference(meta);
    const rawConf = Math.max(
      multibyteFloor,
      0.08,
      0.2 + prior * 0.34
        + prefixBoost * 0.22 + transBoost * 0.25 + motifBoost * 0.18 + edgeBoost * 0.28
        + clamp(dirBoost, -1, 1) * 0.1 + clamp(obsBoost, -1, 1) * OBSERVER_HOP_WEIGHT_REACHABLE
        + linkQuality + affinityBoost + uniquenessBoost + mlBoost + trustedBoost + anchorBoost - distancePenalty - ambiguityPenalty
        - (hashMatchCounts[hopIdx]! - 1) * 0.01,
    );
    const priorStrength = prefixBoost * 0.22 + transBoost * 0.25 + edgeBoost * 0.28 + motifBoost * 0.18;
    const nonLinkCap = Math.min(0.62, 0.41 + priorStrength * 0.25 + affinityBoost * 0.35);
    return multibyteFloor >= BETA_PURPLE_THRESHOLD ? rawConf : Math.min(rawConf, nonLinkCap);
  }

  // dp[hopIdx][j] = best total score of path from rx through trellis[hopIdx][j]
  // back[hopIdx][j] = index into trellis[hopIdx+1] (or -1 for hopIdx=N-1 which uses rx)
  const dp: number[][] = trellis.map((col) => new Array(col.length).fill(-Infinity));
  const back: number[][] = trellis.map((col) => new Array(col.length).fill(-1));

  // Initialise: hopIdx = N-1 (adjacent to rx), prevNode = rx
  for (let j = 0; j < trellis[N - 1].length; j++) {
    const s = hopScore(trellis[N - 1][j]!, rx, N - 1);
    if (isFinite(s)) dp[N - 1][j] = s;
  }

  // Forward pass: hopIdx = N-2 down to 0
  for (let hopIdx = N - 2; hopIdx >= 0; hopIdx--) {
    const prevHopIdx = hopIdx + 1;
    for (let j = 0; j < trellis[hopIdx].length; j++) {
      const candidate = trellis[hopIdx][j]!;
      for (let k = 0; k < trellis[prevHopIdx].length; k++) {
        if (!isFinite(dp[prevHopIdx][k]!)) continue;
        const prevNode = trellis[prevHopIdx][k]!;
        if (prevNode.node_id === candidate.node_id) continue; // no self-loops
        const s = hopScore(candidate, prevNode, hopIdx);
        if (!isFinite(s)) continue;
        const total = dp[prevHopIdx][k]! + s;
        if (total > dp[hopIdx][j]!) {
          dp[hopIdx][j] = total;
          back[hopIdx][j] = k;
        }
      }
    }
  }

  // Find best terminal at hopIdx = 0 (closest to src)
  let bestJ = -1;
  let bestTotal = -Infinity;
  for (let j = 0; j < trellis[0].length; j++) {
    if (!isFinite(dp[0][j]!)) continue;
    let score = dp[0][j]!;
    // Small bonus when this node is also reachable from src
    if (src && hasCoords(src) && distKm(trellis[0][j]!, src) <= MAX_HOP_KM) score += 0.05;
    if (score > bestTotal) { bestTotal = score; bestJ = j; }
  }

  if (bestJ === -1) return null;

  // Backtrack: reconstruct hops in src→rx order (hopIdx 0 → N-1)
  const hops: HopResult[] = new Array(N);
  let idx = bestJ;
  for (let hopIdx = 0; hopIdx < N; hopIdx++) {
    const node = trellis[hopIdx][idx]!;
    // Conf = score of this hop relative to its closer-to-rx neighbour
    const prevNode = hopIdx === N - 1 ? rx : trellis[hopIdx + 1][back[hopIdx][idx]!]!;
    const conf = Math.max(0.03, hopScore(node, prevNode, hopIdx));
    hops[hopIdx] = { node, conf };
    if (hopIdx < N - 1) idx = back[hopIdx][idx]!;
  }
  if (hops.length === 0) return null;

  const totalHops = N;
  const meanHopConfidence = hops.reduce((sum, h) => sum + h.conf, 0) / hops.length;
  const resolvedRatio = hops.length / totalHops;
  const rawConfidence = meanHopConfidence * resolvedRatio;
  const calibratedConfidence = rawConfidence * context.learningModel.confidenceScale + context.learningModel.confidenceBias;
  const confidence = clamp(calibratedConfidence, 0, 1);

  const firstHopPrefix = normalizedHashes[0] ?? null;
  const srcPrefix = hasCoords(src) && firstHopPrefix ? nodePathHash(src.node_id, firstHopPrefix) : null;
  const prependSource = options?.disableSourcePrepend
    ? false
    : Boolean(options?.forceIncludeSource
      ? hasCoords(src)
      : (hasCoords(src) && srcPrefix && srcPrefix !== firstHopPrefix));

  const pathNodes: MeshNode[] = [
    ...(prependSource && hasCoords(src) ? [src] : []),
    ...hops.map((h) => h.node),
    rx,
  ];

  if (pathNodes.length < 2) return null;

  const segmentConfidence: number[] = [];
  const hasSource = prependSource;
  for (let i = 0; i < pathNodes.length - 1; i++) {
    if (hasSource && i === 0) {
      segmentConfidence.push(hops[0]?.conf ?? confidence);
      continue;
    }
    const hopIdx = hasSource ? i - 1 : i;
    segmentConfidence.push(hops[hopIdx]?.conf ?? hops[hops.length - 1]?.conf ?? confidence);
  }

  return {
    path: pathNodes.map((n) => [n.lat!, n.lon!]),
    confidence,
    segmentConfidence,
    nodeIds: pathNodes.map((n) => n.node_id),
  };
}

async function buildLearningModel(network: string): Promise<PathLearningModel> {
  const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
    query<{
      prefix: string;
      receiver_region: string;
      prev_prefix: string | null;
      node_id: string;
      probability: number;
    }>(
      `SELECT prefix, receiver_region, prev_prefix, node_id, probability
       FROM path_prefix_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
    ),
    query<{
      from_node_id: string;
      to_node_id: string;
      receiver_region: string;
      probability: number;
    }>(
      `SELECT from_node_id, to_node_id, receiver_region, probability
       FROM path_transition_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
    ),
    query<{
      from_node_id: string;
      to_node_id: string;
      receiver_region: string;
      hour_bucket: number;
      score: number;
    }>(
      `SELECT from_node_id, to_node_id, receiver_region, hour_bucket, score
       FROM path_edge_priors
       WHERE network = $1
       ORDER BY score DESC, observed_count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
    ),
    query<{
      receiver_region: string;
      hour_bucket: number;
      motif_len: number;
      node_ids: string;
      probability: number;
    }>(
      `SELECT receiver_region, hour_bucket, motif_len, node_ids, probability
       FROM path_motif_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
    ),
    query<{
      confidence_scale: number;
      confidence_bias: number;
    }>(
      `SELECT confidence_scale, confidence_bias
       FROM path_model_calibration
       WHERE network = $1`,
      [network],
    ),
  ]);

  const prefixProbabilities = new Map<string, number>();
  for (const row of prefixRows.rows) {
    const key = `${row.receiver_region}|${row.prefix}|${row.prev_prefix ?? ''}|${row.node_id}`;
    prefixProbabilities.set(key, Number(row.probability));
  }

  const transitionProbabilities = new Map<string, number>();
  for (const row of transitionRows.rows) {
    const key = `${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`;
    transitionProbabilities.set(key, Number(row.probability));
  }

  const edgeScores = new Map<string, number>();
  const edgeTotals = new Map<string, { sum: number; count: number }>();
  for (const row of edgeRows.rows) {
    const key = `${row.receiver_region}|${Number(row.hour_bucket)}|${row.from_node_id}|${row.to_node_id}`;
    const score = Number(row.score);
    edgeScores.set(key, score);
    const aggregateKey = `${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`;
    const agg = edgeTotals.get(aggregateKey) ?? { sum: 0, count: 0 };
    agg.sum += score;
    agg.count += 1;
    edgeTotals.set(aggregateKey, agg);
  }
  for (const [aggregateKey, agg] of edgeTotals) {
    if (agg.count <= 0) continue;
    const [region, from, to] = aggregateKey.split('|');
    if (!region || !from || !to) continue;
    edgeScores.set(`${region}|-1|${from}|${to}`, agg.sum / agg.count);
  }

  const motifProbabilities = new Map<string, number>();
  const motifTotals = new Map<string, { sum: number; count: number }>();
  for (const row of motifRows.rows) {
    const key = `${row.receiver_region}|${Number(row.hour_bucket)}|${Number(row.motif_len)}|${row.node_ids}`;
    const probability = Number(row.probability);
    motifProbabilities.set(key, probability);

    const aggregateKey = `${row.receiver_region}|${Number(row.motif_len)}|${row.node_ids}`;
    const agg = motifTotals.get(aggregateKey) ?? { sum: 0, count: 0 };
    agg.sum += probability;
    agg.count += 1;
    motifTotals.set(aggregateKey, agg);
  }
  for (const [aggregateKey, agg] of motifTotals) {
    if (agg.count <= 0) continue;
    const [region, motifLen, nodeIds] = aggregateKey.split('|');
    if (!region || !motifLen || !nodeIds) continue;
    motifProbabilities.set(`${region}|-1|${motifLen}|${nodeIds}`, agg.sum / agg.count);
  }

  const calibration = calibrationRows.rows[0];
  return {
    prefixProbabilities,
    transitionProbabilities,
    edgeScores,
    motifProbabilities,
    confidenceScale: Number(calibration?.confidence_scale ?? 1),
    confidenceBias: Number(calibration?.confidence_bias ?? 0),
    bucketHours: 6,
  };
}

async function loadContext(network: string): Promise<BetaResolveContext> {
  const now = Date.now();
  const cached = contextCache.get(network);
  if (cached && now - cached.loadedAt < CONTEXT_TTL_MS) return cached;
  const sourceNetworks = expandResolverScope(network);

  const [nodeRows, coverageRows, linkRows, mlScoreRows, learningModel, neighborAffinity] = await Promise.all([
    query<MeshNode>(
      `SELECT node_id, name, lat, lon, iata, role, elevation_m, last_seen::text AS last_seen
       FROM nodes
       WHERE network = ANY($1)
         AND (name IS NULL OR name NOT LIKE '%🚫%')`,
      [sourceNetworks],
    ),
    query<NodeCoverage>(
      `SELECT nc.node_id, nc.radius_m
       FROM node_coverage nc
       JOIN nodes n ON n.node_id = nc.node_id
       WHERE n.network = ANY($1)
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')`,
      [sourceNetworks],
    ),
    query<{
      node_a_id: string;
      node_b_id: string;
      observed_count: number;
      multibyte_observed_count: number;
      itm_path_loss_db: number | null;
      itm_viable: boolean | null;
      force_viable: boolean | null;
      count_a_to_b: number | null;
      count_b_to_a: number | null;
    }>(
      `SELECT nl.node_a_id, nl.node_b_id, nl.observed_count, nl.multibyte_observed_count, nl.itm_path_loss_db, nl.itm_viable, nl.force_viable, nl.count_a_to_b, nl.count_b_to_a
       FROM node_links nl
       JOIN nodes a ON a.node_id = nl.node_a_id
       JOIN nodes b ON b.node_id = nl.node_b_id
       WHERE (nl.itm_viable IS NOT NULL OR nl.force_viable = true)
         AND a.network = ANY($1)
         AND b.network = ANY($1)
         AND (a.name IS NULL OR a.name NOT LIKE '%🚫%')
         AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')`,
      [sourceNetworks],
    ),
    query<{
      hash_2char: string;
      node_id: string;
      score: number;
      observation_count: number;
    }>(
      `SELECT hash_2char, node_id, score, observation_count
       FROM ml_path_prefix_scores
       WHERE network = $1
         AND score >= $2
       ORDER BY score DESC, observation_count DESC
       LIMIT $3`,
      [network, ML_PREFIX_SCORE_MIN, MODEL_LIMIT],
    ),
    buildLearningModel(network),
    buildNeighborAffinityMap(network, query),
  ]);

  const nodesById = new Map<string, MeshNode>();
  for (const row of nodeRows.rows) nodesById.set(row.node_id, row);

  const coverageByNode = new Map<string, number>();
  for (const row of coverageRows.rows) {
    if (row.radius_m != null) coverageByNode.set(row.node_id, Number(row.radius_m));
  }

  const linkPairs = new Set<string>();
  const observedLinkPairs = new Set<string>();
  const trustedPathPairs = new Set<string>();
  const trustedPathNeighbors = new Map<string, Set<string>>();
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const row of linkRows.rows) {
    const key = linkKey(row.node_a_id, row.node_b_id);
    if (row.itm_viable === true || row.force_viable === true) linkPairs.add(key);
    // observedLinkPairs only contains links proven by real packet observations.
    // These are the only links that qualify for the confirmed (highest-confidence) candidate tier.
    if (
      (row.itm_viable === true || row.force_viable === true)
      && (Number(row.observed_count ?? 0) > 0 || Number(row.multibyte_observed_count ?? 0) > 0)
    ) {
      observedLinkPairs.add(key);
    }
    const a = nodesById.get(row.node_a_id);
    const b = nodesById.get(row.node_b_id);
    if (
      row.itm_viable === true
      && Number(row.multibyte_observed_count ?? 0) > 0
      && hasCoords(a)
      && hasCoords(b)
      && distKm(a, b) <= TRUSTED_PATH_MAX_KM
    ) {
      trustedPathPairs.add(key);
      addUndirectedNeighbor(trustedPathNeighbors, row.node_a_id, row.node_b_id);
    }
    linkMetrics.set(key, {
      observed_count: Number(row.observed_count ?? 0),
      multibyte_observed_count: Number(row.multibyte_observed_count ?? 0),
      itm_path_loss_db: row.itm_path_loss_db == null ? null : Number(row.itm_path_loss_db),
      itm_viable: row.itm_viable ?? null,
      count_a_to_b: row.count_a_to_b == null ? null : Number(row.count_a_to_b),
      count_b_to_a: row.count_b_to_a == null ? null : Number(row.count_b_to_a),
    });
  }

  const neighborAffinityNeighbors = buildNeighborAffinityAdjacency(neighborAffinity);
  const mlPrefixScores = new Map<string, Map<string, MlPrefixScore>>();
  for (const row of mlScoreRows.rows) {
    const hash = normalizePathHash(row.hash_2char).slice(0, 2);
    if (!hash || !row.node_id) continue;
    let byNode = mlPrefixScores.get(hash);
    if (!byNode) {
      byNode = new Map<string, MlPrefixScore>();
      mlPrefixScores.set(hash, byNode);
    }
    byNode.set(row.node_id, {
      score: Number(row.score ?? 0),
      observationCount: Number(row.observation_count ?? 0),
    });
  }

  const repeaterNodes = Array.from(nodesById.values()).filter(
    (node) => hasCoords(node) && (node.role === null || node.role === 2),
  );
  const repeaterPathHashIndex = buildNodePathHashIndex(repeaterNodes);
  const clashAdjacency = buildClashAdjacency(repeaterNodes, linkPairs, linkMetrics);

  const context: BetaResolveContext = {
    loadedAt: now,
    nodesById,
    repeaterNodes,
    repeaterPathHashIndex,
    coverageByNode,
    linkPairs,
    observedLinkPairs,
    trustedPathPairs,
    trustedPathNeighbors,
    linkMetrics,
    clashAdjacency,
    neighborAffinity,
    neighborAffinityNeighbors,
    mlPrefixScores,
    learningModel,
  };
  contextCache.set(network, context);
  return context;
}

export type BetaResolvedPayload = {
  ok: boolean;
  packetHash: string;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  permutationCount: number;
  remainingHops: number | null;
  purplePath: [number, number][] | null;
  extraPurplePaths: [number, number][][];
  redPath: [number, number][] | null;
  redSegments: Array<[[number, number], [number, number]]>;
  completionPaths: [number, number][][];
  threshold: number;
  debug: {
    hopsRequested: number;
    hopsUsed: number;
    rxNodeId: string | null;
    srcNodeId: string | null;
    computedAt: string;
  };
};

async function recordPredictedOnline(nodeIds: string[] | null | undefined): Promise<void> {
  if (!Array.isArray(nodeIds) || nodeIds.length < 1) return;
  await touchNodesPredictedOnline(nodeIds);
}

export type PathResolutionOptions = {
  /** Historical rebuilds must not turn old paths into a current online signal. */
  touchPredictedOnline?: boolean;
  /** Bulk rebuilds emit their own summary and should not log each packet. */
  log?: boolean;
};

function shouldTouchPredictedOnline(options?: PathResolutionOptions): boolean {
  return options?.touchPredictedOnline !== false;
}

function logPathResolution(options: PathResolutionOptions | undefined, message: string): void {
  if (options?.log !== false) console.log(message);
}

function warnPathResolution(options: PathResolutionOptions | undefined, message: string): void {
  if (options?.log !== false) console.warn(message);
}

export async function resolveBetaPathForPacketHash(
  packetHash: string,
  network: string,
  observer?: string,
  stickyMap?: Map<string, string>,
  stickyAgeFraction?: number,
  options?: PathResolutionOptions,
): Promise<BetaResolvedPayload | null> {
  const sourceNetworks = expandResolverScope(network);
  const [packetResult, observerHopResult] = await Promise.all([
    query<PathPacket>(
      `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, path_hash_size_bytes
       FROM packets
       WHERE packet_hash = $1
         AND network = ANY($2)
         AND ($3 = 'test' OR split_part(topic, '/', 1) <> 'meshcore-test')
         AND NOT EXISTS (
           SELECT 1 FROM nodes private_node
           WHERE private_node.name LIKE '%🚫%'
             AND private_node.node_id IN (packets.rx_node_id, packets.src_node_id)
         )
         ${observer ? 'AND rx_node_id = $4' : ''}
       ORDER BY COALESCE(cardinality(path_hashes), 0) DESC,
                CASE WHEN path_hash_size_bytes IS NOT NULL THEN 1 ELSE 0 END DESC,
                CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                hop_count ASC NULLS LAST,
                time ASC
       LIMIT 32`,
      observer ? [packetHash, sourceNetworks, network, observer] : [packetHash, sourceNetworks, network],
    ),
    query<{ rx_node_id: string; hop_count: number | null }>(
      `SELECT rx_node_id, MIN(hop_count) AS hop_count
       FROM packets
       WHERE packet_hash = $1
         AND network = ANY($2)
         AND ($3 = 'test' OR split_part(topic, '/', 1) <> 'meshcore-test')
         AND NOT EXISTS (
           SELECT 1 FROM nodes private_node
           WHERE private_node.name LIKE '%🚫%'
             AND private_node.node_id IN (packets.rx_node_id, packets.src_node_id)
         )
         AND rx_node_id IS NOT NULL
       GROUP BY rx_node_id`,
      [packetHash, sourceNetworks, network],
    ),
  ]);

  const context = await loadContext(network);
  const preparedByObserver = new Map<string, PreparedPacketObservation>();
  for (const row of packetResult.rows) {
    const key = row.rx_node_id ?? '__no_observer__';
    const rxNode = row.rx_node_id ? (context.nodesById.get(row.rx_node_id) ?? null) : null;
    const prepared = preparePacketObservation(row, rxNode);
    if (prepared.ignoreForPathing) continue;
    const existing = preparedByObserver.get(key);
    if (!existing || compareCanonicalObserverObservation(prepared, existing) < 0) {
      preparedByObserver.set(key, prepared);
    }
  }
  const packet = Array.from(preparedByObserver.values()).sort(comparePreferredResolvedObservation)[0];
  if (!packet) {
    logPathResolution(options, `[path-beta] hash=${packetHash} network=${network} mode=none reason=all-observations-ignored-self-echo`);
    return null;
  }
  // Private nodes are removed while loading the resolver context, before any
  // path geometry can be derived. Keep this boundary as an identity projection
  // so every return path remains explicit and cannot reintroduce reversible
  // coordinate masking.
  const applyHiddenMask = (payload: BetaResolvedPayload) => payload;
  const logPrefix = `[path-beta] hash=${packetHash} network=${network}`;

  const rx = packet.rx ?? undefined;
  if (!hasCoords(rx)) {
    logPathResolution(options, `${logPrefix} mode=none reason=missing-rx-coords`);
    return applyHiddenMask({
      ok: true,
      packetHash,
      mode: 'none',
      confidence: null,
      permutationCount: 0,
      remainingHops: null,
      purplePath: null,
      extraPurplePaths: [],
      redPath: null,
      redSegments: [],
      completionPaths: [],
      threshold: BETA_PURPLE_THRESHOLD,
      debug: {
        hopsRequested: packet.hashes.length,
        hopsUsed: 0,
        rxNodeId: packet.packet.rx_node_id,
        srcNodeId: packet.packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    });
  }

  const src = packet.packet.src_node_id ? (context.nodesById.get(packet.packet.src_node_id) ?? null) : null;
  const hashes = packet.hashes;

  // Validate path hash lengths against wire-format hash size when available (#4)
  const expectedHexLen = packet.packet.path_hash_size_bytes != null ? packet.packet.path_hash_size_bytes * 2 : null;
  const validatedHashes = expectedHexLen != null
    ? hashes.filter((h) => {
      if (h.length !== expectedHexLen) {
        warnPathResolution(options, `${logPrefix} hash length mismatch: expected ${expectedHexLen} hex chars, got ${h.length} ("${h}")`);
        return false;
      }
      return true;
    })
    : hashes;

  const rawHops = packet.packet.hop_count != null ? validatedHashes.slice(0, Math.max(0, packet.packet.hop_count)) : validatedHashes;
  const hops = trimObserverTerminalHop(rawHops, rx);
  const forceIncludeSource = packet.packet.packet_type === 4;
  const currentHopCount = Number(packet.packet.hop_count ?? 0);
  const observerHopHints: ObserverHopHint[] = currentHopCount > 0 && packet.packet.rx_node_id
    ? observerHopResult.rows.flatMap((row) => {
      if (!row.rx_node_id || row.rx_node_id === packet.packet.rx_node_id) return [];
      const hopCount = Number(row.hop_count ?? 0);
      if (hopCount <= 0) return [];
      const observerNode = context.nodesById.get(row.rx_node_id);
      if (!observerNode || !hasCoords(observerNode)) return [];
      return [{ observerNode, hopCount, hopDelta: hopCount - currentHopCount }];
    })
    : [];
  const directAnchorIndex = buildDirectObserverAnchorIndex(
    Array.from(preparedByObserver.entries()).map(([observerId, prepared]) => ({
      observerId: observerId === '__no_observer__' ? (prepared.packet.rx_node_id ?? observerId) : observerId,
      rx: prepared.rx,
      hops: prepared.hops,
      hopCount: prepared.packet.hop_count,
    })),
  );

  if (hops.length < 1) {
    logPathResolution(options, `${logPrefix} mode=none reason=no-hops rx=${packet.packet.rx_node_id ?? 'unknown'} src=${packet.packet.src_node_id ?? 'unknown'}`);
    return applyHiddenMask({
      ok: true,
      packetHash,
      mode: 'none',
      confidence: null,
      permutationCount: 0,
      remainingHops: 0,
      purplePath: null,
      extraPurplePaths: [],
      redPath: null,
      redSegments: [],
      completionPaths: [],
      threshold: BETA_PURPLE_THRESHOLD,
      debug: {
        hopsRequested: hashes.length,
        hopsUsed: 0,
        rxNodeId: packet.packet.rx_node_id,
        srcNodeId: packet.packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    });
  }

  if ((packet.packet.path_hash_size_bytes ?? 1) > 1) {
    const exactMultibyte = resolveExactMultibyteChain(hops, context);
    if (exactMultibyte) {
      if (shouldTouchPredictedOnline(options)) await recordPredictedOnline(exactMultibyte.nodeIds);
      const exactSplit = splitResolvedAndAlternatives(
        {
          path: exactMultibyte.path,
          nodeIds: exactMultibyte.nodeIds,
          segmentConfidence: exactMultibyte.nodeIds.slice(0, -1).map(() => 1),
        },
        BETA_PURPLE_THRESHOLD,
        context.nodesById,
        context.coverageByNode,
        context.linkPairs,
        context.linkMetrics,
      );
      const purpleEdges = Math.max(0, (exactSplit.purplePath?.length ?? 0) - 1);
      const redEdges = Math.max(0, (exactSplit.redPath?.length ?? 0) - 1);
      const colorMode = purpleEdges > 0 && redEdges > 0
        ? 'mixed'
        : purpleEdges > 0
          ? 'purple-only'
          : redEdges > 0
            ? 'full-red'
            : 'none';
      logPathResolution(options,
        `${logPrefix} mode=resolved color=${colorMode} reason=exact-multibyte-chain conf=1.0000 threshold=${BETA_PURPLE_THRESHOLD.toFixed(2)} hops=${hops.length} purpleEdges=${purpleEdges} redEdges=${redEdges} remaining=${exactSplit.remainingHops} rx=${packet.packet.rx_node_id ?? 'unknown'} src=${packet.packet.src_node_id ?? 'unknown'}`,
      );
      return applyHiddenMask({
        ok: true,
        packetHash,
        mode: 'resolved',
        confidence: 1,
        permutationCount: 0,
        remainingHops: exactSplit.remainingHops,
        purplePath: exactSplit.purplePath,
        extraPurplePaths: [],
        redPath: exactSplit.redPath,
        redSegments: segmentizePath(exactSplit.redPath),
        completionPaths: [],
        threshold: BETA_PURPLE_THRESHOLD,
        debug: {
          hopsRequested: hashes.length,
          hopsUsed: hops.length,
          rxNodeId: packet.packet.rx_node_id,
          srcNodeId: packet.packet.src_node_id,
          computedAt: new Date().toISOString(),
        },
      });
    }
  }

  // Build anchor nodes: any MQTT node whose prefix unambiguously matches a hop hash
  const excludeFromAnchors = new Set([rx.node_id, ...(src ? [src.node_id] : [])]);
  const mqttNodes = Array.from(context.nodesById.values()).filter((n) => n.role === 2 && hasCoords(n));
  const hashAnchors = buildHashMatchedAnchors(hops, mqttNodes, excludeFromAnchors);
  const multibyteAnchors = buildResolvableMultibyteAnchors(
    hops,
    Array.from(context.nodesById.values()),
    excludeFromAnchors,
  );
  const anchorNodes = new Map<number, MeshNode>(multibyteAnchors);
  for (const [hopIdx, node] of hashAnchors) {
    anchorNodes.set(hopIdx, node);
  }
  // Inject sticky nodes for hops not already anchored
  if (stickyMap) {
    for (let i = 0; i < hops.length; i++) {
      if (!anchorNodes.has(i)) {
        const normalized = normalizePathHash(hops[i]);
        const stickyNodeId = normalized ? stickyMap.get(normalized) : undefined;
        if (stickyNodeId) {
          const node = context.nodesById.get(stickyNodeId);
          if (node && hasCoords(node)) anchorNodes.set(i, node);
        }
      }
    }
  }

  let result = resolveBetaPath(hops, hasCoords(src) ? src : null, rx, context, {
    forceIncludeSource,
    observerHopHints,
    anchorNodes: anchorNodes.size > 0 ? anchorNodes : undefined,
    directObserverAnchors: directAnchorsForHops(hops, directAnchorIndex),
    stickyAgeFraction,
  });
  let solvedHopCount = hops.length;
  let solverMode: 'full' | 'suffix-partial' = 'full';
  if (!result && hops.length > 1) {
    // If full solve fails, progressively solve shorter RX-side suffixes so we can
    // still render a confident purple segment near the receiver.
    for (let suffixLen = hops.length - 1; suffixLen >= 1; suffixLen--) {
      const suffix = hops.slice(hops.length - suffixLen);
      const partial = resolveBetaPath(suffix, hasCoords(src) ? src : null, rx, context, {
        forceIncludeSource: false,
        disableSourcePrepend: true,
        blockedNodeIds: hasCoords(src) ? [src.node_id] : [],
        observerHopHints,
      });
      if (!partial) continue;
      result = partial;
      solvedHopCount = suffixLen;
      solverMode = 'suffix-partial';
      break;
    }
  }

  if (result) {
    if (shouldTouchPredictedOnline(options)) await recordPredictedOnline(result.nodeIds);
    const split = splitResolvedAndAlternatives(
      result,
      BETA_PURPLE_THRESHOLD,
      context.nodesById,
      context.coverageByNode,
      context.linkPairs,
      context.linkMetrics,
    );
    let purplePath = split.purplePath;
    const extraPurplePaths: [number, number][][] = [];
    const unresolvedBySolver = Math.max(0, hops.length - solvedHopCount);
    let redPath = attachSrcToPath(split.redPath, purplePath, hasCoords(src) ? src : null, forceIncludeSource);
    if (unresolvedBySolver > 0) {
      const fallbackForUnresolved = buildFallbackPrefixPath(
        hops,
        hasCoords(src) ? src : null,
        rx,
        context.nodesById,
        context.coverageByNode,
        context.linkMetrics,
        forceIncludeSource,
        observerHopHints,
        context.repeaterPathHashIndex,
      );
      redPath = trimRedToPurpleStitch(
        fallbackForUnresolved?.path ?? redPath,
        purplePath,
      );
    }
    const unresolvedFrontEdges = (split.remainingHops ?? 0) + unresolvedBySolver;
    if (packet.packet.packet_type === 4 && hasCoords(src) && unresolvedFrontEdges > 0) {
      let sourcePartial: { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null = null;
      const maxSourcePrefixLen = Math.min(Math.max(1, unresolvedFrontEdges), Math.max(1, hops.length - 1));
      for (let prefixLen = maxSourcePrefixLen; prefixLen >= 1; prefixLen--) {
        const prefix = hops.slice(0, prefixLen);
        const candidate = reverseResolvedPath(resolveBetaPath(
          [...prefix].reverse(),
          rx,
          src,
          context,
          {
            forceIncludeSource: false,
            disableSourcePrepend: true,
            blockedNodeIds: [
              rx.node_id,
              ...(purplePath ? result.nodeIds.slice(-Math.max(1, (purplePath.length - 1))) : []),
            ],
          },
        ));
        if (!candidate || candidate.path.length < 2) continue;
        sourcePartial = candidate;
        break;
      }
      if (sourcePartial) {
        const sourceSplit = splitResolvedFromSource(
          sourcePartial,
          BETA_PURPLE_THRESHOLD,
          context.nodesById,
          context.coverageByNode,
          context.linkPairs,
          context.linkMetrics,
        );
        const sourcePurplePath = sourceSplit.purplePath;
        if (sourcePurplePath && sourcePurplePath.length >= 2) {
          extraPurplePaths.push(sourcePurplePath);
          const sourceStitch = sourcePurplePath[sourcePurplePath.length - 1] ?? null;
          redPath = trimPathBetweenStitches(
            redPath,
            sourceStitch,
            purplePath,
          );
          if (sourceStitch) {
            redPath = retargetRedPathStart(redPath, sourceStitch);
          }
        }
      }
    }
    const purpleEdges = Math.max(0, (purplePath?.length ?? 0) - 1);
    const redEdges = Math.max(0, (redPath?.length ?? 0) - 1);
    const colorMode = purpleEdges > 0 && redEdges > 0
      ? 'mixed'
      : purpleEdges > 0
        ? 'purple-only'
        : redEdges > 0
          ? 'full-red'
          : 'none';
    const reason = colorMode === 'purple-only'
      ? (solverMode === 'suffix-partial' ? 'partial-suffix-all-segments-above-threshold' : 'all-segments-above-threshold')
      : colorMode === 'mixed'
        ? (solverMode === 'suffix-partial' ? 'partial-suffix-plus-red-continuation' : 'split-at-low-confidence-segment')
        : colorMode === 'full-red'
          ? (solverMode === 'suffix-partial' ? 'partial-suffix-but-no-purple-after-threshold' : 'first-segment-below-threshold')
          : 'no-renderable-segments';
    logPathResolution(options,
      `${logPrefix} mode=resolved color=${colorMode} reason=${reason} conf=${result.confidence.toFixed(3)} threshold=${BETA_PURPLE_THRESHOLD.toFixed(2)} ` +
      `hops=${hops.length} solvedHops=${solvedHopCount} unresolvedBySolver=${unresolvedBySolver} ` +
      `purpleEdges=${purpleEdges} redEdges=${redEdges} remaining=${(split.remainingHops ?? 0) + unresolvedBySolver} ` +
      `rx=${packet.packet.rx_node_id ?? 'unknown'} src=${packet.packet.src_node_id ?? 'unknown'}`,
    );
    return applyHiddenMask({
      ok: true,
      packetHash,
      mode: 'resolved',
      confidence: result.confidence,
      permutationCount: 0,
      remainingHops: (split.remainingHops ?? 0) + unresolvedBySolver,
      purplePath,
      extraPurplePaths,
      redPath,
      redSegments: segmentizePath(redPath),
      completionPaths: [],
      threshold: BETA_PURPLE_THRESHOLD,
      debug: {
        hopsRequested: hashes.length,
        hopsUsed: hops.length,
        rxNodeId: packet.packet.rx_node_id,
        srcNodeId: packet.packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    });
  }

  const fallback = buildFallbackPrefixPath(
    hops,
    hasCoords(src) ? src : null,
    rx,
    context.nodesById,
    context.coverageByNode,
    context.linkMetrics,
    forceIncludeSource,
    observerHopHints,
    context.repeaterPathHashIndex,
  );
  if (fallback) {
    if (shouldTouchPredictedOnline(options)) await recordPredictedOnline(fallback.nodeIds);
    const redEdges = Math.max(0, fallback.path.length - 1);
    let completionPaths: [number, number][][] = [];
    let permutationCount = 0;
    if (hasCoords(src)) {
      const permutations = enumeratePrefixContinuations(
        src.node_id,
        hops,
        rx.node_id,
        context.nodesById,
        context.coverageByNode,
        context.linkMetrics,
        {
          dropStartIfNodeId: forceIncludeSource ? undefined : src.node_id,
          maxRenderPaths: MAX_RENDER_PERMUTATIONS,
          maxSearchStates: MAX_PERMUTATION_STATES,
          pathHashIndex: context.repeaterPathHashIndex,
        },
      );
      completionPaths = permutations.paths;
      permutationCount = permutations.totalCount;
    }
    logPathResolution(options,
      `${logPrefix} mode=fallback color=full-red reason=beta-solver-no-solution-prefix-fallback conf=null hops=${hops.length} purpleEdges=0 redEdges=${redEdges} ` +
      `permutations=${permutationCount} remaining=unknown rx=${packet.packet.rx_node_id ?? 'unknown'} src=${packet.packet.src_node_id ?? 'unknown'}`,
    );
    return applyHiddenMask({
      ok: true,
      packetHash,
      mode: 'fallback',
      confidence: null,
      permutationCount,
      remainingHops: null,
      purplePath: null,
      extraPurplePaths: [],
      redPath: fallback.path,
      redSegments: segmentizePath(fallback.path),
      completionPaths,
      threshold: BETA_PURPLE_THRESHOLD,
      debug: {
        hopsRequested: hashes.length,
        hopsUsed: hops.length,
        rxNodeId: packet.packet.rx_node_id,
        srcNodeId: packet.packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    });
  }

  logPathResolution(options,
    `${logPrefix} mode=none reason=unresolved hops=${hops.length} rx=${packet.packet.rx_node_id ?? 'unknown'} src=${packet.packet.src_node_id ?? 'unknown'}`,
  );
  return applyHiddenMask({
    ok: true,
    packetHash,
    mode: 'none',
    confidence: null,
    permutationCount: 0,
    remainingHops: null,
    purplePath: null,
    extraPurplePaths: [],
    redPath: null,
    redSegments: [],
    completionPaths: [],
    threshold: BETA_PURPLE_THRESHOLD,
    debug: {
      hopsRequested: hashes.length,
      hopsUsed: hops.length,
      rxNodeId: packet.packet.rx_node_id,
      srcNodeId: packet.packet.src_node_id,
      computedAt: new Date().toISOString(),
    },
  });
}

// ---------------------------------------------------------------------------
// Multi-observer resolution (Phase 5)
// ---------------------------------------------------------------------------

function findSharedPathPrefix(pathsByObserver: string[][]): string[] {
  if (pathsByObserver.length < 1) return [];
  const first = pathsByObserver[0]!;
  let len = first.length;
  for (let i = 1; i < pathsByObserver.length; i++) {
    const other = pathsByObserver[i]!;
    len = Math.min(len, other.length);
    for (let j = 0; j < len; j++) {
      if (normalizePathHash(first[j]) !== normalizePathHash(other[j])) {
        len = j;
        break;
      }
    }
  }
  return first.slice(0, len);
}

export type RegionLink = {
  fromIata: string;
  toIata: string;
  /** Coordinate [lat, lon] of the bridge node where the two regional paths meet. */
  bridgeCoord: [number, number];
};

export type MultiObserverResolvedPayload = {
  ok: boolean;
  packetHash: string;
  observerCount: number;
  sharedPrefixLength: number;
  results: BetaResolvedPayload[];
  /** Cross-region connection points: where one IATA region's path hands off to another. */
  regionLinks?: RegionLink[];
  /** High-confidence hash→nodeId assignments from this resolution, for sticky anchor persistence. Internal use only — stripped before sending to clients. */
  stickyUpdates?: Record<string, string>;
};

export async function resolveMultiObserverBetaPath(
  packetHash: string,
  network: string,
  stickyMap?: Map<string, string>,
  stickyAgeFraction?: number,
  options?: PathResolutionOptions,
): Promise<MultiObserverResolvedPayload | null> {
  const sourceNetworks = expandResolverScope(network);
  // 1. Load ALL observations for this packet hash
  const allResult = await query<PathPacket & { path_hash_size_bytes: number | null }>(
    `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, path_hash_size_bytes
     FROM packets
     WHERE packet_hash = $1
       AND network = ANY($2)
       AND ($3 = 'test' OR split_part(topic, '/', 1) <> 'meshcore-test')
       AND NOT EXISTS (
         SELECT 1 FROM nodes private_node
         WHERE private_node.name LIKE '%🚫%'
           AND private_node.node_id IN (packets.rx_node_id, packets.src_node_id)
       )
       AND rx_node_id IS NOT NULL
       AND time >= NOW() - ($4::int * INTERVAL '1 hour')
     ORDER BY time DESC,
              COALESCE(cardinality(path_hashes), 0) DESC,
              CASE WHEN path_hash_size_bytes IS NOT NULL THEN 1 ELSE 0 END DESC,
              CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
              hop_count ASC NULLS LAST,
              rx_node_id
     LIMIT $5`,
    [packetHash, sourceNetworks, network, PATH_MULTI_HISTORY_WINDOW_HOURS, PATH_MULTI_MAX_SCAN_ROWS + 1],
  );

  if (allResult.rows.length < 1) return null;
  if (allResult.rows.length > PATH_MULTI_MAX_SCAN_ROWS) {
    throw new Error('PATH_HISTORY_LIMIT');
  }

  const context = await loadContext(network);

  // 2. Group by observer, pick canonical row per observer
  const byObserver = new Map<string, PreparedPacketObservation>();
  for (const row of allResult.rows) {
    if (!row.rx_node_id) continue;
    const rxNode = context.nodesById.get(row.rx_node_id) ?? null;
    const prepared = preparePacketObservation(row, rxNode);
    if (prepared.ignoreForPathing) continue;
    const existing = byObserver.get(row.rx_node_id);
    if (!existing || compareCanonicalObserverObservation(prepared, existing) < 0) {
      byObserver.set(row.rx_node_id, prepared);
    }
  }
  if (byObserver.size > PATH_MULTI_MAX_OBSERVERS) {
    throw new Error('PATH_HISTORY_LIMIT');
  }

  // Single observer — delegate to existing per-observer resolver
  if (byObserver.size <= 1) {
    const [observerId] = byObserver.keys();
    const singleResult = await resolveBetaPathForPacketHash(packetHash, network, observerId, stickyMap, stickyAgeFraction, options);
    return singleResult
      ? { ok: true, packetHash, observerCount: 1, sharedPrefixLength: 0, results: [singleResult] }
      : null;
  }

  const logPrefix = `[path-beta-multi] hash=${packetHash} network=${network}`;

  // 3. Build per-observer data
  type ObserverEntry = {
    observerId: string;
    packet: PathPacket;
    rx: MeshNode;
    hashes: string[];
    hops: string[];
  };

  const entries: ObserverEntry[] = [];
  for (const [observerId, prepared] of byObserver) {
    const rx = prepared.rx;
    if (!hasCoords(rx)) continue;
    const hashes = prepared.hashes;
    const hops = prepared.hops;
    if (hops.length < 1) continue;
    entries.push({ observerId, packet: prepared.packet, rx, hashes, hops });
  }

  if (entries.length < 1) return null;

  if (entries.length === 1) {
    const singleResult = await resolveBetaPathForPacketHash(packetHash, network, entries[0]!.observerId, stickyMap, stickyAgeFraction, options);
    return singleResult
      ? { ok: true, packetHash, observerCount: 1, sharedPrefixLength: 0, results: [singleResult] }
      : null;
  }

  const directAnchorIndex = buildDirectObserverAnchorIndex(entries.map((entry) => ({
    observerId: entry.observerId,
    rx: entry.rx,
    hops: entry.hops,
    hopCount: entry.packet.hop_count,
  })));
  const directAnchorCount = Array.from(directAnchorIndex.values()).reduce((sum, anchors) => sum + anchors.length, 0);

  // 4. Find shared path hash prefix
  const allHops = entries.map((e) => e.hops);
  const sharedPrefix = findSharedPathPrefix(allHops);
  const sharedPrefixLength = sharedPrefix.length;

  logPathResolution(options,
    `${logPrefix} observers=${entries.length} sharedPrefix=${sharedPrefixLength} ` +
    `directAnchors=${directAnchorCount} observerIds=${entries.map((e) => e.observerId.slice(0, 8)).join(',')}`,
  );

  // 5. Pick anchor observer: most hops, most path data
  const anchor = entries.reduce((best, entry) =>
    entry.hops.length > best.hops.length ? entry : best,
  );
  const anchorIdx = entries.indexOf(anchor);

  // Derive source from any entry that has it
  const srcNodeId = entries.find((e) => e.packet.src_node_id)?.packet.src_node_id ?? null;
  const src = srcNodeId ? (context.nodesById.get(srcNodeId) ?? null) : null;
  const forceIncludeSource = entries.some((e) => e.packet.packet_type === 4);

  // All observer rx nodes for corridor relaxation
  const allRxNodes = entries.map((e) => e.rx);

  // Group entries by IATA region so cross-region observers don't distort each other's paths
  const anchorIata = anchor.rx.iata ?? 'unknown';
  const isMultiRegion = entries.some((e) => (e.rx.iata ?? 'unknown') !== anchorIata);

  // Observer hop hints — restricted to same-IATA observers only to avoid cross-region distortion
  const observerHopHints: ObserverHopHint[] = entries.flatMap((entry) => {
    const entryIata = entry.rx.iata ?? 'unknown';
    const hopCount = Number(entry.packet.hop_count ?? 0);
    if (hopCount <= 0) return [];
    return entries
      .filter((other) => other.observerId !== entry.observerId && (other.rx.iata ?? 'unknown') === entryIata)
      .map((other) => {
        const otherHopCount = Number(other.packet.hop_count ?? 0);
        return {
          observerNode: other.rx,
          hopCount: otherHopCount,
          hopDelta: otherHopCount - hopCount,
        };
      })
      .filter((h) => h.hopDelta !== 0);
  });

  // 6. Solve anchor path — corridor restricted to same-IATA observers
  const extraCorridorTargets = allRxNodes.filter(
    (n) => n.node_id !== anchor.rx.node_id && (context.nodesById.get(n.node_id)?.iata ?? 'unknown') === anchorIata,
  );
  const anchorExclude = new Set([anchor.rx.node_id, ...(src ? [src.node_id] : [])]);
  const anchorOtherObservers = entries.filter((e) => e.observerId !== anchor.observerId).map((e) => e.rx).filter((n): n is MeshNode => n !== null && hasCoords(n));
  const anchorHashAnchors = buildHashMatchedAnchors(anchor.hops, anchorOtherObservers, anchorExclude);
  // Inject sticky nodes for hops not already anchored by hash-matched observers
  if (stickyMap) {
    for (let i = 0; i < anchor.hops.length; i++) {
      if (!anchorHashAnchors.has(i)) {
        const normalized = normalizePathHash(anchor.hops[i]);
        const stickyNodeId = normalized ? stickyMap.get(normalized) : undefined;
        if (stickyNodeId) {
          const node = context.nodesById.get(stickyNodeId);
          if (node && hasCoords(node)) anchorHashAnchors.set(i, node);
        }
      }
    }
  }
  const anchorResult = resolveBetaPath(
    anchor.hops,
    hasCoords(src) ? src : null,
    anchor.rx,
    context,
    {
      forceIncludeSource,
      observerHopHints,
      extraCorridorTargets,
      anchorNodes: anchorHashAnchors.size > 0 ? anchorHashAnchors : undefined,
      directObserverAnchors: directAnchorsForHops(anchor.hops, directAnchorIndex),
      stickyAgeFraction,
    },
  );

  // Build anchor node map: hop index → resolved MeshNode
  // Also extract sticky updates: high-confidence hops that should be reused on re-resolution
  const anchorNodeMap = new Map<number, MeshNode>();
  const stickyUpdates: Record<string, string> = {};
  // Bridge node: last intermediate hop in the anchor path — the handoff point to cross-region observers
  let bridgeNode: MeshNode | null = null;
  if (anchorResult) {
    // nodeIds = [src?, hop0, hop1, ..., hopN-1, rx]
    const srcPrepended = anchorResult.nodeIds[0] === src?.node_id;
    const hopStartIdx = srcPrepended ? 1 : 0;
    const hopEndIdx = anchorResult.nodeIds.length - 1; // exclude rx
    for (let i = hopStartIdx; i < hopEndIdx; i++) {
      const nodeId = anchorResult.nodeIds[i]!;
      const node = context.nodesById.get(nodeId);
      if (node) anchorNodeMap.set(i - hopStartIdx, node);
      // Save hop as sticky anchor if confidence is above purple threshold
      const hopIdx = i - hopStartIdx;
      const hash = hopIdx < anchor.hops.length ? normalizePathHash(anchor.hops[hopIdx]) : null;
      const conf = anchorResult.segmentConfidence[i] ?? 0;
      if (hash && nodeId && conf >= BETA_PURPLE_THRESHOLD) {
        stickyUpdates[hash] = nodeId;
      }
    }
    // The bridge is the last resolved hop before the anchor's rx — this is where cross-region paths branch from
    const bridgeNodeId = hopEndIdx > 0 ? anchorResult.nodeIds[hopEndIdx - 1] : null;
    bridgeNode = bridgeNodeId ? (context.nodesById.get(bridgeNodeId) ?? null) : null;
  }

  // 7. Resolve each observer
  const results: BetaResolvedPayload[] = [];
  const predictedOnlineNodeIds = new Set<string>();
  const collectPredictedOnline = (nodeIds: string[] | null | undefined): void => {
    for (const nodeId of nodeIds ?? []) predictedOnlineNodeIds.add(nodeId);
  };

  for (let ei = 0; ei < entries.length; ei++) {
    const entry = entries[ei]!;

    if (ei === anchorIdx && anchorResult) {
      // Use anchor result directly — run through the same post-processing as resolveBetaPathForPacketHash
      collectPredictedOnline(anchorResult.nodeIds);
      const result = buildResolvedPayload(
        packetHash,
        entry,
        anchorResult,
        src,
        context,
        forceIncludeSource,
      );
      results.push(result);
      continue;
    }

    // Build anchor constraints for shared prefix hops
    // The entry's hops may be shorter or same length as anchor's; shared prefix hops get anchored
    const entryAnchorNodes = new Map<number, MeshNode>();
    for (let i = 0; i < sharedPrefixLength && i < entry.hops.length; i++) {
      const node = anchorNodeMap.get(i);
      if (node) entryAnchorNodes.set(i, node);
    }

    // Also anchor hops where another observer's node_id prefix unambiguously matches the hash
    const otherObservers = entries.filter((e) => e.observerId !== entry.observerId).map((e) => e.rx).filter((n): n is MeshNode => n !== null && hasCoords(n));
    const excludeFromEntryAnchors = new Set([entry.rx.node_id, ...(src ? [src.node_id] : [])]);
    const observerHashAnchors = buildHashMatchedAnchors(entry.hops, otherObservers, excludeFromEntryAnchors);
    for (const [i, node] of observerHashAnchors) {
      if (!entryAnchorNodes.has(i)) entryAnchorNodes.set(i, node);
    }

    // Extra corridor — same-IATA observers only to avoid cross-region distortion
    const entryIata = entry.rx.iata ?? 'unknown';
    const entryExtraCorridorTargets = allRxNodes.filter(
      (n) => n.node_id !== entry.rx.node_id && (context.nodesById.get(n.node_id)?.iata ?? 'unknown') === entryIata,
    );

    // Solve with anchor constraints for shared hops + same-IATA corridor
    const entryResult = resolveBetaPath(
      entry.hops,
      hasCoords(src) ? src : null,
      entry.rx,
      context,
      {
        forceIncludeSource,
        observerHopHints,
        anchorNodes: entryAnchorNodes.size > 0 ? entryAnchorNodes : undefined,
        directObserverAnchors: directAnchorsForHops(entry.hops, directAnchorIndex),
        extraCorridorTargets: entryExtraCorridorTargets,
        stickyAgeFraction,
      },
    );

    if (entryResult) {
      collectPredictedOnline(entryResult.nodeIds);
      const result = buildResolvedPayload(
        packetHash,
        entry,
        entryResult,
        src,
        context,
        forceIncludeSource,
      );
      results.push(result);
      continue;
    }

    // Fallback: solve without anchor constraints
    const fallbackResult = resolveBetaPath(
      entry.hops,
      hasCoords(src) ? src : null,
      entry.rx,
      context,
      {
        forceIncludeSource,
        observerHopHints,
        extraCorridorTargets: entryExtraCorridorTargets,
      },
    );

    if (fallbackResult) {
      collectPredictedOnline(fallbackResult.nodeIds);
      const result = buildResolvedPayload(
        packetHash,
        entry,
        fallbackResult,
        src,
        context,
        forceIncludeSource,
      );
      results.push(result);
      continue;
    }

    // Suffix partial solve: progressively shorter RX-side suffixes to get at least some resolved segment
    if (entry.hops.length > 1) {
      let suffixPartialResult: ReturnType<typeof resolveBetaPath> = null;
      for (let suffixLen = entry.hops.length - 1; suffixLen >= 1; suffixLen--) {
        const suffix = entry.hops.slice(entry.hops.length - suffixLen);
        const partial = resolveBetaPath(suffix, hasCoords(src) ? src : null, entry.rx, context, {
          forceIncludeSource: false,
          disableSourcePrepend: true,
          blockedNodeIds: hasCoords(src) ? [src.node_id] : [],
          observerHopHints,
        });
        if (partial) { suffixPartialResult = partial; break; }
      }
      if (suffixPartialResult) {
        collectPredictedOnline(suffixPartialResult.nodeIds);
        const result = buildResolvedPayload(
          packetHash,
          entry,
          suffixPartialResult,
          src,
          context,
          forceIncludeSource,
        );
        results.push(result);
        continue;
      }
    }

    // Last resort: fallback prefix path
    const prefixFallback = buildFallbackPrefixPath(
      entry.hops,
      hasCoords(src) ? src : null,
      entry.rx,
      context.nodesById,
      context.coverageByNode,
      context.linkMetrics,
      forceIncludeSource,
      observerHopHints,
      context.repeaterPathHashIndex,
    );

    if (prefixFallback) {
      collectPredictedOnline(prefixFallback.nodeIds);
      // Enumerate permutations from the fork point if we have a shared backbone
      let completionPaths: [number, number][][] = [];
      let permutationCount = 0;
      const forkNodeId = sharedPrefixLength > 0
        ? anchorNodeMap.get(sharedPrefixLength - 1)?.node_id
        : (hasCoords(src) ? src.node_id : undefined);

      if (forkNodeId) {
        const divergentHashes = entry.hops.slice(sharedPrefixLength);
        if (divergentHashes.length > 0) {
          const permutations = enumeratePrefixContinuations(
            forkNodeId,
            divergentHashes,
            entry.rx.node_id,
            context.nodesById,
            context.coverageByNode,
            context.linkMetrics,
            {
              maxRenderPaths: MAX_RENDER_PERMUTATIONS,
              maxSearchStates: MAX_PERMUTATION_STATES,
              pathHashIndex: context.repeaterPathHashIndex,
            },
          );
          completionPaths = permutations.paths;
          permutationCount = permutations.totalCount;
        }
      } else if (hasCoords(src)) {
        const permutations = enumeratePrefixContinuations(
          src.node_id,
          entry.hops,
          entry.rx.node_id,
          context.nodesById,
          context.coverageByNode,
          context.linkMetrics,
          {
            dropStartIfNodeId: forceIncludeSource ? undefined : src.node_id,
            maxRenderPaths: MAX_RENDER_PERMUTATIONS,
            maxSearchStates: MAX_PERMUTATION_STATES,
            pathHashIndex: context.repeaterPathHashIndex,
          },
        );
        completionPaths = permutations.paths;
        permutationCount = permutations.totalCount;
      }

      results.push({
        ok: true,
        packetHash,
        mode: 'fallback',
        confidence: null,
        permutationCount,
        remainingHops: null,
        purplePath: null,
        extraPurplePaths: [],
        redPath: prefixFallback.path,
        redSegments: segmentizePath(prefixFallback.path),
        completionPaths,
        threshold: BETA_PURPLE_THRESHOLD,
        debug: {
          hopsRequested: entry.hashes.length,
          hopsUsed: entry.hops.length,
          rxNodeId: entry.observerId,
          srcNodeId: srcNodeId,
          computedAt: new Date().toISOString(),
        },
      });
      continue;
    }

    // Complete failure for this observer
    results.push({
      ok: true,
      packetHash,
      mode: 'none',
      confidence: null,
      permutationCount: 0,
      remainingHops: null,
      purplePath: null,
      extraPurplePaths: [],
      redPath: null,
      redSegments: [],
      completionPaths: [],
      threshold: BETA_PURPLE_THRESHOLD,
      debug: {
        hopsRequested: entry.hashes.length,
        hopsUsed: entry.hops.length,
        rxNodeId: entry.observerId,
        srcNodeId: srcNodeId,
        computedAt: new Date().toISOString(),
      },
    });
  }

  const resolvedCount = results.filter((r) => r.mode === 'resolved').length;
  if (shouldTouchPredictedOnline(options)) {
    await recordPredictedOnline(Array.from(predictedOnlineNodeIds));
  }
  const bestConf = results.reduce<number | null>((best, r) => {
    if (r.confidence == null) return best;
    return best == null ? r.confidence : Math.max(best, r.confidence);
  }, null);

  // Build region link descriptors: one per cross-region IATA, pointing at the bridge coord
  const regionLinks: RegionLink[] = [];
  if (isMultiRegion && bridgeNode && hasCoords(bridgeNode)) {
    const seenIatas = new Set<string>();
    for (const entry of entries) {
      const entryIata = entry.rx.iata ?? 'unknown';
      if (entryIata !== anchorIata && !seenIatas.has(entryIata)) {
        seenIatas.add(entryIata);
        regionLinks.push({ fromIata: anchorIata, toIata: entryIata, bridgeCoord: [bridgeNode.lat!, bridgeNode.lon!] });
      }
    }
  }

  const multiRegionSuffix = isMultiRegion
    ? ` regions=${[anchorIata, ...regionLinks.map((l) => l.toIata)].join(',')}`
    : '';
  logPathResolution(options,
    `${logPrefix} done observers=${entries.length} resolved=${resolvedCount}/${entries.length} ` +
    `sharedPrefix=${sharedPrefixLength} bestConf=${bestConf?.toFixed(3) ?? 'null'}${multiRegionSuffix}`,
  );

  return { ok: true, packetHash, observerCount: entries.length, sharedPrefixLength, results, regionLinks: regionLinks.length > 0 ? regionLinks : undefined, stickyUpdates };
}

/** Build a BetaResolvedPayload from a successful resolveBetaPath result */
function buildResolvedPayload(
  packetHash: string,
  entry: { observerId: string; packet: PathPacket; rx: MeshNode; hashes: string[]; hops: string[] },
  result: { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] },
  src: MeshNode | null,
  context: BetaResolveContext,
  forceIncludeSource: boolean,
): BetaResolvedPayload {
  const split = splitResolvedAndAlternatives(
    result,
    BETA_PURPLE_THRESHOLD,
    context.nodesById,
    context.coverageByNode,
    context.linkPairs,
    context.linkMetrics,
  );
  let purplePath = split.purplePath;
  const extraPurplePaths: [number, number][][] = [];
  let redPath = attachSrcToPath(split.redPath, purplePath, hasCoords(src) ? src : null, forceIncludeSource);

  const unresolvedFrontEdges = split.remainingHops ?? 0;
  if (entry.packet.packet_type === 4 && hasCoords(src) && unresolvedFrontEdges > 0) {
    let sourcePartial: { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null = null;
    const maxSourcePrefixLen = Math.min(Math.max(1, unresolvedFrontEdges), Math.max(1, entry.hops.length - 1));
    for (let prefixLen = maxSourcePrefixLen; prefixLen >= 1; prefixLen--) {
      const prefix = entry.hops.slice(0, prefixLen);
      const candidate = reverseResolvedPath(resolveBetaPath(
        [...prefix].reverse(),
        entry.rx,
        src,
        context,
        {
          forceIncludeSource: false,
          disableSourcePrepend: true,
          blockedNodeIds: [
            entry.rx.node_id,
            ...(purplePath ? result.nodeIds.slice(-Math.max(1, (purplePath.length - 1))) : []),
          ],
        },
      ));
      if (!candidate || candidate.path.length < 2) continue;
      sourcePartial = candidate;
      break;
    }
    if (sourcePartial) {
      const sourceSplit = splitResolvedFromSource(
        sourcePartial,
        BETA_PURPLE_THRESHOLD,
        context.nodesById,
        context.coverageByNode,
        context.linkPairs,
        context.linkMetrics,
      );
      const sourcePurplePath = sourceSplit.purplePath;
      if (sourcePurplePath && sourcePurplePath.length >= 2) {
        extraPurplePaths.push(sourcePurplePath);
        const sourceStitch = sourcePurplePath[sourcePurplePath.length - 1] ?? null;
        redPath = trimPathBetweenStitches(redPath, sourceStitch, purplePath);
        if (sourceStitch) {
          redPath = retargetRedPathStart(redPath, sourceStitch);
        }
      }
    }
  }

  return {
    ok: true,
    packetHash,
    mode: 'resolved',
    confidence: result.confidence,
    permutationCount: 0,
    remainingHops: split.remainingHops ?? 0,
    purplePath,
    extraPurplePaths,
    redPath,
    redSegments: segmentizePath(redPath),
    completionPaths: [],
    threshold: BETA_PURPLE_THRESHOLD,
    debug: {
      hopsRequested: entry.hashes.length,
      hopsUsed: entry.hops.length,
      rxNodeId: entry.observerId,
      srcNodeId: entry.packet.src_node_id,
      computedAt: new Date().toISOString(),
    },
  };
}
