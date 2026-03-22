import { query, touchNodesPredictedOnline } from '../db/index.js';
import {
  buildNodePathHashIndex,
  countNodesForPathHash,
  getNodesForPathHash,
  nodePathHash,
  normalizePathHash,
} from '../path-hash/utils.js';
import {
  ANCHOR_CONFIDENCE_DEFAULT,
  BETA_PURPLE_THRESHOLD,
  CONTEXT_TTL_MS,
  MAX_BETA_HOPS,
  MAX_HOP_KM,
  MAX_PERMUTATION_HOP_KM,
  MAX_PERMUTATION_STATES,
  MAX_RENDER_PERMUTATIONS,
  MODEL_LIMIT,
  OBSERVER_HOP_WEIGHT_CONFIRMED,
  OBSERVER_HOP_WEIGHT_FALLBACK,
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
  turnContinuityScore,
} from './geometry.js';
import {
  compareFallbackCandidates,
  fallbackEdgeAllowed,
  isImpossibleLink,
  isLooseOrBetter,
  isWeakOrBetter,
  retargetRedPathStart,
  segmentizePath,
  softFallbackCandidateAllowed,
  trimPathBetweenStitches,
  trimRedToPurpleStitch,
} from './fallback.js';
import type { BetaResolveContext, LinkMetrics, MeshNode, NodeCoverage, ObserverHopHint, PathLearningModel, PathPacket } from './types.js';

const contextCache = new Map<string, BetaResolveContext>();

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

function confirmedLinkConfidence(
  meta: LinkMetrics | undefined,
  fromId: string,
  toId: string,
  prior?: { prefix?: number; transition?: number; motif?: number; edge?: number; ambiguity?: number },
): number {
  if (!meta) return 0;

  const observed = Number(meta.observed_count ?? 0);
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
    + linkColorPreference(meta)
    + radioNeighborPreference(meta);
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
  const observed = meta.observed_count ?? 0;
  const pathLoss = meta.itm_path_loss_db;
  if (pathLoss == null) return 0;
  const radioBoost = radioNeighborPreference(meta);
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
  const observed = meta.observed_count ?? 0;
  const pathLoss = meta.itm_path_loss_db;
  let base: number;
  if (pathLoss == null) base = observed >= 60 ? 0.72 : observed >= 30 ? 0.62 : 0.45;
  else if (observed >= 120 && pathLoss <= 121.5) base = 0.86;
  else if (observed >= 70 && pathLoss <= 129.5) base = 0.80;
  else if (observed >= 35 && pathLoss <= 137.5) base = 0.74;
  else if (pathLoss <= 137.5) base = Math.min(0.72, 0.48 + Math.log10(1 + observed) * 0.10);
  else base = Math.min(0.58, 0.40 + Math.log10(1 + observed) * 0.08);

  const radioBoost = radioNeighborPreference(meta);
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
  linkMetrics: Map<string, LinkMetrics>,
): boolean {
  const from = nodesById.get(fromId);
  const to = nodesById.get(toId);
  if (!hasCoords(from) || !hasCoords(to)) return false;

  const distance = distKm(from, to);
  if (distance > MAX_HOP_KM) return false;

  const meta = linkMetrics.get(linkKey(fromId, toId));
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
  linkMetrics: Map<string, LinkMetrics>,
): { purplePath: [number, number][] | null; redPath: [number, number][] | null; remainingHops: number } {
  const seg = result.segmentConfidence.map((v, i) => {
    const fromId = result.nodeIds[i];
    const toId = result.nodeIds[i + 1];
    if (!fromId || !toId) return v;
    if (!purpleEdgeAllowed(fromId, toId, nodesById, coverageByNode, linkMetrics)) return 0;
    // Only boost with edgeMetricConfidence for links with real observed traffic.
    // Unobserved ITM-viable links keep the Viterbi confidence as-is (capped below purple).
    const observed = linkMetrics.get(linkKey(fromId, toId))?.observed_count ?? 0;
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
  linkMetrics: Map<string, LinkMetrics>,
): { purplePath: [number, number][] | null; remainingHops: number } {
  const seg = result.segmentConfidence.map((v, i) => {
    const fromId = result.nodeIds[i];
    const toId = result.nodeIds[i + 1];
    if (!fromId || !toId) return v;
    if (!purpleEdgeAllowed(fromId, toId, nodesById, coverageByNode, linkMetrics)) return 0;
    const observed = linkMetrics.get(linkKey(fromId, toId))?.observed_count ?? 0;
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
): { path: [number, number][]; nodeIds: string[] } | null {
  const repeaters = Array.from(nodesById.values()).filter(
    (n) => hasCoords(n) && (n.role === null || n.role === 2),
  );
  const pathHashIndex = buildNodePathHashIndex(repeaters);

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
          const observed = Number(meta?.observed_count ?? 0);
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
            const observed = Number(meta?.observed_count ?? 0);
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
  options?: { dropStartIfNodeId?: string; maxRenderPaths?: number; maxSearchStates?: number; blockedNodeIds?: string[] },
): { paths: [number, number][][]; totalCount: number; truncated: boolean; longestPrefixDepth: number } {
  const maxRenderPaths = Math.max(1, options?.maxRenderPaths ?? MAX_RENDER_PERMUTATIONS);
  const maxSearchStates = Math.max(1000, options?.maxSearchStates ?? MAX_PERMUTATION_STATES);
  const candidates = Array.from(nodesById.values()).filter(
    (n) => hasCoords(n) && (n.role === null || n.role === 2),
  );
  const pathHashIndex = buildNodePathHashIndex(candidates);

  const start = nodesById.get(startNodeId);
  const end = nodesById.get(endNodeId);
  if (!start || !end || !hasCoords(start) || !hasCoords(end)) {
    return { paths: [], totalCount: 0, truncated: false, longestPrefixDepth: 0 };
  }
  const blocked = new Set(options?.blockedNodeIds ?? []);
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
  const repeaters = Array.from(context.nodesById.values()).filter(
    (n) => hasCoords(n) && (n.role === null || n.role === 2),
  );
  const pathHashIndex = buildNodePathHashIndex(repeaters, [pathHashes[0]?.length ?? 0]);
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
    extraCorridorTargets?: MeshNode[];
    /** Age of sticky anchors as fraction of their TTL (0=fresh, 1=expired). Used to decay confidence. */
    stickyAgeFraction?: number;
  },
): { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null {
  const normalizedHashes = pathHashes.map(normalizePathHash).filter(Boolean);
  if (!hasCoords(rx) || normalizedHashes.length === 0) return null;
  if (normalizedHashes.length >= MAX_BETA_HOPS) return null;
  const rxLat = rx.lat!;
  const rxLon = rx.lon!;

  type HopResult = { node: MeshNode; conf: number };
  const blockedNodeIds = new Set(options?.blockedNodeIds ?? []);
  const candidatesPool = Array.from(context.nodesById.values()).filter(
    (n) => hasCoords(n) && (n.role === null || n.role === 2) && !blockedNodeIds.has(n.node_id),
  );
  const pathHashIndex = buildNodePathHashIndex(candidatesPool);

  const totalDist = hasCoords(src) ? distKm(src, rx) : 0;
  const corridorMaxKm = Math.max(10, Math.min(80, totalDist * 0.35));
  // Typical per-hop spacing used to normalise observer hint directionality.
  // Clamped 5–40 km so hints remain meaningful in both dense and sparse networks.
  const typicalHopKm = normalizedHashes.length > 0
    ? Math.max(5, Math.min(40, totalDist / normalizedHashes.length))
    : 25;
  const receiverRegion = rx.iata ?? 'unknown';
  const bucketHours = context.learningModel.bucketHours ?? 6;
  const hourBucket = currentHourBucket(bucketHours);
  const activeObserverHopHints = options?.observerHopHints ?? [];
  const extraCorridorTargets = options?.extraCorridorTargets ?? [];
  const anchorNodes = options?.anchorNodes;
  // Decay sticky anchor weight as they age: fresh=1.0, at TTL boundary=0.7
  const stickyConfidenceScale = 1.0 - Math.max(0, Math.min(1, options?.stickyAgeFraction ?? 0)) * 0.3;

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

  const clashAdjacency = buildClashAdjacency(candidatesPool, context.linkPairs, context.linkMetrics);
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

  function clashPressure(candidate: MeshNode, pathHash: string): number {
    const peers = getNodesForPathHash(pathHashIndex, pathHash);
    if (peers.length <= 1) return 0;
    let raw = 0;
    for (const peer of peers) {
      if (peer.node_id === candidate.node_id) continue;
      const hops = twoHopDistance(candidate.node_id, peer.node_id);
      if (hops === 1) raw += 1;
      else if (hops === 2) raw += 0.5;
    }
    return clamp(raw / 3, 0, 1);
  }

  function corridorCheck(candidate: MeshNode, prevNode: MeshNode, pathHash: string, tgtLat: number, tgtLon: number, maxKm: number): boolean {
    if (!hasCoords(src)) return true;
    const bx = src.lon! - tgtLon;
    const by = src.lat! - tgtLat;
    const segLen2 = bx * bx + by * by;
    if (segLen2 < 1e-9) return true;
    const px = candidate.lon! - tgtLon;
    const py = candidate.lat! - tgtLat;
    const t = (px * bx + py * by) / segLen2;
    const pressure = clashPressure(candidate, pathHash);
    const tPadding = 0.15 + (1 - pressure) * 0.15;
    if (t < -tPadding || t > 1 + tPadding) return false;

    const projx = tgtLon + t * bx;
    const projy = tgtLat + t * by;
    const midLat = ((candidate.lat! + projy) / 2) * (Math.PI / 180);
    const kmPerLon = 111 * Math.cos(midLat);
    const dxKm = (candidate.lon! - projx) * kmPerLon;
    const dyKm = (candidate.lat! - projy) * 111;
    const crossTrackKm = Math.hypot(dxKm, dyKm);
    const corridorAllowance = maxKm * (1 + (1 - pressure) * 0.35);
    if (crossTrackKm > corridorAllowance) return false;

    return distKm(candidate, src) <= distKm(prevNode, src) + 8;
  }

  function inCorridor(candidate: MeshNode, prevNode: MeshNode, pathHash: string): boolean {
    if (!hasCoords(src)) return true;
    if (corridorCheck(candidate, prevNode, pathHash, rxLat, rxLon, corridorMaxKm)) return true;
    for (const target of extraCorridorTargets) {
      if (!hasCoords(target)) continue;
      const td = distKm(src, target);
      if (corridorCheck(candidate, prevNode, pathHash, target.lat!, target.lon!, Math.max(10, Math.min(80, td * 0.35)))) return true;
    }
    return false;
  }

  // --- Viterbi HMM decoder ---
  // Replaces the budget-limited DFS. Finds the globally optimal node assignment
  // across all hop positions simultaneously in O(K²·N) time.

  const N = normalizedHashes.length;

  // Precompute total hash-match counts for ambiguity scoring (per hop)
  const hashMatchCounts = normalizedHashes.map((h) => getNodesForPathHash(pathHashIndex, h).length);

  // Build trellis: trellis[hopIdx] = candidate nodes at that hop.
  // hopIdx N-1 is adjacent to rx; hopIdx 0 is adjacent to src.
  function buildColumn(hopIdx: number): MeshNode[] {
    const prefix = normalizedHashes[hopIdx]!;
    const anchor = anchorNodes?.get(hopIdx);
    if (anchor && hasCoords(anchor) && !blockedNodeIds.has(anchor.node_id)) {
      if (nodePathHash(anchor.node_id, prefix) === prefix) return [anchor];
    }
    return getNodesForPathHash(pathHashIndex, prefix).filter((n) => !blockedNodeIds.has(n.node_id));
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
        const observed = Number(meta?.observed_count ?? 0);
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

    // --- Confirmed tier: link has real observed packet traffic ---
    if (context.observedLinkPairs.has(mKey)) {
      const confirmedFloor = strongConfirmedFloor(meta);
      const baseConf = confirmedLinkConfidence(meta, candidate.node_id, prevNode.node_id, {
        prefix: prefixBoost * 0.2 + clamp(dirBoost, -1, 1) * 0.08 + clamp(obsBoost, -1, 1) * OBSERVER_HOP_WEIGHT_CONFIRMED + uniquenessBoost + 0.16,
        transition: transBoost * 0.24,
        motif: motifBoost * 0.2,
        edge: edgeBoost * 0.3,
        ambiguity: -ambiguityPenalty,
      });
      return Math.max(baseConf, confirmedFloor, multibyteFloor);
    }

    // --- ITM-viable but unobserved: valid link, lower confidence ---
    // linkColorPreference encodes path-loss quality: green (+0.28) > amber (+0.14) > red (+0.02),
    // so the Viterbi naturally prefers paths through lower-loss (stronger) links.
    const dist = distKm(candidate, prevNode);
    const distancePenalty = Math.min(0.12, dist / 120);
    const linkQuality = linkColorPreference(meta) + radioNeighborPreference(meta);
    const rawConf = Math.max(
      multibyteFloor,
      0.08,
      0.2 + prior * 0.34
        + prefixBoost * 0.22 + transBoost * 0.25 + motifBoost * 0.18 + edgeBoost * 0.28
        + clamp(dirBoost, -1, 1) * 0.1 + clamp(obsBoost, -1, 1) * OBSERVER_HOP_WEIGHT_REACHABLE
        + linkQuality + uniquenessBoost - distancePenalty - ambiguityPenalty
        - (hashMatchCounts[hopIdx]! - 1) * 0.01,
    );
    const priorStrength = prefixBoost * 0.22 + transBoost * 0.25 + edgeBoost * 0.28 + motifBoost * 0.18;
    const nonLinkCap = Math.min(0.50, 0.41 + priorStrength * 0.25);
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

  const [nodeRows, coverageRows, linkRows, learningModel] = await Promise.all([
    query<MeshNode>(
      `SELECT node_id, name, lat, lon, iata, role, elevation_m, last_seen::text AS last_seen
       FROM nodes
       WHERE ($1 = 'all' OR network = $1)`,
      [network],
    ),
    query<NodeCoverage>(
      `SELECT nc.node_id, nc.radius_m
       FROM node_coverage nc
       JOIN nodes n ON n.node_id = nc.node_id
       WHERE ($1 = 'all' OR n.network = $1)`,
      [network],
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
         AND ($1 = 'all' OR (a.network = $1 AND b.network = $1))`,
      [network],
    ),
    buildLearningModel(network),
  ]);

  const nodesById = new Map<string, MeshNode>();
  for (const row of nodeRows.rows) nodesById.set(row.node_id, row);

  const coverageByNode = new Map<string, number>();
  for (const row of coverageRows.rows) {
    if (row.radius_m != null) coverageByNode.set(row.node_id, Number(row.radius_m));
  }

  const linkPairs = new Set<string>();
  const observedLinkPairs = new Set<string>();
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const row of linkRows.rows) {
    const key = linkKey(row.node_a_id, row.node_b_id);
    if (row.itm_viable === true || row.force_viable === true) linkPairs.add(key);
    // observedLinkPairs only contains links proven by real packet observations.
    // These are the only links that qualify for the confirmed (highest-confidence) candidate tier.
    if ((row.itm_viable === true || row.force_viable === true) && Number(row.observed_count ?? 0) > 0) {
      observedLinkPairs.add(key);
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

  const context: BetaResolveContext = {
    loadedAt: now,
    nodesById,
    coverageByNode,
    linkPairs,
    observedLinkPairs,
    linkMetrics,
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

const PROHIBITED_NODE_MARKER = '🚫';
const HIDDEN_NODE_MASK_RADIUS_MILES = 1;

function isProhibitedMapNode(node: MeshNode | null | undefined): boolean {
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

function buildHiddenCoordMask(nodesById: Map<string, MeshNode>): Map<string, [number, number]> {
  const mask = new Map<string, [number, number]>();
  for (const node of nodesById.values()) {
    if (!hasCoords(node) || !isProhibitedMapNode(node)) continue;
    const activityKey = node.last_seen ?? 'unknown';
    const seed = `${node.node_id}|${activityKey}`;
    mask.set(hiddenCoordKey(node.lat!, node.lon!), stablePointWithinMiles(node.lat!, node.lon!, seed));
  }
  return mask;
}

function maskPoint(point: [number, number], hiddenCoordMask: Map<string, [number, number]>): [number, number] {
  return hiddenCoordMask.get(hiddenCoordKey(point[0], point[1])) ?? point;
}

function maskPath(path: [number, number][] | null, hiddenCoordMask: Map<string, [number, number]>): [number, number][] | null {
  if (!path || path.length < 1 || hiddenCoordMask.size < 1) return path;
  return path.map((point) => maskPoint(point, hiddenCoordMask));
}

function maskSegments(
  segments: Array<[[number, number], [number, number]]>,
  hiddenCoordMask: Map<string, [number, number]>,
): Array<[[number, number], [number, number]]> {
  if (segments.length < 1 || hiddenCoordMask.size < 1) return segments;
  return segments.map(([a, b]) => [maskPoint(a, hiddenCoordMask), maskPoint(b, hiddenCoordMask)]);
}

function maskResolvedPayload(
  payload: BetaResolvedPayload,
  hiddenCoordMask: Map<string, [number, number]>,
): BetaResolvedPayload {
  if (hiddenCoordMask.size < 1) return payload;
  return {
    ...payload,
    purplePath: maskPath(payload.purplePath, hiddenCoordMask),
    extraPurplePaths: payload.extraPurplePaths.map((path) => maskPath(path, hiddenCoordMask) ?? path),
    redPath: maskPath(payload.redPath, hiddenCoordMask),
    redSegments: maskSegments(payload.redSegments, hiddenCoordMask),
    completionPaths: payload.completionPaths.map((path) => maskPath(path, hiddenCoordMask) ?? path),
  };
}

async function recordPredictedOnline(nodeIds: string[] | null | undefined): Promise<void> {
  if (!Array.isArray(nodeIds) || nodeIds.length < 1) return;
  await touchNodesPredictedOnline(nodeIds);
}

export async function resolveBetaPathForPacketHash(packetHash: string, network: string, observer?: string, stickyMap?: Map<string, string>, stickyAgeFraction?: number): Promise<BetaResolvedPayload | null> {
  const [packetResult, observerHopResult] = await Promise.all([
    query<PathPacket>(
      `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, path_hash_size_bytes
       FROM packets
       WHERE packet_hash = $1
         AND ($2 = 'all' OR network = $2)
         ${observer ? 'AND rx_node_id = $3' : ''}
       ORDER BY COALESCE(cardinality(path_hashes), 0) DESC,
                CASE WHEN path_hash_size_bytes IS NOT NULL THEN 1 ELSE 0 END DESC,
                CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                hop_count ASC NULLS LAST,
                time ASC
       LIMIT 32`,
      observer ? [packetHash, network, observer] : [packetHash, network],
    ),
    query<{ rx_node_id: string; hop_count: number | null }>(
      `SELECT rx_node_id, MIN(hop_count) AS hop_count
       FROM packets
       WHERE packet_hash = $1
         AND ($2 = 'all' OR network = $2)
         AND rx_node_id IS NOT NULL
       GROUP BY rx_node_id`,
      [packetHash, network],
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
    console.log(`[path-beta] hash=${packetHash} network=${network} mode=none reason=all-observations-ignored-self-echo`);
    return null;
  }
  const hiddenCoordMask = buildHiddenCoordMask(context.nodesById);
  const applyHiddenMask = (payload: BetaResolvedPayload) => maskResolvedPayload(payload, hiddenCoordMask);
  const logPrefix = `[path-beta] hash=${packetHash} network=${network}`;

  const rx = packet.rx ?? undefined;
  if (!hasCoords(rx)) {
    console.log(`${logPrefix} mode=none reason=missing-rx-coords`);
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
        console.warn(`${logPrefix} hash length mismatch: expected ${expectedHexLen} hex chars, got ${h.length} ("${h}")`);
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

  if (hops.length < 1) {
    console.log(`${logPrefix} mode=none reason=no-hops rx=${packet.packet.rx_node_id ?? 'unknown'} src=${packet.packet.src_node_id ?? 'unknown'}`);
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
      await recordPredictedOnline(exactMultibyte.nodeIds);
      const exactSplit = splitResolvedAndAlternatives(
        {
          path: exactMultibyte.path,
          nodeIds: exactMultibyte.nodeIds,
          segmentConfidence: exactMultibyte.nodeIds.slice(0, -1).map(() => 1),
        },
        BETA_PURPLE_THRESHOLD,
        context.nodesById,
        context.coverageByNode,
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
      console.log(
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
    await recordPredictedOnline(result.nodeIds);
    const split = splitResolvedAndAlternatives(
      result,
      BETA_PURPLE_THRESHOLD,
      context.nodesById,
      context.coverageByNode,
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
    console.log(
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
  );
  if (fallback) {
    await recordPredictedOnline(fallback.nodeIds);
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
        },
      );
      completionPaths = permutations.paths;
      permutationCount = permutations.totalCount;
    }
    console.log(
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

  console.log(
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
): Promise<MultiObserverResolvedPayload | null> {
  // 1. Load ALL observations for this packet hash
  const allResult = await query<PathPacket & { path_hash_size_bytes: number | null }>(
    `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, path_hash_size_bytes
     FROM packets
     WHERE packet_hash = $1
       AND ($2 = 'all' OR network = $2)
       AND rx_node_id IS NOT NULL
     ORDER BY COALESCE(cardinality(path_hashes), 0) DESC,
              CASE WHEN path_hash_size_bytes IS NOT NULL THEN 1 ELSE 0 END DESC,
              CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
              hop_count ASC NULLS LAST,
              time ASC`,
    [packetHash, network],
  );

  if (allResult.rows.length < 1) return null;

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

  // Single observer — delegate to existing per-observer resolver
  if (byObserver.size <= 1) {
    const [observerId] = byObserver.keys();
    const singleResult = await resolveBetaPathForPacketHash(packetHash, network, observerId, stickyMap, stickyAgeFraction);
    return singleResult
      ? { ok: true, packetHash, observerCount: 1, sharedPrefixLength: 0, results: [singleResult] }
      : null;
  }

  const hiddenCoordMask = buildHiddenCoordMask(context.nodesById);
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
    const singleResult = await resolveBetaPathForPacketHash(packetHash, network, entries[0]!.observerId, stickyMap, stickyAgeFraction);
    return singleResult
      ? { ok: true, packetHash, observerCount: 1, sharedPrefixLength: 0, results: [singleResult] }
      : null;
  }

  // 4. Find shared path hash prefix
  const allHops = entries.map((e) => e.hops);
  const sharedPrefix = findSharedPathPrefix(allHops);
  const sharedPrefixLength = sharedPrefix.length;

  console.log(
    `${logPrefix} observers=${entries.length} sharedPrefix=${sharedPrefixLength} ` +
    `observerIds=${entries.map((e) => e.observerId.slice(0, 8)).join(',')}`,
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

  for (let ei = 0; ei < entries.length; ei++) {
    const entry = entries[ei]!;

    if (ei === anchorIdx && anchorResult) {
      // Use anchor result directly — run through the same post-processing as resolveBetaPathForPacketHash
      await recordPredictedOnline(anchorResult.nodeIds);
      const result = buildResolvedPayload(
        packetHash,
        entry,
        anchorResult,
        src,
        context,
        forceIncludeSource,
        observerHopHints,
      );
      results.push(maskResolvedPayload(result, hiddenCoordMask));
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
        extraCorridorTargets: entryExtraCorridorTargets,
        stickyAgeFraction,
      },
    );

    if (entryResult) {
      await recordPredictedOnline(entryResult.nodeIds);
      const result = buildResolvedPayload(
        packetHash,
        entry,
        entryResult,
        src,
        context,
        forceIncludeSource,
        observerHopHints,
      );
      results.push(maskResolvedPayload(result, hiddenCoordMask));
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
      await recordPredictedOnline(fallbackResult.nodeIds);
      const result = buildResolvedPayload(
        packetHash,
        entry,
        fallbackResult,
        src,
        context,
        forceIncludeSource,
        observerHopHints,
      );
      results.push(maskResolvedPayload(result, hiddenCoordMask));
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
        await recordPredictedOnline(suffixPartialResult.nodeIds);
        const result = buildResolvedPayload(
          packetHash,
          entry,
          suffixPartialResult,
          src,
          context,
          forceIncludeSource,
          observerHopHints,
        );
        results.push(maskResolvedPayload(result, hiddenCoordMask));
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
    );

    if (prefixFallback) {
      await recordPredictedOnline(prefixFallback.nodeIds);
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
            { maxRenderPaths: MAX_RENDER_PERMUTATIONS, maxSearchStates: MAX_PERMUTATION_STATES },
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
          },
        );
        completionPaths = permutations.paths;
        permutationCount = permutations.totalCount;
      }

      results.push(maskResolvedPayload({
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
      }, hiddenCoordMask));
      continue;
    }

    // Complete failure for this observer
    results.push(maskResolvedPayload({
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
    }, hiddenCoordMask));
  }

  const resolvedCount = results.filter((r) => r.mode === 'resolved').length;
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
  console.log(
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
  observerHopHints: ObserverHopHint[],
): BetaResolvedPayload {
  const split = splitResolvedAndAlternatives(
    result,
    BETA_PURPLE_THRESHOLD,
    context.nodesById,
    context.coverageByNode,
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

  const purpleEdges = Math.max(0, (purplePath?.length ?? 0) - 1);
  const redEdges = Math.max(0, (redPath?.length ?? 0) - 1);
  const colorMode = purpleEdges > 0 && redEdges > 0
    ? 'mixed'
    : purpleEdges > 0
      ? 'purple-only'
      : redEdges > 0
        ? 'full-red'
        : 'none';

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
