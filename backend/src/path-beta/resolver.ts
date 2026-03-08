import { MIN_LINK_OBSERVATIONS, query } from '../db/index.js';
import {
  buildNodePathHashIndex,
  countNodesForPathHash,
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

function observerHopPrior(candidate: MeshNode, prevNode: MeshNode, hints: ObserverHopHint[]): number {
  if (hints.length < 1) return 0;
  let weighted = 0;
  let totalWeight = 0;
  for (const hint of hints) {
    const observer = hint.observerNode;
    if (!hasCoords(observer) || hint.hopDelta === 0) continue;
    const prevDist = distKm(prevNode, observer);
    const candidateDist = distKm(candidate, observer);
    const towardObserver = clamp((prevDist - candidateDist) / 25, -1, 1);
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
  } else if (pathLoss <= 120) {
    base = 0.95;
  } else if (pathLoss <= 125) {
    base = 0.9;
  } else if (pathLoss <= 130) {
    base = 0.84;
  } else if (pathLoss <= 133) {
    base = 0.78;
  } else if (pathLoss <= 135) {
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
    + Number(prior?.ambiguity ?? 0);
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
  if (observed >= 120 && pathLoss != null && pathLoss <= 125) return 0.82;
  if (observed >= 70 && pathLoss != null && pathLoss <= 130) return 0.76;
  if (observed >= 35 && pathLoss != null && pathLoss <= 133) return 0.70;
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
  else if (observed >= 120 && pathLoss <= 125) base = 0.86;
  else if (observed >= 70 && pathLoss <= 130) base = 0.80;
  else if (observed >= 35 && pathLoss <= 133) base = 0.74;
  else if (pathLoss <= 135) base = Math.min(0.72, 0.48 + Math.log10(1 + observed) * 0.10);
  else base = Math.min(0.58, 0.40 + Math.log10(1 + observed) * 0.08);

  if (observed >= 120) return Math.max(base, 0.82);
  if (observed >= 70) return Math.max(base, 0.74);
  if (observed >= 35) return Math.max(base, 0.62);
  if (observed >= 20) return Math.max(base, 0.56);
  return base;
}

function splitResolvedAndAlternatives(
  result: { path: [number, number][]; segmentConfidence: number[]; nodeIds: string[] },
  threshold: number,
  linkMetrics: Map<string, LinkMetrics>,
): { purplePath: [number, number][] | null; redPath: [number, number][] | null; remainingHops: number } {
  const seg = result.segmentConfidence.map((v, i) => {
    const fromId = result.nodeIds[i];
    const toId = result.nodeIds[i + 1];
    if (!fromId || !toId) return v;
    return Math.max(v, edgeMetricConfidence(fromId, toId, linkMetrics));
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
  linkMetrics: Map<string, LinkMetrics>,
): { purplePath: [number, number][] | null; remainingHops: number } {
  const seg = result.segmentConfidence.map((v, i) => {
    const fromId = result.nodeIds[i];
    const toId = result.nodeIds[i + 1];
    if (!fromId || !toId) return v;
    return Math.max(v, edgeMetricConfidence(fromId, toId, linkMetrics));
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
    const compareCandidates = (a: MeshNode, b: MeshNode): number => {
      const hopBias = observerHopPrior(b, prev, observerHopHints) - observerHopPrior(a, prev, observerHopHints);
      const base = compareFallbackCandidates(a, b, prev, src, nextTowardRx, coverageByNode, linkMetrics);
      return base + hopBias * 0.6;
    };
    let candidates = getNodesForPathHash(pathHashIndex, prefix)
      .filter((n) => !visited.has(n.node_id) && fallbackEdgeAllowed(n, prev, coverageByNode, linkMetrics))
      .sort(compareCandidates);
    if (candidates.length < 1) {
      candidates = getNodesForPathHash(pathHashIndex, prefix)
        .filter((n) => (
          !visited.has(n.node_id)
          && softFallbackCandidateAllowed(n, prev, src, nextTowardRx)
        ))
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

function resolveBetaPath(
  pathHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  context: BetaResolveContext,
  options?: { forceIncludeSource?: boolean; disableSourcePrepend?: boolean; blockedNodeIds?: string[]; observerHopHints?: ObserverHopHint[] },
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
  const receiverRegion = rx.iata ?? 'unknown';
  const bucketHours = context.learningModel.bucketHours ?? 6;
  const hourBucket = currentHourBucket(bucketHours);
  const activeObserverHopHints = options?.observerHopHints ?? [];

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

  function inCorridor(candidate: MeshNode, prevNode: MeshNode, pathHash: string): boolean {
    if (!hasCoords(src)) return true;
    const bx = src.lon! - rxLon;
    const by = src.lat! - rxLat;
    const segLen2 = bx * bx + by * by;
    if (segLen2 < 1e-9) return true;
    const px = candidate.lon! - rxLon;
    const py = candidate.lat! - rxLat;
    const t = (px * bx + py * by) / segLen2;
    const pressure = clashPressure(candidate, pathHash);
    const tPadding = 0.15 + (1 - pressure) * 0.15;
    if (t < -tPadding || t > 1 + tPadding) return false;

    const projx = rxLon + t * bx;
    const projy = rxLat + t * by;
    const midLat = ((candidate.lat! + projy) / 2) * (Math.PI / 180);
    const kmPerLon = 111 * Math.cos(midLat);
    const dxKm = (candidate.lon! - projx) * kmPerLon;
    const dyKm = (candidate.lat! - projy) * 111;
    const crossTrackKm = Math.hypot(dxKm, dyKm);
    const corridorAllowance = corridorMaxKm * (1 + (1 - pressure) * 0.35);
    if (crossTrackKm > corridorAllowance) return false;

    return distKm(candidate, src) <= distKm(prevNode, src) + 8;
  }

  function getCandidates(prefix: string, prevPrefix: string, prevNode: MeshNode, nextTowardRx: string | null): Array<{ node: MeshNode; conf: number }> {
    const all = getNodesForPathHash(pathHashIndex, prefix);
    if (all.length === 0) return [];
    const nextTowardRxNode = nextTowardRx ? (context.nodesById.get(nextTowardRx) ?? null) : null;

    function directionalPrior(c: MeshNode): number {
      return sourceProgressScore(c, prevNode, src) * 0.75
        + turnContinuityScore(c, prevNode, nextTowardRxNode) * 0.95;
    }

    function multiObserverPrior(c: MeshNode): number {
      return observerHopPrior(c, prevNode, activeObserverHopHints);
    }

    function sortScore(c: MeshNode): number {
      const corridorBonus = inCorridor(c, prevNode, prefix) ? 0.25 : -0.6;
      return directionalPrior(c) + multiObserverPrior(c) * 0.9 - distKm(c, prevNode) / 50 + corridorBonus;
    }

    const usedIds = new Set<string>();

    const confirmed = all
      .filter((c) => {
        const key = linkKey(c.node_id, prevNode.node_id);
        if (!context.linkPairs.has(key)) return false;
        const meta = context.linkMetrics.get(key);
        if (!meta || meta.count_a_to_b == null || meta.count_b_to_a == null) return true;
        const dir = directionalSupport(meta, c.node_id, prevNode.node_id);
        const observed = meta.observed_count ?? 0;
        if (observed < 20) return true;
        return dir >= minimumDirectionalSupport(observed) * 0.6;
      })
      .sort((a, b) => sortScore(b) - sortScore(a))
      .slice(0, 16)
      .map((c) => {
        usedIds.add(c.node_id);
        const meta = context.linkMetrics.get(linkKey(c.node_id, prevNode.node_id));
        const priorBoost = prefixPrior(prefix, prevPrefix, c.node_id) * 0.2;
        const transitionBoost = transitionPrior(c.node_id, prevNode.node_id) * 0.24;
        const motifBoost = motifPrior([c.node_id, prevNode.node_id]) * 0.2
          + (nextTowardRx ? motifPrior([c.node_id, prevNode.node_id, nextTowardRx]) * 0.25 : 0);
        const edgeBoost = edgePrior(c.node_id, prevNode.node_id) * 0.3;
        const ambiguityPenalty = localPrefixAmbiguityPenalty(c, prevNode, prefix);
        const directionalBoost = clamp(directionalPrior(c), -1, 1) * 0.08;
        const observerHopBoost = clamp(multiObserverPrior(c), -1, 1) * 0.09;
        const confirmedFloor = strongConfirmedFloor(meta);
        const baseConf = confirmedLinkConfidence(meta, c.node_id, prevNode.node_id, {
          prefix: priorBoost + directionalBoost + observerHopBoost,
          transition: transitionBoost,
          motif: motifBoost,
          edge: edgeBoost,
          ambiguity: -ambiguityPenalty,
        });
        return { node: c, conf: Math.max(baseConf, confirmedFloor) };
      });

    const reachable = all
      .filter((c) => {
        if (usedIds.has(c.node_id)) return false;
        if (!inCorridor(c, prevNode, prefix)) return false;
        const meta = context.linkMetrics.get(linkKey(c.node_id, prevNode.node_id));
        const reachOk = canReach(c, prevNode, context.coverageByNode);
        const losOk = hasLoS(c, prevNode);
        return (reachOk && losOk) || (reachOk && isWeakOrBetter(meta)) || (losOk && isWeakOrBetter(meta));
      })
      .sort((a, b) => sortScore(b) - sortScore(a))
      .slice(0, 10)
      .map((c) => {
        usedIds.add(c.node_id);
        const distancePenalty = Math.min(0.12, distKm(c, prevNode) / 120);
        const prior = distanceElevationPrior(c, prevNode);
        const prefixBoost = prefixPrior(prefix, prevPrefix, c.node_id) * 0.22;
        const transitionBoost = transitionPrior(c.node_id, prevNode.node_id) * 0.25;
        const motifBoost = motifPrior([c.node_id, prevNode.node_id]) * 0.18
          + (nextTowardRx ? motifPrior([c.node_id, prevNode.node_id, nextTowardRx]) * 0.2 : 0);
        const edgeBoost = edgePrior(c.node_id, prevNode.node_id) * 0.28;
        const ambiguityPenalty = localPrefixAmbiguityPenalty(c, prevNode, prefix);
        const directionalBoost = clamp(directionalPrior(c), -1, 1) * 0.1;
        const observerHopBoost = clamp(multiObserverPrior(c), -1, 1) * 0.11;
        return {
          node: c,
          conf: Math.max(0.08, 0.2 + prior * 0.34 + prefixBoost + transitionBoost + motifBoost + edgeBoost + directionalBoost + observerHopBoost - distancePenalty - ambiguityPenalty - (all.length - 1) * 0.01),
        };
      });

    const fallback = all
      .filter((c) => {
        if (usedIds.has(c.node_id)) return false;
        if (!inCorridor(c, prevNode, prefix)) return false;
        if (distKm(c, prevNode) >= MAX_HOP_KM * 0.5) return false;
        const meta = context.linkMetrics.get(linkKey(c.node_id, prevNode.node_id));
        const reachOk = canReach(c, prevNode, context.coverageByNode);
        const losOk = hasLoS(c, prevNode);
        return (reachOk && losOk) || (reachOk && isLooseOrBetter(meta)) || isWeakOrBetter(meta);
      })
      .sort((a, b) => sortScore(b) - sortScore(a))
      .slice(0, 6)
      .map((c) => {
        const prior = distanceElevationPrior(c, prevNode);
        const prefixBoost = prefixPrior(prefix, prevPrefix, c.node_id) * 0.16;
        const transitionBoost = transitionPrior(c.node_id, prevNode.node_id) * 0.16;
        const motifBoost = motifPrior([c.node_id, prevNode.node_id]) * 0.12;
        const edgeBoost = edgePrior(c.node_id, prevNode.node_id) * 0.18;
        const ambiguityPenalty = localPrefixAmbiguityPenalty(c, prevNode, prefix);
        const directionalBoost = clamp(directionalPrior(c), -1, 1) * 0.08;
        const observerHopBoost = clamp(multiObserverPrior(c), -1, 1) * 0.1;
        return {
          node: c,
          conf: Math.max(0.03, 0.04 + prior * 0.2 + prefixBoost + transitionBoost + motifBoost + edgeBoost + directionalBoost + observerHopBoost - ambiguityPenalty) / Math.max(1, all.length),
        };
      });

    return [...confirmed, ...reachable, ...fallback];
  }

  const ambiguity = normalizedHashes.reduce((sum, h) => sum + countNodesForPathHash(pathHashIndex, h), 0);
  let budget = Math.max(3_000, Math.min(308_232, 1_000 + normalizedHashes.length * 3_000 + ambiguity * 800));

  function solve(hopIdx: number, prevNode: MeshNode, nextTowardRx: string | null, visited: Set<string>): HopResult[] | null {
    if (hopIdx < 0) return [];
    if (--budget <= 0) return null;

    const prefix = normalizedHashes[hopIdx]!;
    const prevPrefix = hopIdx > 0 ? normalizedHashes[hopIdx - 1]! : '';
    const options = getCandidates(prefix, prevPrefix, prevNode, nextTowardRx).filter((o) => !visited.has(o.node.node_id));

    for (const opt of options) {
      const nextVisited = new Set(visited);
      nextVisited.add(opt.node.node_id);
      const rest = solve(hopIdx - 1, opt.node, prevNode.node_id, nextVisited);
      if (rest !== null) return [opt, ...rest];
    }
    return null;
  }

  const raw = solve(normalizedHashes.length - 1, rx, null, new Set([rx.node_id]));
  if (!raw) return null;

  const hops = [...raw].reverse();
  if (hops.length === 0) return null;

  const totalHops = raw.length;
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
      `SELECT node_id, name, lat, lon, iata, role, elevation_m
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
      itm_path_loss_db: number | null;
      count_a_to_b: number | null;
      count_b_to_a: number | null;
    }>(
      `SELECT nl.node_a_id, nl.node_b_id, nl.observed_count, nl.itm_path_loss_db, nl.count_a_to_b, nl.count_b_to_a
       FROM node_links nl
       JOIN nodes a ON a.node_id = nl.node_a_id
       JOIN nodes b ON b.node_id = nl.node_b_id
       WHERE (nl.itm_viable = true OR nl.force_viable = true)
         AND nl.observed_count >= $2
         AND ($1 = 'all' OR (a.network = $1 AND b.network = $1))`,
      [network, MIN_LINK_OBSERVATIONS],
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
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const row of linkRows.rows) {
    const key = linkKey(row.node_a_id, row.node_b_id);
    linkPairs.add(key);
    linkMetrics.set(key, {
      observed_count: Number(row.observed_count ?? 0),
      itm_path_loss_db: row.itm_path_loss_db == null ? null : Number(row.itm_path_loss_db),
      count_a_to_b: row.count_a_to_b == null ? null : Number(row.count_a_to_b),
      count_b_to_a: row.count_b_to_a == null ? null : Number(row.count_b_to_a),
    });
  }

  const context: BetaResolveContext = {
    loadedAt: now,
    nodesById,
    coverageByNode,
    linkPairs,
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

export async function resolveBetaPathForPacketHash(packetHash: string, network: string, observer?: string): Promise<BetaResolvedPayload | null> {
  const [packetResult, observerHopResult] = await Promise.all([
    query<PathPacket>(
      `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes
       FROM packets
       WHERE packet_hash = $1
         AND ($2 = 'all' OR network = $2)
         ${observer ? 'AND LOWER(rx_node_id) = LOWER($3)' : ''}
       ORDER BY hop_count ASC NULLS LAST, time ASC
       LIMIT 1`,
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

  const packet = packetResult.rows[0];
  if (!packet) return null;
  const context = await loadContext(network);
  const logPrefix = `[path-beta] hash=${packetHash} network=${network}`;

  const rx = packet.rx_node_id ? context.nodesById.get(packet.rx_node_id) : undefined;
  if (!hasCoords(rx)) {
    console.log(`${logPrefix} mode=none reason=missing-rx-coords`);
    return {
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
        hopsRequested: Number((packet.path_hashes ?? []).length),
        hopsUsed: 0,
        rxNodeId: packet.rx_node_id,
        srcNodeId: packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    };
  }

  const src = packet.src_node_id ? (context.nodesById.get(packet.src_node_id) ?? null) : null;
  const hashes = packet.path_hashes ?? [];
  const hops = packet.hop_count != null ? hashes.slice(0, Math.max(0, packet.hop_count)) : hashes;
  const forceIncludeSource = packet.packet_type === 4;
  const currentHopCount = Number(packet.hop_count ?? 0);
  const observerHopHints: ObserverHopHint[] = currentHopCount > 0 && packet.rx_node_id
    ? observerHopResult.rows.flatMap((row) => {
      if (!row.rx_node_id || row.rx_node_id === packet.rx_node_id) return [];
      const hopCount = Number(row.hop_count ?? 0);
      if (hopCount <= 0) return [];
      const observerNode = context.nodesById.get(row.rx_node_id);
      if (!observerNode || !hasCoords(observerNode)) return [];
      return [{ observerNode, hopCount, hopDelta: hopCount - currentHopCount }];
    })
    : [];

  if (hops.length < 1) {
    console.log(`${logPrefix} mode=none reason=no-hops rx=${packet.rx_node_id ?? 'unknown'} src=${packet.src_node_id ?? 'unknown'}`);
    return {
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
        rxNodeId: packet.rx_node_id,
        srcNodeId: packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    };
  }

  let result = resolveBetaPath(hops, hasCoords(src) ? src : null, rx, context, { forceIncludeSource, observerHopHints });
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
    const split = splitResolvedAndAlternatives(result, BETA_PURPLE_THRESHOLD, context.linkMetrics);
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
    if (packet.packet_type === 4 && hasCoords(src) && unresolvedFrontEdges > 0) {
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
        const sourceSplit = splitResolvedFromSource(sourcePartial, BETA_PURPLE_THRESHOLD, context.linkMetrics);
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
      `rx=${packet.rx_node_id ?? 'unknown'} src=${packet.src_node_id ?? 'unknown'}`,
    );
    return {
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
        rxNodeId: packet.rx_node_id,
        srcNodeId: packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    };
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
      `permutations=${permutationCount} remaining=unknown rx=${packet.rx_node_id ?? 'unknown'} src=${packet.src_node_id ?? 'unknown'}`,
    );
    return {
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
        rxNodeId: packet.rx_node_id,
        srcNodeId: packet.src_node_id,
        computedAt: new Date().toISOString(),
      },
    };
  }

  console.log(
    `${logPrefix} mode=none reason=unresolved hops=${hops.length} rx=${packet.rx_node_id ?? 'unknown'} src=${packet.src_node_id ?? 'unknown'}`,
  );
  return {
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
      rxNodeId: packet.rx_node_id,
      srcNodeId: packet.src_node_id,
      computedAt: new Date().toISOString(),
    },
  };
}
