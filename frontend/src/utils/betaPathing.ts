import type { MeshNode } from '../hooks/useNodes.js';
import type { NodeCoverage } from '../hooks/useCoverage.js';
import { hasCoords, linkKey, type LinkMetrics } from './pathing.js';
import { confirmedLinkConfidence, directionalSupport, minimumDirectionalSupport } from './betaLinks.js';
export type { LinkMetrics } from './pathing.js';

const MAX_BETA_HOPS = 25;
const R_EFF_M = 6_371_000 / (1 - 0.25);
const PREFIX_AMBIGUITY_FLOOR_KM = 45;
const WEAK_LINK_PATHLOSS_MAX_DB = 135;

export type PathLearningModel = {
  prefixProbabilities: Map<string, number>;
  transitionProbabilities: Map<string, number>;
  edgeScores: Map<string, number>;
  motifProbabilities: Map<string, number>;
  confidenceScale: number;
  confidenceBias: number;
  recommendedThreshold: number;
  bucketHours: number;
};

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function distKm(a: MeshNode, b: MeshNode): number {
  const midLat = ((a.lat! + b.lat!) / 2) * (Math.PI / 180);
  const dlat = (a.lat! - b.lat!) * 111;
  const dlon = (a.lon! - b.lon!) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

function hasLoS(a: MeshNode, b: MeshNode): boolean {
  const hA = (a.elevation_m ?? 0) + 5;
  const hB = (b.elevation_m ?? 0) + 5;
  const d = distKm(a, b) * 1000;
  if (d < 1) return true;
  for (let i = 1; i < 20; i++) {
    const t = i / 20;
    const x = t * d;
    const los = hA + (hB - hA) * t;
    const bulge = x * (d - x) / (2 * R_EFF_M);
    if (los < bulge) return false;
  }
  return true;
}

function nodeRange(nodeId: string, coverage: NodeCoverage[]): number {
  const cov = coverage.find((c) => c.node_id === nodeId);
  if (!cov?.radius_m) return 50;
  return Math.min(80, Math.max(50, cov.radius_m / 1000));
}

function canReach(a: MeshNode, b: MeshNode, coverage: NodeCoverage[]): boolean {
  const threshold = Math.max(nodeRange(a.node_id, coverage), nodeRange(b.node_id, coverage));
  return distKm(a, b) < threshold;
}

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

function isWeakOrBetter(meta: LinkMetrics | undefined): boolean {
  const pathLoss = meta?.itm_path_loss_db;
  return pathLoss != null && pathLoss <= WEAK_LINK_PATHLOSS_MAX_DB;
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

export function resolveBetaPath(
  pathHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  allNodes: Map<string, MeshNode>,
  coverage: NodeCoverage[],
  linkPairs: Set<string>,
  linkMetrics: Map<string, LinkMetrics>,
  learningModel?: PathLearningModel | null,
  options?: { forceIncludeSource?: boolean },
): { path: [number, number][]; confidence: number; segmentConfidence: number[]; nodeIds: string[] } | null {
  if (!hasCoords(rx) || pathHashes.length === 0) return null;
  if (pathHashes.length >= MAX_BETA_HOPS) return null;
  const rxLat = rx.lat;
  const rxLon = rx.lon;

  type HopResult = { node: MeshNode; conf: number } | null;
  const candidatesPool = Array.from(allNodes.values()).filter(
    (n) => hasCoords(n) && (n.role === undefined || n.role === 2) && !n.name?.includes('🚫'),
  );
  const prefixCounts = new Map<string, number>();
  const prefixBuckets = new Map<string, MeshNode[]>();
  for (const n of candidatesPool) {
    const p = n.node_id.slice(0, 2).toUpperCase();
    prefixCounts.set(p, (prefixCounts.get(p) ?? 0) + 1);
    const existing = prefixBuckets.get(p);
    if (existing) existing.push(n);
    else prefixBuckets.set(p, [n]);
  }

  const totalDist = hasCoords(src) ? distKm(src, rx) : 0;
  const corridorMaxKm = Math.max(10, Math.min(45, totalDist * 0.33));
  const receiverRegion = rx.iata ?? 'unknown';
  const bucketHours = learningModel?.bucketHours ?? 6;
  const hourBucket = currentHourBucket(bucketHours);

  function prefixPrior(prefix: string, prevPrefix: string, nodeId: string): number {
    if (!learningModel) return 0;
    const exactKey = `${receiverRegion}|${prefix}|${prevPrefix}|${nodeId}`;
    const regionOnlyKey = `unknown|${prefix}|${prevPrefix}|${nodeId}`;
    const noPrevKey = `${receiverRegion}|${prefix}||${nodeId}`;
    const noPrevFallbackKey = `unknown|${prefix}||${nodeId}`;
    return learningModel.prefixProbabilities.get(exactKey)
      ?? learningModel.prefixProbabilities.get(regionOnlyKey)
      ?? learningModel.prefixProbabilities.get(noPrevKey)
      ?? learningModel.prefixProbabilities.get(noPrevFallbackKey)
      ?? 0;
  }

  function transitionPrior(fromId: string, toId: string): number {
    if (!learningModel) return 0;
    const key = `${receiverRegion}|${fromId}|${toId}`;
    const fallback = `unknown|${fromId}|${toId}`;
    return learningModel.transitionProbabilities.get(key)
      ?? learningModel.transitionProbabilities.get(fallback)
      ?? 0;
  }

  function edgePrior(fromId: string, toId: string): number {
    if (!learningModel) return 0;
    const exact = edgeKey(receiverRegion, hourBucket, fromId, toId);
    const regionFallback = edgeKey(receiverRegion, -1, fromId, toId);
    const unknownExact = edgeKey('unknown', hourBucket, fromId, toId);
    const unknownFallback = edgeKey('unknown', -1, fromId, toId);
    return learningModel.edgeScores.get(exact)
      ?? learningModel.edgeScores.get(regionFallback)
      ?? learningModel.edgeScores.get(unknownExact)
      ?? learningModel.edgeScores.get(unknownFallback)
      ?? 0;
  }

  function motifPrior(nodeIds: string[]): number {
    if (!learningModel || (nodeIds.length !== 2 && nodeIds.length !== 3)) return 0;
    const exact = motifKey(receiverRegion, hourBucket, nodeIds);
    const regionFallback = motifKey(receiverRegion, -1, nodeIds);
    const unknownExact = motifKey('unknown', hourBucket, nodeIds);
    const unknownFallback = motifKey('unknown', -1, nodeIds);
    return learningModel.motifProbabilities.get(exact)
      ?? learningModel.motifProbabilities.get(regionFallback)
      ?? learningModel.motifProbabilities.get(unknownExact)
      ?? learningModel.motifProbabilities.get(unknownFallback)
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

  const clashAdjacency = buildClashAdjacency(candidatesPool, linkPairs, linkMetrics);
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

  function localPrefixAmbiguityPenalty(candidate: MeshNode, prevNode: MeshNode): number {
    const prefix = candidate.node_id.slice(0, 2).toUpperCase();
    const peers = prefixBuckets.get(prefix) ?? [];
    if (peers.length <= 1) return 0;

    const inRangeKm = Math.max(PREFIX_AMBIGUITY_FLOOR_KM, nodeRange(candidate.node_id, coverage), nodeRange(prevNode.node_id, coverage));
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

      // Beta clash weighting:
      // 1-hop same-prefix neighbors apply full penalty, 2-hop apply half penalty.
      const hopDistance = twoHopDistance(candidate.node_id, peer.node_id);
      if (hopDistance === 1) hopRaw += 1.0 * distanceSimilarity;
      else if (hopDistance === 2) hopRaw += 0.5 * distanceSimilarity;
    }
    const localPenalty = clamp(localRaw * 0.08, 0, 0.14);
    const hopPenalty = clamp(hopRaw * 0.07, 0, 0.16);
    return clamp(localPenalty + hopPenalty, 0, 0.30);
  }

  function clashPressure(candidate: MeshNode): number {
    const prefix = candidate.node_id.slice(0, 2).toUpperCase();
    const peers = prefixBuckets.get(prefix) ?? [];
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

  function inCorridor(candidate: MeshNode, prevNode: MeshNode): boolean {
    if (!hasCoords(src)) return true;

    const bx = src.lon - rxLon;
    const by = src.lat - rxLat;
    const segLen2 = bx * bx + by * by;
    if (segLen2 < 1e-9) return true;

    const px = candidate.lon! - rxLon;
    const py = candidate.lat! - rxLat;
    const t = (px * bx + py * by) / segLen2;
    const pressure = clashPressure(candidate);
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

  function getCandidates(
    prefix: string,
    prevPrefix: string,
    prevNode: MeshNode,
    nextTowardRx: string | null,
  ): Array<{ node: MeshNode; conf: number }> {
    const all = candidatesPool.filter((n) => n.node_id.toUpperCase().startsWith(prefix));
    if (all.length === 0) return [];

    function align(c: MeshNode): number {
      if (!hasCoords(src)) return 0;
      const dLat = src.lat! - prevNode.lat!;
      const dLon = src.lon! - prevNode.lon!;
      const cLat = c.lat! - prevNode.lat!;
      const cLon = c.lon! - prevNode.lon!;
      const dot = dLat * cLat + dLon * cLon;
      const mag = Math.hypot(dLat, dLon) * Math.hypot(cLat, cLon);
      return mag > 0 ? dot / mag : 0;
    }

    function sortScore(c: MeshNode): number {
      const corridorBonus = inCorridor(c, prevNode) ? 0.25 : -0.6;
      return align(c) - distKm(c, prevNode) / 50 + corridorBonus;
    }

    const usedIds = new Set<string>();

    const confirmed = all
      .filter((c) => {
        const key = linkKey(c.node_id, prevNode.node_id);
        if (!linkPairs.has(key)) return false;
        const meta = linkMetrics.get(key);
        if (!meta || meta.count_a_to_b == null || meta.count_b_to_a == null) return true;
        const dir = directionalSupport(meta, c.node_id, prevNode.node_id);
        const observed = meta.observed_count ?? 0;
        if (observed < 20) return true;
        return dir >= minimumDirectionalSupport(observed) * 0.6;
      })
      .sort((a, b) => sortScore(b) - sortScore(a))
      .slice(0, 4)
      .map((c) => {
        usedIds.add(c.node_id);
        const meta = linkMetrics.get(linkKey(c.node_id, prevNode.node_id));
        const priorBoost = prefixPrior(prefix, prevPrefix, c.node_id) * 0.2;
        const transitionBoost = transitionPrior(c.node_id, prevNode.node_id) * 0.24;
        const motifBoost = motifPrior([c.node_id, prevNode.node_id]) * 0.2
          + (nextTowardRx ? motifPrior([c.node_id, prevNode.node_id, nextTowardRx]) * 0.25 : 0);
        const edgeBoost = edgePrior(c.node_id, prevNode.node_id) * 0.3;
        const ambiguityPenalty = localPrefixAmbiguityPenalty(c, prevNode);
        const confirmedFloor = strongConfirmedFloor(meta);
        const baseConf = confirmedLinkConfidence(meta, c.node_id, prevNode.node_id, {
          prefix: priorBoost,
          transition: transitionBoost,
          motif: motifBoost,
          edge: edgeBoost,
          ambiguity: -ambiguityPenalty,
        });
        return {
          node: c,
          conf: Math.max(baseConf, confirmedFloor),
        };
      });

    const reachable = all
      .filter((c) => {
        if (usedIds.has(c.node_id)) return false;
        if (!inCorridor(c, prevNode)) return false;
        const meta = linkMetrics.get(linkKey(c.node_id, prevNode.node_id));
        const reachOk = canReach(c, prevNode, coverage);
        const losOk = hasLoS(c, prevNode);
        // Loosen: accept if either physical model passes, or a weak-or-better learned link exists.
        return (reachOk && losOk) || (reachOk && isWeakOrBetter(meta)) || (losOk && isWeakOrBetter(meta));
      })
      .sort((a, b) => sortScore(b) - sortScore(a))
      .slice(0, 3)
      .map((c) => {
        usedIds.add(c.node_id);
        const distancePenalty = Math.min(0.12, distKm(c, prevNode) / 120);
        const prior = distanceElevationPrior(c, prevNode);
        const prefixBoost = prefixPrior(prefix, prevPrefix, c.node_id) * 0.22;
        const transitionBoost = transitionPrior(c.node_id, prevNode.node_id) * 0.25;
        const motifBoost = motifPrior([c.node_id, prevNode.node_id]) * 0.18
          + (nextTowardRx ? motifPrior([c.node_id, prevNode.node_id, nextTowardRx]) * 0.2 : 0);
        const edgeBoost = edgePrior(c.node_id, prevNode.node_id) * 0.28;
        const ambiguityPenalty = localPrefixAmbiguityPenalty(c, prevNode);
        return {
          node: c,
          conf: Math.max(0.08, 0.2 + prior * 0.34 + prefixBoost + transitionBoost + motifBoost + edgeBoost - distancePenalty - ambiguityPenalty - (all.length - 1) * 0.01),
        };
      });

    const fallback = all
      .filter((c) => {
        if (usedIds.has(c.node_id)) return false;
        if (!inCorridor(c, prevNode)) return false;
        if (distKm(c, prevNode) >= 65) return false;
        const meta = linkMetrics.get(linkKey(c.node_id, prevNode.node_id));
        return hasLoS(c, prevNode) || isWeakOrBetter(meta);
      })
      .sort((a, b) => sortScore(b) - sortScore(a))
      .slice(0, 1)
      .map((c) => {
        const prior = distanceElevationPrior(c, prevNode);
        const prefixBoost = prefixPrior(prefix, prevPrefix, c.node_id) * 0.16;
        const transitionBoost = transitionPrior(c.node_id, prevNode.node_id) * 0.16;
        const motifBoost = motifPrior([c.node_id, prevNode.node_id]) * 0.12;
        const edgeBoost = edgePrior(c.node_id, prevNode.node_id) * 0.18;
        const ambiguityPenalty = localPrefixAmbiguityPenalty(c, prevNode);
        return {
          node: c,
          conf: Math.max(0.03, 0.04 + prior * 0.2 + prefixBoost + transitionBoost + motifBoost + edgeBoost - ambiguityPenalty) / Math.max(1, all.length),
        };
      });

    return [...confirmed, ...reachable, ...fallback];
  }

  const maxSkips = Math.min(3, Math.floor(pathHashes.length / 2));
  const ambiguity = pathHashes.reduce(
    (sum, h) => sum + (prefixCounts.get(h.slice(0, 2).toUpperCase()) ?? 0),
    0,
  );
  let budget = Math.max(600, Math.min(12_000, 260 + pathHashes.length * 140 + ambiguity * 42));

  function solve(
    hopIdx: number,
    prevNode: MeshNode,
    nextTowardRx: string | null,
    skipsLeft: number,
    visited: Set<string>,
  ): HopResult[] | null {
    if (hopIdx < 0) return [];
    if (--budget <= 0) return null;

    const prefix = pathHashes[hopIdx]!.slice(0, 2).toUpperCase();
    const prevPrefix = hopIdx > 0 ? pathHashes[hopIdx - 1]!.slice(0, 2).toUpperCase() : '';
    const options = getCandidates(prefix, prevPrefix, prevNode, nextTowardRx).filter((o) => !visited.has(o.node.node_id));

    for (const opt of options) {
      const nextVisited = new Set(visited);
      nextVisited.add(opt.node.node_id);
      const rest = solve(hopIdx - 1, opt.node, prevNode.node_id, skipsLeft, nextVisited);
      if (rest !== null) return [opt, ...rest];
    }

    if (skipsLeft > 0) {
      const rest = solve(hopIdx - 1, prevNode, nextTowardRx, skipsLeft - 1, visited);
      if (rest !== null) return [null, ...rest];
    }

    return null;
  }

  const raw = solve(pathHashes.length - 1, rx, null, maxSkips, new Set([rx.node_id]));
  if (!raw) return null;

  const hops = [...raw].reverse().filter((r): r is { node: MeshNode; conf: number } => r !== null);
  if (hops.length === 0) return null;

  const totalHops = raw.length;
  const skipped = totalHops - hops.length;
  const meanHopConfidence = hops.reduce((sum, h) => sum + h.conf, 0) / hops.length;
  const resolvedRatio = hops.length / totalHops;
  const skipPenalty = Math.max(0.2, 1 - skipped * 0.28);
  const rawConfidence = meanHopConfidence * resolvedRatio * skipPenalty;
  const calibratedConfidence = rawConfidence * (learningModel?.confidenceScale ?? 1) + (learningModel?.confidenceBias ?? 0);
  const confidence = clamp(calibratedConfidence, 0, 1);

  const srcPrefix = hasCoords(src) ? src.node_id.slice(0, 2).toUpperCase() : null;
  const firstHopPrefix = pathHashes[0]?.slice(0, 2).toUpperCase() ?? null;
  const prependSource = Boolean(options?.forceIncludeSource
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
    // Use direct per-edge confidence instead of min-adjacent blending.
    // This prevents low confidence later in the chain from turning early,
    // high-confidence segments red.
    if (hasSource && i === 0) {
      segmentConfidence.push(hops[0]?.conf ?? confidence); // src -> first hop
      continue;
    }
    const hopIdx = hasSource ? i : i;
    segmentConfidence.push(hops[hopIdx]?.conf ?? hops[hops.length - 1]?.conf ?? confidence);
  }

  return { path: pathNodes.map((n) => [n.lat!, n.lon!]), confidence, segmentConfidence, nodeIds: pathNodes.map((n) => n.node_id) };
}

export function enumerateBetaCompletions(
  startNodeId: string,
  endNodeId: string,
  stepsRemaining: number,
  allNodes: Map<string, MeshNode>,
  linkPairs: Set<string>,
  linkMetrics: Map<string, LinkMetrics>,
  maxPaths = 24,
): [number, number][][] {
  if (stepsRemaining <= 0 || startNodeId === endNodeId || maxPaths <= 0) return [];
  const candidates = Array.from(allNodes.values()).filter(
    (n) => hasCoords(n) && (n.role === undefined || n.role === 2) && !n.name?.includes('🚫'),
  );
  const byId = new Map<string, MeshNode>();
  for (const n of candidates) byId.set(n.node_id, n);
  if (!byId.has(startNodeId) || !byId.has(endNodeId)) return [];

  const adjacency = buildClashAdjacency(candidates, linkPairs, linkMetrics);
  const endNode = byId.get(endNodeId)!;
  const results: string[][] = [];
  let expansions = 0;
  const maxExpansions = 8_000;

  const dfs = (currentId: string, depth: number, path: string[], visited: Set<string>) => {
    if (results.length >= maxPaths || expansions >= maxExpansions) return;
    expansions += 1;
    if (depth === stepsRemaining) {
      if (currentId === endNodeId) results.push([...path]);
      return;
    }
    const neighbors = Array.from(adjacency.get(currentId) ?? [])
      .filter((id) => !visited.has(id))
      .sort((a, b) => {
        const an = byId.get(a);
        const bn = byId.get(b);
        if (!an || !bn) return 0;
        return distKm(an, endNode) - distKm(bn, endNode);
      });
    for (const nextId of neighbors) {
      if (!byId.has(nextId)) continue;
      visited.add(nextId);
      path.push(nextId);
      dfs(nextId, depth + 1, path, visited);
      path.pop();
      visited.delete(nextId);
      if (results.length >= maxPaths || expansions >= maxExpansions) return;
    }
  };

  const startVisited = new Set<string>([startNodeId]);
  dfs(startNodeId, 0, [startNodeId], startVisited);

  return results
    .map((ids) => ids.map((id) => byId.get(id)!).filter((n): n is MeshNode => Boolean(n)))
    .filter((nodes) => nodes.length === stepsRemaining + 1)
    .map((nodes) => nodes.map((n) => [n.lat!, n.lon!]));
}

export function buildNearestPrefixContinuation(
  startNodeId: string,
  remainingPrefixes: string[],
  endNodeId: string,
  allNodes: Map<string, MeshNode>,
  options?: { dropStartIfNodeId?: string },
): [number, number][] | null {
  const candidates = Array.from(allNodes.values()).filter(
    (n) => hasCoords(n) && (n.role === undefined || n.role === 2) && !n.name?.includes('🚫'),
  );
  const byId = new Map<string, MeshNode>();
  for (const n of candidates) byId.set(n.node_id, n);

  const start = byId.get(startNodeId);
  const end = byId.get(endNodeId);
  if (!start || !end) return null;

  const pathNodes: MeshNode[] = [start];
  const visited = new Set<string>([start.node_id]);
  let current = start;

  for (let idx = 0; idx < remainingPrefixes.length; idx++) {
    const rawPrefix = remainingPrefixes[idx]!;
    const isLastPrefix = idx === remainingPrefixes.length - 1;
    const prefix = rawPrefix.slice(0, 2).toUpperCase();
    const matches = candidates
      .filter((n) => {
        if (visited.has(n.node_id)) return false;
        if (n.node_id.slice(0, 2).toUpperCase() !== prefix) return false;
        // Hard rule: do not consume RX as an intermediate node.
        if (n.node_id === end.node_id && !isLastPrefix) return false;
        return true;
      })
      .sort((a, b) => distKm(a, current) - distKm(b, current));
    const chosen = matches[0];
    if (!chosen) continue;
    pathNodes.push(chosen);
    visited.add(chosen.node_id);
    current = chosen;
  }

  if (pathNodes[pathNodes.length - 1]?.node_id !== end.node_id) {
    // Hard rule: never repeat a node in the same rendered path.
    if (visited.has(end.node_id)) return null;
    pathNodes.push(end);
    visited.add(end.node_id);
  }
  if (new Set(pathNodes.map((n) => n.node_id)).size !== pathNodes.length) return null;
  const renderNodes = (options?.dropStartIfNodeId && pathNodes[0]?.node_id === options.dropStartIfNodeId)
    ? pathNodes.slice(1)
    : pathNodes;
  if (renderNodes.length < 2) return null;
  return renderNodes.map((n) => [n.lat!, n.lon!]);
}

export function enumeratePrefixContinuations(
  startNodeId: string,
  remainingPrefixes: string[],
  endNodeId: string,
  allNodes: Map<string, MeshNode>,
  options?: { dropStartIfNodeId?: string; maxRenderPaths?: number; maxSearchStates?: number },
): { paths: [number, number][][]; totalCount: number; truncated: boolean } {
  const maxRenderPaths = Math.max(1, options?.maxRenderPaths ?? 320);
  const maxSearchStates = Math.max(1000, options?.maxSearchStates ?? 120_000);

  const candidates = Array.from(allNodes.values()).filter(
    (n) => hasCoords(n) && (n.role === undefined || n.role === 2) && !n.name?.includes('🚫'),
  );
  const byId = new Map<string, MeshNode>();
  const byPrefix = new Map<string, MeshNode[]>();
  for (const n of candidates) {
    byId.set(n.node_id, n);
    const p = n.node_id.slice(0, 2).toUpperCase();
    const arr = byPrefix.get(p);
    if (arr) arr.push(n);
    else byPrefix.set(p, [n]);
  }

  const start = byId.get(startNodeId);
  const end = byId.get(endNodeId);
  if (!start || !end) return { paths: [], totalCount: 0, truncated: false };

  const discovered: string[][] = [];
  let totalCount = 0;
  let states = 0;
  let truncated = false;

  const dfs = (idx: number, current: MeshNode, path: string[], visited: Set<string>) => {
    if (states++ >= maxSearchStates) {
      truncated = true;
      return;
    }
    if (idx >= remainingPrefixes.length) {
      if (current.node_id !== end.node_id) {
        if (visited.has(end.node_id)) return;
        path.push(end.node_id);
      }
      totalCount += 1;
      if (discovered.length < maxRenderPaths) discovered.push([...path]);
      if (current.node_id !== end.node_id) path.pop();
      return;
    }

    const prefix = remainingPrefixes[idx]!.slice(0, 2).toUpperCase();
    const nodesForPrefix = (byPrefix.get(prefix) ?? [])
      .filter((n) => !visited.has(n.node_id) && n.node_id !== end.node_id)
      .sort((a, b) => distKm(a, current) - distKm(b, current));
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

  const paths = discovered
    .map((ids) => {
      const renderIds = (options?.dropStartIfNodeId && ids[0] === options.dropStartIfNodeId) ? ids.slice(1) : ids;
      const nodes = renderIds.map((id) => byId.get(id)).filter((n): n is MeshNode => Boolean(n));
      if (new Set(nodes.map((n) => n.node_id)).size !== nodes.length) return null;
      if (nodes.length < 2) return null;
      return nodes.map((n) => [n.lat!, n.lon!]) as [number, number][];
    })
    .filter((p): p is [number, number][] => Array.isArray(p));

  return { paths, totalCount, truncated };
}
