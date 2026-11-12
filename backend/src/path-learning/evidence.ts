import { undirectedLinkKey as linkKey, positionPrefixKey, corridorKey, positionTransitionKey } from '../path-shared/priorKeys.js';
import { buildNodePathHashIndex, getNodesForPathHash, nodePathHash, normalizePathHash } from '../path-hash/utils.js';
import {
  MAX_PREFIX_CHOICES_PER_GROUP,
  MAX_TRANSITIONS_PER_GROUP,
  MAX_EDGE_CHOICES_PER_GROUP,
  MAX_MOTIF2_CHOICES_PER_GROUP,
  MAX_MOTIF3_CHOICES_PER_GROUP,
  MAX_POSITION_PREFIX_CHOICES_PER_GROUP,
  MAX_CORRIDOR_CHOICES_PER_GROUP,
  MAX_POSITION_TRANSITIONS_PER_GROUP,
  HOUR_BUCKET_SIZE,
  PREFIX_AMBIGUITY_RADIUS_KM,
} from './parameters.js';
import type {
  LearningPacket,
  LearningNode,
  LearningLink,
  ResolvedHop,
  LearningPathNode,
  PrefixPriorWriteRow,
  TransitionPriorWriteRow,
  PositionPrefixPriorWriteRow,
  CorridorPriorWriteRow,
  PositionTransitionPriorWriteRow,
  EdgePriorWriteRow,
  MotifPriorWriteRow,
  PathLearningCalibration,
  LearningDatasets,
} from './model.js';

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function distKm(a: LearningNode, b: LearningNode): number {
  const midLat = ((a.lat + b.lat) / 2) * (Math.PI / 180);
  const dLat = (a.lat - b.lat) * 111;
  const dLon = (a.lon - b.lon) * 111 * Math.cos(midLat);
  return Math.hypot(dLat, dLon);
}

function distancePrior(a: LearningNode, b: LearningNode): number {
  const d = distKm(a, b);
  const distScore = Math.exp(-d / 22);
  const elevA = a.elevation_m ?? 0;
  const elevB = b.elevation_m ?? 0;
  const elevScore = Math.min(1, Math.max(0, (Math.min(elevA, elevB) + 60) / 320));
  return 0.65 * distScore + 0.35 * elevScore;
}

function hourBucket(ts: Date): number {
  return Math.floor(ts.getUTCHours() / HOUR_BUCKET_SIZE);
}

function recencyScore(lastSeenMs: number | undefined, nowMs: number): number {
  if (!lastSeenMs) return 0.05;
  const ageDays = Math.max(0, (nowMs - lastSeenMs) / 86_400_000);
  return clamp(Math.exp(-ageDays / 21), 0.05, 1);
}

function localPrefixAmbiguityPenalty(
  anchor: LearningNode,
  target: LearningNode,
  pathHash: string,
  pathHashIndex: Map<string, LearningNode[]>,
): number {
  const samePrefixNodes = getNodesForPathHash(pathHashIndex, pathHash);
  if (samePrefixNodes.length <= 1) return 0;

  const targetDist = distKm(anchor, target);
  let raw = 0;
  for (const candidate of samePrefixNodes) {
    if (candidate.node_id === target.node_id) continue;
    const candDist = distKm(anchor, candidate);
    if (candDist > PREFIX_AMBIGUITY_RADIUS_KM) continue;
    const distanceSimilarity = clamp(1 - Math.abs(candDist - targetDist) / PREFIX_AMBIGUITY_RADIUS_KM, 0, 1);
    const proximity = clamp(1 - candDist / PREFIX_AMBIGUITY_RADIUS_KM, 0, 1);
    raw += distanceSimilarity * proximity;
  }

  // Keep this bounded so we only nudge confidence down in ambiguous local clusters.
  return clamp(raw * 0.12, 0, 0.24);
}

export function resolvePathForPacket(
  pathHashes: string[],
  srcNode: LearningNode | undefined,
  rxNode: LearningNode,
  pathHashIndex: Map<string, LearningNode[]>,
  confirmedLinks: Set<string>,
): ResolvedHop[] {
  const resolved: ResolvedHop[] = [];
  const visited = new Set<string>([rxNode.node_id]);
  let prev = rxNode;

  for (let i = pathHashes.length - 1; i >= 0; i--) {
    const prefix = normalizePathHash(pathHashes[i]);
    const allCandidates = getNodesForPathHash(pathHashIndex, prefix);
    const candidates = allCandidates.filter((n) => allCandidates.length === 1 || !visited.has(n.node_id));
    if (candidates.length === 0) continue;

    let best: LearningNode | null = null;
    let bestScore = Number.NEGATIVE_INFINITY;

    for (const candidate of candidates) {
      const confirmed = confirmedLinks.has(linkKey(candidate.node_id, prev.node_id)) ? 1 : 0;
      const distanceScore = distancePrior(candidate, prev);
      const srcScore = srcNode ? (distKm(srcNode, prev) - distKm(srcNode, candidate)) / 100 : 0;
      const ambiguityPenalty = localPrefixAmbiguityPenalty(prev, candidate, prefix, pathHashIndex);
      const score = confirmed * 2.5 + distanceScore * 1.3 + srcScore - ambiguityPenalty;
      if (score > bestScore) {
        best = candidate;
        bestScore = score;
      }
    }

    if (best) {
      resolved.unshift({ position: i, prefix, node: best, isUnique: allCandidates.length === 1 });
      visited.add(best.node_id);
      prev = best;
    }
  }

  return resolved;
}

function increment(map: Map<string, number>, key: string): void {
  map.set(key, (map.get(key) ?? 0) + 1);
}

function truncateBest(
  entries: Array<{ key: string; count: number }>,
  max: number,
): Array<{ key: string; count: number }> {
  return entries
    .sort((a, b) => b.count - a.count)
    .slice(0, max);
}

function directionalSupport(link: LearningLink, fromId: string, toId: string): number {
  let forward = 0;
  let reverse = 0;
  if (fromId === link.node_a_id && toId === link.node_b_id) {
    forward = link.count_a_to_b;
    reverse = link.count_b_to_a;
  } else if (fromId === link.node_b_id && toId === link.node_a_id) {
    forward = link.count_b_to_a;
    reverse = link.count_a_to_b;
  } else if (fromId === link.node_a_id) {
    forward = link.count_a_to_b;
    reverse = link.count_b_to_a;
  } else if (fromId === link.node_b_id) {
    forward = link.count_b_to_a;
    reverse = link.count_a_to_b;
  } else {
    return 0.5;
  }
  const total = forward + reverse;
  if (total <= 0) return 0.5;
  return forward / total;
}

export function isPrivatePathPrefix(
  value: string | null | undefined,
  privateNodeIds: Iterable<string>,
): boolean {
  const prefix = String(value ?? '').trim().toUpperCase();
  if (prefix.length === 0) return false;
  for (const nodeId of privateNodeIds) {
    if (String(nodeId).trim().toUpperCase().startsWith(prefix)) return true;
  }
  return false;
}

/** Publication-side twin of the public path-read filter: a prior row whose own
 * prefix or carried previous-hop prefix resolves onto a private node is dropped
 * before it can reach `analytics.path_prefix_priors`. */
export function dropPrivatePrefixRows<T extends { prefix: string; prev_prefix: string }>(
  rows: readonly T[],
  privateNodeIds: Iterable<string>,
): T[] {
  const ids = Array.from(privateNodeIds);
  return rows.filter((row) => (
    !isPrivatePathPrefix(row.prefix, ids) && !isPrivatePathPrefix(row.prev_prefix, ids)
  ));
}

export type LearningEvidenceInput = {
  nodes: LearningNode[];
  links: LearningLink[];
  packets: LearningPacket[];
  privateNodeIds: Iterable<string>;
  nowMs: number;
  maxAggregateKeys: number;
  assertActive: () => void;
};

export function aggregateLearningEvidence(input: LearningEvidenceInput): { datasets: LearningDatasets; calibration: PathLearningCalibration } {
  const nowMs = input.nowMs;
  const pathHashIndex = buildNodePathHashIndex(input.nodes);
  const nodesById = new Map(input.nodes.map(node => [node.node_id, node]));
  const confirmedLinks = new Set(input.links.map((r) => linkKey(r.node_a_id, r.node_b_id)));
  const linkMetaByPair = new Map(input.links.map((r) => [linkKey(r.node_a_id, r.node_b_id), r]));
  const adjacency = new Map<string, Set<string>>();
  for (const link of input.links) {
    if (!adjacency.has(link.node_a_id)) adjacency.set(link.node_a_id, new Set());
    if (!adjacency.has(link.node_b_id)) adjacency.set(link.node_b_id, new Set());
    adjacency.get(link.node_a_id)!.add(link.node_b_id);
    adjacency.get(link.node_b_id)!.add(link.node_a_id);
  }

  const prefixChoiceCounts = new Map<string, number>();
  const prefixGroupTotals = new Map<string, number>();
  const positionPrefixChoiceCounts = new Map<string, number>();
  const positionPrefixGroupTotals = new Map<string, number>();
  const corridorChoiceCounts = new Map<string, number>();
  const corridorGroupTotals = new Map<string, number>();
  const transitionCounts = new Map<string, number>();
  const transitionGroupTotals = new Map<string, number>();
  const positionTransitionCounts = new Map<string, number>();
  const positionTransitionGroupTotals = new Map<string, number>();

  const edgeObservedCounts = new Map<string, number>();
  const edgeLastSeenMs = new Map<string, number>();
  const activeFromCounts = new Map<string, number>();
  const motif2Counts = new Map<string, number>();
  const motif2GroupTotals = new Map<string, number>();
  const motif3Counts = new Map<string, number>();
  const motif3GroupTotals = new Map<string, number>();

  let evaluatedPackets = 0;
  let successPackets = 0;
  let confidenceSum = 0;

  for (let packetIndex = 0; packetIndex < input.packets.length; packetIndex += 1) {
    if (packetIndex % 256 === 0) {
      input.assertActive();
    }
    const packet = input.packets[packetIndex]!;
    let hashes = packet.path_hashes?.map(normalizePathHash).filter(Boolean) ?? [];
    if (hashes.length === 0) continue;
    const rx = nodesById.get(packet.rx_node_id);
    if (!rx) continue;
    if (packet.direction !== 'rx' && hashes.length > 0 && rx.node_id.toUpperCase().startsWith(hashes[hashes.length - 1]!)) {
      hashes = hashes.slice(0, -1);
    }
    if (hashes.length === 0) continue;

    const src = packet.src_node_id ? nodesById.get(packet.src_node_id) : undefined;
    const region = rx.iata ?? 'unknown';
    const resolved = resolvePathForPacket(hashes, src, rx, pathHashIndex, confirmedLinks);
    if (resolved.length === 0) continue;
    const championRoute = resolved.length === hashes.length && resolved.every((hop) => hop.isUnique);

    const ts = new Date(packet.time);
    const bucket = hourBucket(ts);

    const resolvedByPosition = new Map(resolved.map((hop) => [hop.position, hop]));
    const fullNodes: (LearningPathNode | null)[] = [
      ...(src ? [{ node: src, isVerified: true }] : []),
      ...hashes.map((_, position) => {
        const hop = resolvedByPosition.get(position);
        return hop ? { node: hop.node, isVerified: hop.isUnique } : null;
      }),
      { node: rx, isVerified: true },
    ];
    let successfulEdges = 0;
    let totalEdges = 0;
    for (let i = 0; i < fullNodes.length - 1; i++) {
      const from = fullNodes[i]!;
      const to = fullNodes[i + 1]!;
      if (!from?.isVerified || !to?.isVerified) continue;
      totalEdges++;
      if (confirmedLinks.has(linkKey(from.node.node_id, to.node.node_id))) successfulEdges++;
    }
    if (totalEdges > 0) {
      const packetConfidence = successfulEdges / totalEdges;
      evaluatedPackets++;
      confidenceSum += packetConfidence;
      if (packetConfidence >= 0.6) successPackets++;
    }

    for (let i = 0; i < resolved.length; i++) {
      const hop = resolved[i]!;
      if (!hop.isUnique) continue;
      const prevPrefix = hop.position > 0 ? hashes[hop.position - 1]! : '';
      const prefixGroup = `${hop.prefix}|${region}|${prevPrefix}`;
      const choiceKey = `${prefixGroup}|${hop.node.node_id}`;
      increment(prefixChoiceCounts, choiceKey);
      increment(prefixGroupTotals, prefixGroup);
    }

    if (championRoute) {
      for (let i = 0; i < resolved.length; i++) {
        const hop = resolved[i]!;
        const prefix = hop.prefix.slice(0, 2).toUpperCase();
        const positionPrefixGroup = `${prefix}|${i}`;
        increment(positionPrefixChoiceCounts, positionPrefixKey(prefix, i, hop.node.node_id));
        increment(positionPrefixGroupTotals, positionPrefixGroup);

        if (src) {
          const corridorGroup = `${src.node_id}|${rx.node_id}|${i}`;
          increment(corridorChoiceCounts, corridorKey(src.node_id, rx.node_id, i, hop.node.node_id));
          increment(corridorGroupTotals, corridorGroup);
        }
      }

      if (src) {
        const championNodes = [src, ...resolved.map((hop) => hop.node), rx];
        for (let edgePosition = 0; edgePosition < championNodes.length - 1; edgePosition++) {
          const from = championNodes[edgePosition]!;
          const to = championNodes[edgePosition + 1]!;
          const group = `${edgePosition}|${from.node_id}`;
          increment(positionTransitionCounts, positionTransitionKey(edgePosition, from.node_id, to.node_id));
          increment(positionTransitionGroupTotals, group);
        }
      }
    }

    for (let i = 0; i < fullNodes.length - 1; i++) {
      const from = fullNodes[i]!;
      const to = fullNodes[i + 1]!;
      if (!from?.isVerified || !to?.isVerified) continue;
      const group = `${from.node.node_id}|${region}`;
      const edgeKey = `${group}|${to.node.node_id}`;
      increment(transitionCounts, edgeKey);
      increment(transitionGroupTotals, group);

      const fromGroup = `${region}|${bucket}|${from.node.node_id}`;
      const directedEdgeKey = `${fromGroup}|${to.node.node_id}`;
      increment(activeFromCounts, fromGroup);
      increment(edgeObservedCounts, directedEdgeKey);
      edgeLastSeenMs.set(directedEdgeKey, ts.getTime());

      const motif2Group = `${region}|${bucket}|${from.node.node_id}`;
      const motif2Key = `${motif2Group}|${from.node.node_id}>${to.node.node_id}`;
      increment(motif2Counts, motif2Key);
      increment(motif2GroupTotals, motif2Group);

      if (i < fullNodes.length - 2) {
        const next = fullNodes[i + 2]!;
        if (!next?.isVerified) continue;
        const motif3Group = `${region}|${bucket}|${from.node.node_id}>${to.node.node_id}`;
        const motif3Key = `${motif3Group}|${from.node.node_id}>${to.node.node_id}>${next.node.node_id}`;
        increment(motif3Counts, motif3Key);
        increment(motif3GroupTotals, motif3Group);
      }
    }
    if (
      prefixChoiceCounts.size + prefixGroupTotals.size
      + positionPrefixChoiceCounts.size + positionPrefixGroupTotals.size
      + corridorChoiceCounts.size + corridorGroupTotals.size
      + transitionCounts.size + transitionGroupTotals.size
      + positionTransitionCounts.size + positionTransitionGroupTotals.size
      + edgeObservedCounts.size + edgeLastSeenMs.size
      + activeFromCounts.size + motif2Counts.size + motif2GroupTotals.size
      + motif3Counts.size + motif3GroupTotals.size > input.maxAggregateKeys
    ) {
      throw new Error('PATH_LEARNING_AGGREGATE_LIMIT');
    }
  }

  input.assertActive();

  const groupedPrefix = new Map<string, Array<{ nodeId: string; count: number }>>();
  for (const [key, count] of prefixChoiceCounts) {
    const [prefix, region, prevPrefix, nodeId] = key.split('|');
    const groupKey = `${prefix}|${region}|${prevPrefix}`;
    const row = groupedPrefix.get(groupKey) ?? [];
    row.push({ nodeId: nodeId!, count });
    groupedPrefix.set(groupKey, row);
  }

  const prefixRows: PrefixPriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedPrefix) {
    const [prefix, region, prevPrefix] = groupKey.split('|');
    const total = prefixGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.nodeId, count: r.count })), MAX_PREFIX_CHOICES_PER_GROUP)) {
      prefixRows.push({
        prefix: prefix!,
        receiver_region: region!,
        prev_prefix: prevPrefix || '',
        node_id: row.key,
        count: row.count,
        probability: row.count / total,
      });
    }
  }
  // `prev_prefix` is a raw preceding path hash carried straight from the packet;
  // drop every prior that would republish a private node's prefix.
  const publishablePrefixRows = dropPrivatePrefixRows(prefixRows, input.privateNodeIds);

  const groupedPositionPrefixes = new Map<string, Array<{ nodeId: string; count: number }>>();
  for (const [key, count] of positionPrefixChoiceCounts) {
    const [prefix, position, nodeId] = key.split('|');
    const groupKey = `${prefix}|${position}`;
    const rows = groupedPositionPrefixes.get(groupKey) ?? [];
    rows.push({ nodeId: nodeId!, count });
    groupedPositionPrefixes.set(groupKey, rows);
  }
  const positionPrefixRows: PositionPrefixPriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedPositionPrefixes) {
    const [prefix, positionText] = groupKey.split('|');
    const total = positionPrefixGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(
      rows.map((candidate) => ({ key: candidate.nodeId, count: candidate.count })),
      MAX_POSITION_PREFIX_CHOICES_PER_GROUP,
    )) {
      positionPrefixRows.push({
        prefix: prefix!,
        position: Number(positionText),
        node_id: row.key,
        count: row.count,
        probability: row.count / total,
      });
    }
  }

  const groupedCorridors = new Map<string, Array<{ nodeId: string; count: number }>>();
  for (const [key, count] of corridorChoiceCounts) {
    const [srcNodeId, rxNodeId, position, nodeId] = key.split('|');
    const groupKey = `${srcNodeId}|${rxNodeId}|${position}`;
    const rows = groupedCorridors.get(groupKey) ?? [];
    rows.push({ nodeId: nodeId!, count });
    groupedCorridors.set(groupKey, rows);
  }
  const corridorRows: CorridorPriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedCorridors) {
    const [srcNodeId, rxNodeId, positionText] = groupKey.split('|');
    const total = corridorGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(
      rows.map((candidate) => ({ key: candidate.nodeId, count: candidate.count })),
      MAX_CORRIDOR_CHOICES_PER_GROUP,
    )) {
      corridorRows.push({
        src_node_id: srcNodeId!,
        rx_node_id: rxNodeId!,
        position: Number(positionText),
        node_id: row.key,
        count: row.count,
        probability: row.count / total,
      });
    }
  }

  const groupedTransitions = new Map<string, Array<{ toNodeId: string; count: number }>>();
  for (const [key, count] of transitionCounts) {
    const [fromNodeId, region, toNodeId] = key.split('|');
    const groupKey = `${fromNodeId}|${region}`;
    const row = groupedTransitions.get(groupKey) ?? [];
    row.push({ toNodeId: toNodeId!, count });
    groupedTransitions.set(groupKey, row);
  }

  const transitionRows: TransitionPriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedTransitions) {
    const [fromNodeId, region] = groupKey.split('|');
    const total = transitionGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.toNodeId, count: r.count })), MAX_TRANSITIONS_PER_GROUP)) {
      transitionRows.push({
        from_node_id: fromNodeId!,
        to_node_id: row.key,
        receiver_region: region!,
        count: row.count,
        probability: row.count / total,
      });
    }
  }

  const groupedPositionTransitions = new Map<string, Array<{ toNodeId: string; count: number }>>();
  for (const [key, count] of positionTransitionCounts) {
    const [position, fromNodeId, toNodeId] = key.split('|');
    const groupKey = `${position}|${fromNodeId}`;
    const rows = groupedPositionTransitions.get(groupKey) ?? [];
    rows.push({ toNodeId: toNodeId!, count });
    groupedPositionTransitions.set(groupKey, rows);
  }
  const positionTransitionRows: PositionTransitionPriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedPositionTransitions) {
    const [positionText, fromNodeId] = groupKey.split('|');
    const total = positionTransitionGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(
      rows.map((candidate) => ({ key: candidate.toNodeId, count: candidate.count })),
      MAX_POSITION_TRANSITIONS_PER_GROUP,
    )) {
      positionTransitionRows.push({
        position: Number(positionText),
        from_node_id: fromNodeId!,
        to_node_id: row.key,
        count: row.count,
        probability: row.count / total,
      });
    }
  }

  const groupedEdges = new Map<string, Array<{ toNodeId: string; score: number; observed: number; expected: number; missing: number; directional: number; recency: number; reliability: number; pathLoss: number | null; consistencyPenalty: number }>>();

  for (const [fromGroup, activeCount] of activeFromCounts) {
    const [region, bucketText, fromNodeId] = fromGroup.split('|');
    const bucket = Number(bucketText);
    const neighbors = Array.from(adjacency.get(fromNodeId!) ?? []);
    if (neighbors.length === 0) continue;
    const degree = Math.max(1, neighbors.length);
    const uniformExpected = Math.max(1, Math.round(activeCount / degree));
    const rows: Array<{ toNodeId: string; score: number; observed: number; expected: number; missing: number; directional: number; recency: number; reliability: number; pathLoss: number | null; consistencyPenalty: number }> = [];

    for (const toNodeId of neighbors) {
      const directedEdgeKey = `${fromGroup}|${toNodeId}`;
      const observed = edgeObservedCounts.get(directedEdgeKey) ?? 0;
      const expected = Math.max(observed, uniformExpected);
      const missing = Math.max(0, expected - observed);

      const linkMeta = linkMetaByPair.get(linkKey(fromNodeId!, toNodeId));
      const directional = linkMeta ? directionalSupport(linkMeta, fromNodeId!, toNodeId) : 0.5;
      const recency = recencyScore(edgeLastSeenMs.get(directedEdgeKey), nowMs);
      const reliability = observed / (expected + 2);
      const pathLoss = linkMeta?.itm_path_loss_db ?? null;
      const pathLossScore = pathLoss == null ? 0.55 : clamp((160 - pathLoss) / 45, 0, 1);
      const missPenalty = expected > 0 ? (missing / expected) * 0.3 : 0;

      let consistencyPenalty = 0;
      const fromNode = nodesById.get(fromNodeId!);
      const toNode = nodesById.get(toNodeId);
      if (fromNode && toNode) {
        if (pathLoss != null) {
          const dKm = distKm(fromNode, toNode);
          if (dKm > 55 && pathLoss > 150) consistencyPenalty += 0.14;
        }
        consistencyPenalty += localPrefixAmbiguityPenalty(fromNode, toNode, nodePathHash(toNode.node_id, 2), pathHashIndex);
      }
      if (observed < 3 && missing >= 3) consistencyPenalty += 0.1;
      if (directional < 0.06 && observed >= 5) consistencyPenalty += 0.06;
      consistencyPenalty = clamp(consistencyPenalty, 0, 0.35);

      const score = clamp(
        0.02,
        0.995,
        0.42 * reliability + 0.22 * directional + 0.2 * recency + 0.16 * pathLossScore - missPenalty - consistencyPenalty,
      );

      rows.push({
        toNodeId,
        score,
        observed,
        expected,
        missing,
        directional,
        recency,
        reliability,
        pathLoss,
        consistencyPenalty,
      });
    }

    groupedEdges.set(`${region}|${bucket}|${fromNodeId}`, rows);
  }

  const edgeRows: EdgePriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedEdges) {
    const [region, bucketText, fromNodeId] = groupKey.split('|');
    const bucket = Number(bucketText);
    for (const row of rows.sort((a, b) => b.score - a.score).slice(0, MAX_EDGE_CHOICES_PER_GROUP)) {
      edgeRows.push({
        from_node_id: fromNodeId!,
        to_node_id: row.toNodeId,
        receiver_region: region!,
        hour_bucket: bucket,
        observed_count: row.observed,
        expected_count: row.expected,
        missing_count: row.missing,
        directional_support: row.directional,
        recency_score: row.recency,
        reliability: row.reliability,
        itm_path_loss_db: row.pathLoss,
        score: row.score,
        consistency_penalty: row.consistencyPenalty,
      });
    }
  }

  const groupedMotif2 = new Map<string, Array<{ nodeIds: string; count: number }>>();
  for (const [key, count] of motif2Counts) {
    const [region, bucket, fromNodeId, nodeIds] = key.split('|');
    const groupKey = `${region}|${bucket}|${fromNodeId}`;
    const row = groupedMotif2.get(groupKey) ?? [];
    row.push({ nodeIds: nodeIds!, count });
    groupedMotif2.set(groupKey, row);
  }

  const motifRows: MotifPriorWriteRow[] = [];
  for (const [groupKey, rows] of groupedMotif2) {
    const [region, bucketText] = groupKey.split('|');
    const bucket = Number(bucketText);
    const total = motif2GroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.nodeIds, count: r.count })), MAX_MOTIF2_CHOICES_PER_GROUP)) {
      motifRows.push({
        receiver_region: region!,
        hour_bucket: bucket,
        motif_len: 2,
        node_ids: row.key,
        count: row.count,
        probability: row.count / total,
      });
    }
  }

  const groupedMotif3 = new Map<string, Array<{ nodeIds: string; count: number }>>();
  for (const [key, count] of motif3Counts) {
    const [region, bucket, head, nodeIds] = key.split('|');
    const groupKey = `${region}|${bucket}|${head}`;
    const row = groupedMotif3.get(groupKey) ?? [];
    row.push({ nodeIds: nodeIds!, count });
    groupedMotif3.set(groupKey, row);
  }

  for (const [groupKey, rows] of groupedMotif3) {
    const [region, bucketText] = groupKey.split('|');
    const bucket = Number(bucketText);
    const total = motif3GroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.nodeIds, count: r.count })), MAX_MOTIF3_CHOICES_PER_GROUP)) {
      motifRows.push({
        receiver_region: region!,
        hour_bucket: bucket,
        motif_len: 3,
        node_ids: row.key,
        count: row.count,
        probability: row.count / total,
      });
    }
  }

  const top1Accuracy = evaluatedPackets > 0 ? successPackets / evaluatedPackets : 0;
  const meanPredConfidence = evaluatedPackets > 0 ? confidenceSum / evaluatedPackets : 0;
  const confidenceScale = meanPredConfidence > 0 ? clamp(top1Accuracy / meanPredConfidence, 0.55, 1.7) : 1;
  const confidenceBias = clamp(top1Accuracy - meanPredConfidence * confidenceScale, -0.2, 0.2);
  const recommendedThreshold = clamp(0.35 + (1 - top1Accuracy) * 0.2, 0.3, 0.88);

  input.assertActive();
  const calibration = {
    evaluatedPackets,
    top1Accuracy,
    meanPredConfidence,
    confidenceScale,
    confidenceBias,
    recommendedThreshold,
  };
  const datasets: LearningDatasets = [
    publishablePrefixRows,
    positionPrefixRows,
    corridorRows,
    transitionRows,
    positionTransitionRows,
    edgeRows,
    motifRows,
  ];
  return { datasets, calibration };
}
