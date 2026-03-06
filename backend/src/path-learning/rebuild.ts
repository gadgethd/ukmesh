import { MIN_LINK_OBSERVATIONS, query } from '../db/index.js';

type LearningNode = {
  node_id: string;
  lat: number;
  lon: number;
  elevation_m: number | null;
  iata: string | null;
};

type LearningPacket = {
  time: string;
  rx_node_id: string;
  src_node_id: string | null;
  path_hashes: string[] | null;
};

type LearningLink = {
  node_a_id: string;
  node_b_id: string;
  itm_path_loss_db: number | null;
  count_a_to_b: number;
  count_b_to_a: number;
};

type ResolvedHop = {
  prefix: string;
  node: LearningNode;
};

const MAX_TRAINING_PACKETS = 120_000;
const MAX_PREFIX_CHOICES_PER_GROUP = 3;
const MAX_TRANSITIONS_PER_GROUP = 5;
const MAX_EDGE_CHOICES_PER_GROUP = 8;
const MAX_MOTIF2_CHOICES_PER_GROUP = 6;
const MAX_MOTIF3_CHOICES_PER_GROUP = 4;
const HOUR_BUCKET_SIZE = 6;
const PREFIX_AMBIGUITY_RADIUS_KM = 45;

function linkKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}

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
  prefixMap: Map<string, LearningNode[]>,
): number {
  const prefix = target.node_id.slice(0, 2).toUpperCase();
  const samePrefixNodes = prefixMap.get(prefix) ?? [];
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

function resolvePathForPacket(
  pathHashes: string[],
  srcNode: LearningNode | undefined,
  rxNode: LearningNode,
  prefixMap: Map<string, LearningNode[]>,
  confirmedLinks: Set<string>,
): ResolvedHop[] {
  const resolved: ResolvedHop[] = [];
  const visited = new Set<string>([rxNode.node_id]);
  let prev = rxNode;

  for (let i = pathHashes.length - 1; i >= 0; i--) {
    const prefix = pathHashes[i]!.slice(0, 2).toUpperCase();
    const candidates = (prefixMap.get(prefix) ?? []).filter((n) => !visited.has(n.node_id));
    if (candidates.length === 0) continue;

    let best: LearningNode | null = null;
    let bestScore = Number.NEGATIVE_INFINITY;

    for (const candidate of candidates) {
      const confirmed = confirmedLinks.has(linkKey(candidate.node_id, prev.node_id)) ? 1 : 0;
      const distanceScore = distancePrior(candidate, prev);
      const srcScore = srcNode ? (distKm(srcNode, prev) - distKm(srcNode, candidate)) / 100 : 0;
      const ambiguityPenalty = localPrefixAmbiguityPenalty(prev, candidate, prefixMap);
      const score = confirmed * 2.5 + distanceScore * 1.3 + srcScore - ambiguityPenalty;
      if (score > bestScore) {
        best = candidate;
        bestScore = score;
      }
    }

    if (best) {
      resolved.unshift({ prefix, node: best });
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

export async function rebuildPathLearningModels(): Promise<void> {
  const networksResult = await query<{ network: string }>(
    `SELECT DISTINCT network
     FROM (
       SELECT network FROM packets
       UNION
       SELECT network FROM nodes
     ) t
     WHERE network IS NOT NULL`,
  );
  const networks = networksResult.rows.map((r) => r.network).filter(Boolean);
  if (networks.length === 0) return;

  for (const network of networks) {
    await rebuildNetwork(network, network);
  }
  await rebuildNetwork('all', undefined);
}

async function rebuildNetwork(modelNetwork: string, sourceNetwork: string | undefined): Promise<void> {
  const nowMs = Date.now();
  const nodeNetworkFilter = sourceNetwork ? 'AND network = $1' : '';
  const packetNetworkFilter = sourceNetwork ? 'AND network = $1' : '';
  const linkNetworkFilter = sourceNetwork ? 'AND a.network = $1 AND b.network = $1' : '';
  const linkObsParam = sourceNetwork ? '$2' : '$1';
  const nodeParams: unknown[] = sourceNetwork ? [sourceNetwork] : [];
  const packetParams: unknown[] = sourceNetwork ? [sourceNetwork, MAX_TRAINING_PACKETS] : [MAX_TRAINING_PACKETS];
  const linkParams: unknown[] = sourceNetwork ? [sourceNetwork, MIN_LINK_OBSERVATIONS] : [MIN_LINK_OBSERVATIONS];

  const nodesResult = await query<LearningNode>(
    `SELECT node_id, lat, lon, elevation_m, iata
     FROM nodes
     WHERE lat IS NOT NULL
       AND lon IS NOT NULL
       AND (name IS NULL OR name NOT LIKE '%🚫%')
       AND (role IS NULL OR role = 2)
       ${nodeNetworkFilter}`,
    nodeParams,
  );
  const nodesById = new Map(nodesResult.rows.map((n) => [n.node_id, n]));

  const prefixMap = new Map<string, LearningNode[]>();
  for (const node of nodesResult.rows) {
    const prefix = node.node_id.slice(0, 2).toUpperCase();
    const existing = prefixMap.get(prefix);
    if (existing) existing.push(node);
    else prefixMap.set(prefix, [node]);
  }

  const linksResult = await query<LearningLink>(
    `SELECT nl.node_a_id, nl.node_b_id, nl.itm_path_loss_db, nl.count_a_to_b, nl.count_b_to_a
     FROM node_links nl
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       AND nl.observed_count >= ${linkObsParam}
       ${linkNetworkFilter}`,
    linkParams,
  );

  const confirmedLinks = new Set(linksResult.rows.map((r) => linkKey(r.node_a_id, r.node_b_id)));
  const linkMetaByPair = new Map(linksResult.rows.map((r) => [linkKey(r.node_a_id, r.node_b_id), r]));
  const adjacency = new Map<string, Set<string>>();
  for (const link of linksResult.rows) {
    if (!adjacency.has(link.node_a_id)) adjacency.set(link.node_a_id, new Set());
    if (!adjacency.has(link.node_b_id)) adjacency.set(link.node_b_id, new Set());
    adjacency.get(link.node_a_id)!.add(link.node_b_id);
    adjacency.get(link.node_b_id)!.add(link.node_a_id);
  }

  const packetsResult = await query<LearningPacket>(
    `SELECT time, rx_node_id, src_node_id, path_hashes
     FROM packets
     WHERE rx_node_id IS NOT NULL
       AND path_hashes IS NOT NULL
       AND cardinality(path_hashes) > 0
       AND time > NOW() - INTERVAL '120 days'
       ${packetNetworkFilter}
     ORDER BY time DESC
     LIMIT $${sourceNetwork ? 2 : 1}`,
    packetParams,
  );

  const prefixChoiceCounts = new Map<string, number>();
  const prefixGroupTotals = new Map<string, number>();
  const transitionCounts = new Map<string, number>();
  const transitionGroupTotals = new Map<string, number>();

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

  for (const packet of packetsResult.rows) {
    const hashes = packet.path_hashes?.map((h) => h.slice(0, 2).toUpperCase()) ?? [];
    if (hashes.length === 0) continue;
    const rx = nodesById.get(packet.rx_node_id);
    if (!rx) continue;

    const src = packet.src_node_id ? nodesById.get(packet.src_node_id) : undefined;
    const region = rx.iata ?? 'unknown';
    const resolved = resolvePathForPacket(hashes, src, rx, prefixMap, confirmedLinks);
    if (resolved.length === 0) continue;

    const ts = new Date(packet.time);
    const bucket = hourBucket(ts);

    evaluatedPackets++;

    const fullNodes = [...(src ? [src] : []), ...resolved.map((r) => r.node), rx];
    let successfulEdges = 0;
    let totalEdges = 0;
    for (let i = 0; i < fullNodes.length - 1; i++) {
      const from = fullNodes[i]!;
      const to = fullNodes[i + 1]!;
      totalEdges++;
      if (confirmedLinks.has(linkKey(from.node_id, to.node_id))) successfulEdges++;
    }
    const packetConfidence = totalEdges > 0 ? successfulEdges / totalEdges : 0;
    confidenceSum += packetConfidence;
    if (packetConfidence >= 0.6) successPackets++;

    for (let i = 0; i < resolved.length; i++) {
      const hop = resolved[i]!;
      const prevPrefix = i > 0 ? resolved[i - 1]!.prefix : '';
      const prefixGroup = `${hop.prefix}|${region}|${prevPrefix}`;
      const choiceKey = `${prefixGroup}|${hop.node.node_id}`;
      increment(prefixChoiceCounts, choiceKey);
      increment(prefixGroupTotals, prefixGroup);
    }

    for (let i = 0; i < fullNodes.length - 1; i++) {
      const from = fullNodes[i]!;
      const to = fullNodes[i + 1]!;
      const group = `${from.node_id}|${region}`;
      const edgeKey = `${group}|${to.node_id}`;
      increment(transitionCounts, edgeKey);
      increment(transitionGroupTotals, group);

      const fromGroup = `${region}|${bucket}|${from.node_id}`;
      const directedEdgeKey = `${fromGroup}|${to.node_id}`;
      increment(activeFromCounts, fromGroup);
      increment(edgeObservedCounts, directedEdgeKey);
      edgeLastSeenMs.set(directedEdgeKey, ts.getTime());

      const motif2Group = `${region}|${bucket}|${from.node_id}`;
      const motif2Key = `${motif2Group}|${from.node_id}>${to.node_id}`;
      increment(motif2Counts, motif2Key);
      increment(motif2GroupTotals, motif2Group);

      if (i < fullNodes.length - 2) {
        const next = fullNodes[i + 2]!;
        const motif3Group = `${region}|${bucket}|${from.node_id}>${to.node_id}`;
        const motif3Key = `${motif3Group}|${from.node_id}>${to.node_id}>${next.node_id}`;
        increment(motif3Counts, motif3Key);
        increment(motif3GroupTotals, motif3Group);
      }
    }
  }

  await query('DELETE FROM path_prefix_priors WHERE network = $1', [modelNetwork]);
  await query('DELETE FROM path_transition_priors WHERE network = $1', [modelNetwork]);
  await query('DELETE FROM path_edge_priors WHERE network = $1', [modelNetwork]);
  await query('DELETE FROM path_motif_priors WHERE network = $1', [modelNetwork]);

  const groupedPrefix = new Map<string, Array<{ nodeId: string; count: number }>>();
  for (const [key, count] of prefixChoiceCounts) {
    const [prefix, region, prevPrefix, nodeId] = key.split('|');
    const groupKey = `${prefix}|${region}|${prevPrefix}`;
    const row = groupedPrefix.get(groupKey) ?? [];
    row.push({ nodeId: nodeId!, count });
    groupedPrefix.set(groupKey, row);
  }

  for (const [groupKey, rows] of groupedPrefix) {
    const [prefix, region, prevPrefix] = groupKey.split('|');
    const total = prefixGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.nodeId, count: r.count })), MAX_PREFIX_CHOICES_PER_GROUP)) {
      await query(
        `INSERT INTO path_prefix_priors
           (network, prefix, receiver_region, prev_prefix, node_id, count, probability, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
         ON CONFLICT (network, prefix, receiver_region, prev_prefix, node_id) DO UPDATE SET
           count = EXCLUDED.count,
           probability = EXCLUDED.probability,
           updated_at = NOW()`,
        [modelNetwork, prefix, region, prevPrefix || '', row.key, row.count, row.count / total],
      );
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

  for (const [groupKey, rows] of groupedTransitions) {
    const [fromNodeId, region] = groupKey.split('|');
    const total = transitionGroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.toNodeId, count: r.count })), MAX_TRANSITIONS_PER_GROUP)) {
      await query(
        `INSERT INTO path_transition_priors
           (network, from_node_id, to_node_id, receiver_region, count, probability, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())
         ON CONFLICT (network, from_node_id, to_node_id, receiver_region) DO UPDATE SET
           count = EXCLUDED.count,
           probability = EXCLUDED.probability,
           updated_at = NOW()`,
        [modelNetwork, fromNodeId, row.key, region, row.count, row.count / total],
      );
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
        consistencyPenalty += localPrefixAmbiguityPenalty(fromNode, toNode, prefixMap);
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

  let edgeRowsInserted = 0;
  for (const [groupKey, rows] of groupedEdges) {
    const [region, bucketText, fromNodeId] = groupKey.split('|');
    const bucket = Number(bucketText);
    for (const row of rows.sort((a, b) => b.score - a.score).slice(0, MAX_EDGE_CHOICES_PER_GROUP)) {
      await query(
        `INSERT INTO path_edge_priors
           (network, from_node_id, to_node_id, receiver_region, hour_bucket, observed_count, expected_count, missing_count,
            directional_support, recency_score, reliability, itm_path_loss_db, score, consistency_penalty, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
         ON CONFLICT (network, receiver_region, hour_bucket, from_node_id, to_node_id) DO UPDATE SET
           observed_count = EXCLUDED.observed_count,
           expected_count = EXCLUDED.expected_count,
           missing_count = EXCLUDED.missing_count,
           directional_support = EXCLUDED.directional_support,
           recency_score = EXCLUDED.recency_score,
           reliability = EXCLUDED.reliability,
           itm_path_loss_db = EXCLUDED.itm_path_loss_db,
           score = EXCLUDED.score,
           consistency_penalty = EXCLUDED.consistency_penalty,
           updated_at = NOW()`,
        [
          modelNetwork,
          fromNodeId,
          row.toNodeId,
          region,
          bucket,
          row.observed,
          row.expected,
          row.missing,
          row.directional,
          row.recency,
          row.reliability,
          row.pathLoss,
          row.score,
          row.consistencyPenalty,
        ],
      );
      edgeRowsInserted++;
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

  let motifRowsInserted = 0;
  for (const [groupKey, rows] of groupedMotif2) {
    const [region, bucketText, fromNodeId] = groupKey.split('|');
    const bucket = Number(bucketText);
    const total = motif2GroupTotals.get(groupKey) ?? 1;
    for (const row of truncateBest(rows.map((r) => ({ key: r.nodeIds, count: r.count })), MAX_MOTIF2_CHOICES_PER_GROUP)) {
      await query(
        `INSERT INTO path_motif_priors
           (network, receiver_region, hour_bucket, motif_len, node_ids, count, probability, updated_at)
         VALUES ($1, $2, $3, 2, $4, $5, $6, NOW())
         ON CONFLICT (network, receiver_region, hour_bucket, motif_len, node_ids) DO UPDATE SET
           count = EXCLUDED.count,
           probability = EXCLUDED.probability,
           updated_at = NOW()`,
        [modelNetwork, region, bucket, row.key, row.count, row.count / total],
      );
      motifRowsInserted++;
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
      await query(
        `INSERT INTO path_motif_priors
           (network, receiver_region, hour_bucket, motif_len, node_ids, count, probability, updated_at)
         VALUES ($1, $2, $3, 3, $4, $5, $6, NOW())
         ON CONFLICT (network, receiver_region, hour_bucket, motif_len, node_ids) DO UPDATE SET
           count = EXCLUDED.count,
           probability = EXCLUDED.probability,
           updated_at = NOW()`,
        [modelNetwork, region, bucket, row.key, row.count, row.count / total],
      );
      motifRowsInserted++;
    }
  }

  const top1Accuracy = evaluatedPackets > 0 ? successPackets / evaluatedPackets : 0;
  const meanPredConfidence = evaluatedPackets > 0 ? confidenceSum / evaluatedPackets : 0;
  const confidenceScale = meanPredConfidence > 0 ? clamp(top1Accuracy / meanPredConfidence, 0.55, 1.7) : 1;
  const confidenceBias = clamp(top1Accuracy - meanPredConfidence * confidenceScale, -0.2, 0.2);
  const recommendedThreshold = clamp(0.35 + (1 - top1Accuracy) * 0.2, 0.3, 0.88);

  await query(
    `INSERT INTO path_model_calibration
       (network, evaluated_packets, top1_accuracy, mean_pred_confidence, confidence_scale, confidence_bias, recommended_threshold, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
     ON CONFLICT (network) DO UPDATE SET
       evaluated_packets = EXCLUDED.evaluated_packets,
       top1_accuracy = EXCLUDED.top1_accuracy,
       mean_pred_confidence = EXCLUDED.mean_pred_confidence,
       confidence_scale = EXCLUDED.confidence_scale,
       confidence_bias = EXCLUDED.confidence_bias,
       recommended_threshold = EXCLUDED.recommended_threshold,
       updated_at = NOW()`,
    [modelNetwork, evaluatedPackets, top1Accuracy, meanPredConfidence, confidenceScale, confidenceBias, recommendedThreshold],
  );

  console.log(
    `[path-learning] model=${modelNetwork} source=${sourceNetwork ?? 'all'} packets=${evaluatedPackets} ` +
    `top1=${top1Accuracy.toFixed(3)} scale=${confidenceScale.toFixed(3)} edges=${edgeRowsInserted} motifs=${motifRowsInserted}`,
  );
}
