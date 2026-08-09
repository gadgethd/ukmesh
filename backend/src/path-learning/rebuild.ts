import { createHash } from 'node:crypto';
import { getPublicVisibilityGeneration, pool, query } from '../db/index.js';
import {
  analysisGeneration,
  beginAnalysisRun,
  finishAnalysisRun,
  startAnalysisRunHeartbeat,
  updateAnalysisRunTotalItems,
  type AnalysisRunHandle,
  type AnalysisRunHeartbeat,
} from '../analysis/runState.js';
import { assertAnalysisPublicationLease } from '../analysis/publicationFence.js';
import {
  buildNodePathHashIndex,
  getNodesForPathHash,
  nodePathHash,
  normalizePathHash,
} from '../path-hash/utils.js';
import { expandResolverScope } from '../networks.js';
import {
  PATH_LEARNING_DELTA_DEFINITIONS,
  publishPathLearningDelta,
} from './deltaPublication.js';

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
  isUnique: boolean;
};

type LearningPathNode = {
  node: LearningNode;
  isVerified: boolean;
};

const MAX_TRAINING_PACKETS = Math.min(
  500_000,
  Math.max(1_000, Number(process.env['PATH_LEARNING_MAX_TRAINING_PACKETS'] ?? 500_000) || 500_000),
);
const TRAINING_SPLIT_PERCENT = Math.min(
  100,
  Math.max(1, Number(process.env['PATH_LEARNING_TRAINING_SPLIT_PERCENT'] ?? 70) || 70),
);
const MAX_LEARNING_NODES = Math.min(
  200_000,
  Math.max(1_000, Number(process.env['PATH_LEARNING_MAX_NODES'] ?? 50_000) || 50_000),
);
const MAX_LEARNING_LINKS = Math.min(
  1_000_000,
  Math.max(1_000, Number(process.env['PATH_LEARNING_MAX_LINKS'] ?? 200_000) || 200_000),
);
const MAX_AGGREGATE_KEYS = Math.min(
  5_000_000,
  Math.max(10_000, Number(process.env['PATH_LEARNING_MAX_AGGREGATE_KEYS'] ?? 1_000_000) || 1_000_000),
);
const LEARNING_RUN_DEADLINE_MS = Math.max(
  60_000,
  Number(process.env['PATH_LEARNING_RUN_DEADLINE_MS'] ?? 45 * 60_000) || 45 * 60_000,
);
const MAX_PREFIX_CHOICES_PER_GROUP = 3;
const MAX_TRANSITIONS_PER_GROUP = 5;
const MAX_EDGE_CHOICES_PER_GROUP = 8;
const MAX_MOTIF2_CHOICES_PER_GROUP = 6;
const MAX_MOTIF3_CHOICES_PER_GROUP = 4;
const MAX_POSITION_PREFIX_CHOICES_PER_GROUP = 64;
const MAX_CORRIDOR_CHOICES_PER_GROUP = 16;
const MAX_POSITION_TRANSITIONS_PER_GROUP = 16;
const HOUR_BUCKET_SIZE = 6;
const PREFIX_AMBIGUITY_RADIUS_KM = 45;
const PATH_LEARNING_ALGORITHM_VERSION = 'path-learning-v4-delta';

type PrefixPriorWriteRow = {
  prefix: string;
  receiver_region: string;
  prev_prefix: string;
  node_id: string;
  count: number;
  probability: number;
};

type TransitionPriorWriteRow = {
  from_node_id: string;
  to_node_id: string;
  receiver_region: string;
  count: number;
  probability: number;
};

type PositionPrefixPriorWriteRow = {
  prefix: string;
  position: number;
  node_id: string;
  count: number;
  probability: number;
};

type CorridorPriorWriteRow = {
  src_node_id: string;
  rx_node_id: string;
  position: number;
  node_id: string;
  count: number;
  probability: number;
};

type PositionTransitionPriorWriteRow = {
  position: number;
  from_node_id: string;
  to_node_id: string;
  count: number;
  probability: number;
};

type EdgePriorWriteRow = {
  from_node_id: string;
  to_node_id: string;
  receiver_region: string;
  hour_bucket: number;
  observed_count: number;
  expected_count: number;
  missing_count: number;
  directional_support: number;
  recency_score: number;
  reliability: number;
  itm_path_loss_db: number | null;
  score: number;
  consistency_penalty: number;
};

type MotifPriorWriteRow = {
  receiver_region: string;
  hour_bucket: number;
  motif_len: number;
  node_ids: string;
  count: number;
  probability: number;
};

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

function resolvePathForPacket(
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
    const candidates = allCandidates.filter((n) => !visited.has(n.node_id));
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
      resolved.unshift({ prefix, node: best, isUnique: allCandidates.length === 1 });
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

type PathLearningCalibration = {
  evaluatedPackets: number;
  top1Accuracy: number;
  meanPredConfidence: number;
  confidenceScale: number;
  confidenceBias: number;
  recommendedThreshold: number;
};

export function pathLearningInputHash(input: {
  modelNetwork: string;
  sourceNetwork: string;
  windowStart: Date;
  windowEnd: Date;
  privacyGeneration: number;
  nodes: LearningNode[];
  links: LearningLink[];
  packets: LearningPacket[];
}): string {
  const hash = createHash('sha256');
  hash.update(JSON.stringify({
    algorithm: PATH_LEARNING_ALGORITHM_VERSION,
    modelNetwork: input.modelNetwork,
    sourceNetwork: input.sourceNetwork,
    windowStart: input.windowStart.toISOString(),
    windowEnd: input.windowEnd.toISOString(),
    privacyGeneration: input.privacyGeneration,
    config: {
      trainingSplitPercent: TRAINING_SPLIT_PERCENT,
      maxTrainingPackets: MAX_TRAINING_PACKETS,
      hourBucketSize: HOUR_BUCKET_SIZE,
      prefixAmbiguityRadiusKm: PREFIX_AMBIGUITY_RADIUS_KM,
      prefixChoices: MAX_PREFIX_CHOICES_PER_GROUP,
      transitions: MAX_TRANSITIONS_PER_GROUP,
      edgeChoices: MAX_EDGE_CHOICES_PER_GROUP,
      motif2Choices: MAX_MOTIF2_CHOICES_PER_GROUP,
      motif3Choices: MAX_MOTIF3_CHOICES_PER_GROUP,
      positionPrefixChoices: MAX_POSITION_PREFIX_CHOICES_PER_GROUP,
      corridorChoices: MAX_CORRIDOR_CHOICES_PER_GROUP,
      positionTransitions: MAX_POSITION_TRANSITIONS_PER_GROUP,
    },
  }));
  for (const [label, rows] of [
    ['nodes', input.nodes],
    ['links', input.links],
    ['packets', input.packets],
  ] as const) {
    hash.update(`\n${label}\n`);
    for (const row of rows) hash.update(`${JSON.stringify(row)}\n`);
  }
  return hash.digest('hex');
}

function pathLearningModelHash(
  datasets: object[][],
  calibration: PathLearningCalibration,
): string {
  const hash = createHash('sha256');
  hash.update(JSON.stringify({
    algorithm: PATH_LEARNING_ALGORITHM_VERSION,
    calibration,
  }));
  for (const [index, rows] of datasets.entries()) {
    hash.update(`\ntable:${PATH_LEARNING_DELTA_DEFINITIONS[index]?.target ?? index}\n`);
    for (const row of rows.map((value) => JSON.stringify(value)).sort()) {
      hash.update(`${row}\n`);
    }
  }
  return hash.digest('hex');
}

async function publishPathLearningRowsDelta(
  network: string,
  datasets: object[][],
  calibration: PathLearningCalibration,
  metadata: {
    inputHash: string;
    modelHash: string;
    privacyGeneration: number;
    windowStart: Date;
    windowEnd: Date;
  },
  analysisRun: AnalysisRunHandle,
  heartbeat: AnalysisRunHeartbeat,
): Promise<{ skipped: boolean; upserted: number; deleted: number }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    heartbeat.assertOwned();
    await assertAnalysisPublicationLease(client, analysisRun);
    const current = await client.query<{ model_hash: string | null }>(
      `SELECT model_hash FROM path_model_calibration WHERE network = $1 FOR UPDATE`,
      [network],
    );
    let upserted = 0;
    let deleted = 0;
    const skipped = current.rows[0]?.model_hash === metadata.modelHash;
    if (!skipped) {
      const delta = await publishPathLearningDelta(
        client,
        network,
        PATH_LEARNING_DELTA_DEFINITIONS.map((definition, index) => ({
          definition,
          rows: datasets[index] ?? [],
        })),
        heartbeat.assertOwned,
      );
      upserted = delta.upserted;
      deleted = delta.deleted;
    }
    const mutationCount = upserted + deleted;
    await client.query(
      `INSERT INTO path_model_calibration
         (network, evaluated_packets, top1_accuracy, mean_pred_confidence,
          confidence_scale, confidence_bias, recommended_threshold,
          input_hash, model_hash, algorithm_version, privacy_generation,
          window_start, window_end, last_mutation_count, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
       ON CONFLICT (network) DO UPDATE SET
         evaluated_packets = EXCLUDED.evaluated_packets,
         top1_accuracy = EXCLUDED.top1_accuracy,
         mean_pred_confidence = EXCLUDED.mean_pred_confidence,
         confidence_scale = EXCLUDED.confidence_scale,
         confidence_bias = EXCLUDED.confidence_bias,
         recommended_threshold = EXCLUDED.recommended_threshold,
         input_hash = EXCLUDED.input_hash,
         model_hash = EXCLUDED.model_hash,
         algorithm_version = EXCLUDED.algorithm_version,
         privacy_generation = EXCLUDED.privacy_generation,
         window_start = EXCLUDED.window_start,
         window_end = EXCLUDED.window_end,
         last_mutation_count = EXCLUDED.last_mutation_count,
         updated_at = NOW()`,
      [
        network,
        calibration.evaluatedPackets,
        calibration.top1Accuracy,
        calibration.meanPredConfidence,
        calibration.confidenceScale,
        calibration.confidenceBias,
        calibration.recommendedThreshold,
        metadata.inputHash,
        metadata.modelHash,
        PATH_LEARNING_ALGORITHM_VERSION,
        metadata.privacyGeneration,
        metadata.windowStart,
        metadata.windowEnd,
        mutationCount,
      ],
    );
    heartbeat.assertOwned();
    await assertAnalysisPublicationLease(client, analysisRun);
    await client.query('COMMIT');
    return { skipped, upserted, deleted };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function rebuildPathLearningModels(): Promise<void> {
  const windowEnd = new Date();
  const windowStart = new Date(windowEnd.getTime() - 30 * 24 * 60 * 60_000);
  const networksResult = await query<{ network: string }>(
    `SELECT DISTINCT network
     FROM packets
     WHERE network IS NOT NULL
       AND time > $1::timestamptz
       AND time <= $2::timestamptz
       AND rx_node_id IS NOT NULL
       AND path_hashes IS NOT NULL
       AND cardinality(path_hashes) > 0
       AND path_hash_size_bytes >= 2
       AND mod(hashtext(packet_hash)::bigint + 2147483648, 100) < ${TRAINING_SPLIT_PERCENT}
     ORDER BY network
     LIMIT 33`,
    [windowStart, windowEnd],
  );
  if (networksResult.rows.length > 32) throw new Error('PATH_LEARNING_NETWORK_LIMIT');
  const networks = networksResult.rows.map((r) => r.network).filter(Boolean);
  for (const network of networks) {
    await rebuildNetwork(network, network, windowStart, windowEnd);
  }
  // The lazy resolver always falls back to this model. An empty source window
  // skips publication and deliberately leaves the last complete `all` model.
  await rebuildNetwork('all', undefined, windowStart, windowEnd);
}

async function rebuildNetwork(
  modelNetwork: string,
  sourceNetwork: string | undefined,
  windowStart: Date,
  windowEnd: Date,
): Promise<void> {
  const visibilityGeneration = await getPublicVisibilityGeneration();
  const run = await beginAnalysisRun({
    workload: 'path-learning',
    scope: modelNetwork,
    windowStart,
    windowEnd,
    totalItems: 0,
    deadlineMs: LEARNING_RUN_DEADLINE_MS,
    privacyGeneration: visibilityGeneration,
    modelGeneration: PATH_LEARNING_ALGORITHM_VERSION,
  });
  const heartbeat = startAnalysisRunHeartbeat(run);
  try {
    const generation = await rebuildNetworkUnderLease(
      modelNetwork,
      sourceNetwork,
      run,
      heartbeat,
    );
    await heartbeat();
    await finishAnalysisRun(run, {
      status: 'complete',
      checkpoint: 1,
      generation,
    });
  } catch (error) {
    try {
      const timedOut = Date.now() >= run.deadlineAt.getTime();
      if (timedOut) await heartbeat.stopForTerminal();
      else await heartbeat();
      await finishAnalysisRun(run, {
        status: timedOut ? 'timed_out' : 'failed',
        checkpoint: 0,
        error: error instanceof Error ? error.message : String(error),
      });
    } catch (finishError) {
      console.error('[path-learning] could not record terminal run', finishError);
    }
    throw error;
  } finally {
    await heartbeat().catch(() => {});
  }
}

async function rebuildNetworkUnderLease(
  modelNetwork: string,
  sourceNetwork: string | undefined,
  run: AnalysisRunHandle,
  heartbeat: AnalysisRunHeartbeat,
): Promise<string> {
  const nowMs = Date.now();
  const deadline = run.deadlineAt.getTime();
  heartbeat.assertOwned();
  const nodeNetworkFilter = sourceNetwork ? 'AND network = $1' : '';
  const packetNetworkFilter = sourceNetwork
    ? (modelNetwork === 'ukmesh' ? 'AND network = ANY($1)' : 'AND network = $1')
    : '';
  const linkNetworkFilter = sourceNetwork ? 'AND a.network = $1 AND b.network = $1' : '';
  const nodeParams: unknown[] = sourceNetwork ? [sourceNetwork, MAX_LEARNING_NODES + 1] : [MAX_LEARNING_NODES + 1];
  const packetParams: unknown[] = sourceNetwork
    ? [
      modelNetwork === 'ukmesh' ? expandResolverScope(modelNetwork) : sourceNetwork,
      run.windowStart,
      run.windowEnd,
      MAX_TRAINING_PACKETS,
    ]
    : [run.windowStart, run.windowEnd, MAX_TRAINING_PACKETS];
  const linkParams: unknown[] = sourceNetwork ? [sourceNetwork, MAX_LEARNING_LINKS + 1] : [MAX_LEARNING_LINKS + 1];

  const packetsResult = await query<LearningPacket>(
    `SELECT DISTINCT ON (packet_hash, rx_node_id, src_node_id, path_hashes)
            time, rx_node_id, src_node_id, path_hashes
       FROM packets
      WHERE rx_node_id IS NOT NULL
        AND path_hashes IS NOT NULL
        AND cardinality(path_hashes) > 0
        AND path_hash_size_bytes >= 2
        AND time > $${sourceNetwork ? 2 : 1}::timestamptz
        AND time <= $${sourceNetwork ? 3 : 2}::timestamptz
        AND mod(hashtext(packet_hash)::bigint + 2147483648, 100) < ${TRAINING_SPLIT_PERCENT}
        ${packetNetworkFilter}
      ORDER BY packet_hash, rx_node_id, src_node_id, path_hashes, time DESC
      LIMIT $${sourceNetwork ? 4 : 3}`,
    packetParams,
    heartbeat.signal,
  );
  await updateAnalysisRunTotalItems(run, packetsResult.rows.length, heartbeat.signal);
  if (packetsResult.rows.length === 0) {
    console.log(`[path-learning] model=${modelNetwork} skipped-empty-selected-window`);
    return analysisGeneration({
      algorithm: PATH_LEARNING_ALGORITHM_VERSION,
      modelNetwork,
      sourceNetwork: sourceNetwork ?? 'all',
      windowStart: run.windowStart.toISOString(),
      windowEnd: run.windowEnd.toISOString(),
      privacyGeneration: run.privacyGeneration,
      empty: true,
    });
  }

  const nodesResult = await query<LearningNode>(
    `SELECT node_id, lat, lon, elevation_m, iata
     FROM nodes
     WHERE lat IS NOT NULL
       AND lon IS NOT NULL
       AND (name IS NULL OR name NOT LIKE '%🚫%')
       AND (role IS NULL OR role = 2)
       ${nodeNetworkFilter}
     ORDER BY node_id
    LIMIT $${sourceNetwork ? 2 : 1}`,
    nodeParams,
    heartbeat.signal,
  );
  heartbeat.assertOwned();
  if (nodesResult.rows.length > MAX_LEARNING_NODES) throw new Error('PATH_LEARNING_NODE_LIMIT');
  const nodesById = new Map(nodesResult.rows.map((n) => [n.node_id, n]));

  const pathHashIndex = buildNodePathHashIndex(nodesResult.rows);

  const linksResult = await query<LearningLink>(
    `SELECT nl.node_a_id, nl.node_b_id, nl.itm_path_loss_db, nl.count_a_to_b, nl.count_b_to_a
     FROM node_links nl
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       ${linkNetworkFilter}
     ORDER BY nl.node_a_id, nl.node_b_id
    LIMIT $${sourceNetwork ? 2 : 1}`,
    linkParams,
    heartbeat.signal,
  );
  if (linksResult.rows.length > MAX_LEARNING_LINKS) throw new Error('PATH_LEARNING_LINK_LIMIT');

  const confirmedLinks = new Set(linksResult.rows.map((r) => linkKey(r.node_a_id, r.node_b_id)));
  const linkMetaByPair = new Map(linksResult.rows.map((r) => [linkKey(r.node_a_id, r.node_b_id), r]));
  const adjacency = new Map<string, Set<string>>();
  for (const link of linksResult.rows) {
    if (!adjacency.has(link.node_a_id)) adjacency.set(link.node_a_id, new Set());
    if (!adjacency.has(link.node_b_id)) adjacency.set(link.node_b_id, new Set());
    adjacency.get(link.node_a_id)!.add(link.node_b_id);
    adjacency.get(link.node_b_id)!.add(link.node_a_id);
  }

  const inputHash = pathLearningInputHash({
    modelNetwork,
    sourceNetwork: sourceNetwork ?? 'all',
    windowStart: run.windowStart,
    windowEnd: run.windowEnd,
    privacyGeneration: Number(run.privacyGeneration),
    nodes: nodesResult.rows,
    links: linksResult.rows,
    packets: packetsResult.rows,
  });

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

  for (let packetIndex = 0; packetIndex < packetsResult.rows.length; packetIndex += 1) {
    if (packetIndex % 256 === 0) {
      heartbeat.assertOwned();
      if (Date.now() >= deadline) throw new Error('PATH_LEARNING_TIMEOUT');
    }
    const packet = packetsResult.rows[packetIndex]!;
    let hashes = packet.path_hashes?.map(normalizePathHash).filter(Boolean) ?? [];
    if (hashes.length === 0) continue;
    const rx = nodesById.get(packet.rx_node_id);
    if (!rx) continue;
    if (hashes.length > 0 && rx.node_id.toUpperCase().startsWith(hashes[hashes.length - 1]!)) {
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

    const fullNodes: LearningPathNode[] = [
      ...(src ? [{ node: src, isVerified: true }] : []),
      ...resolved.map((hop) => ({ node: hop.node, isVerified: hop.isUnique })),
      { node: rx, isVerified: true },
    ];
    let successfulEdges = 0;
    let totalEdges = 0;
    for (let i = 0; i < fullNodes.length - 1; i++) {
      const from = fullNodes[i]!;
      const to = fullNodes[i + 1]!;
      if (!from.isVerified || !to.isVerified) continue;
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
      const prevPrefix = i > 0 ? resolved[i - 1]!.prefix : '';
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
        increment(positionPrefixChoiceCounts, `${positionPrefixGroup}|${hop.node.node_id}`);
        increment(positionPrefixGroupTotals, positionPrefixGroup);

        if (src) {
          const corridorGroup = `${src.node_id}|${rx.node_id}|${i}`;
          increment(corridorChoiceCounts, `${corridorGroup}|${hop.node.node_id}`);
          increment(corridorGroupTotals, corridorGroup);
        }
      }

      if (src) {
        const championNodes = [src, ...resolved.map((hop) => hop.node), rx];
        for (let edgePosition = 0; edgePosition < championNodes.length - 1; edgePosition++) {
          const from = championNodes[edgePosition]!;
          const to = championNodes[edgePosition + 1]!;
          const group = `${edgePosition}|${from.node_id}`;
          increment(positionTransitionCounts, `${group}|${to.node_id}`);
          increment(positionTransitionGroupTotals, group);
        }
      }
    }

    for (let i = 0; i < fullNodes.length - 1; i++) {
      const from = fullNodes[i]!;
      const to = fullNodes[i + 1]!;
      if (!from.isVerified || !to.isVerified) continue;
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
        if (!next.isVerified) continue;
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
      + motif3Counts.size + motif3GroupTotals.size > MAX_AGGREGATE_KEYS
    ) {
      throw new Error('PATH_LEARNING_AGGREGATE_LIMIT');
    }
  }

  heartbeat.assertOwned();
  if (Date.now() >= deadline) throw new Error('PATH_LEARNING_TIMEOUT');

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

  heartbeat.assertOwned();
  if (Date.now() >= deadline) throw new Error('PATH_LEARNING_TIMEOUT');
  const calibration = {
    evaluatedPackets,
    top1Accuracy,
    meanPredConfidence,
    confidenceScale,
    confidenceBias,
    recommendedThreshold,
  };
  const datasets: object[][] = [
    prefixRows,
    positionPrefixRows,
    corridorRows,
    transitionRows,
    positionTransitionRows,
    edgeRows,
    motifRows,
  ];
  const modelHash = pathLearningModelHash(datasets, calibration);
  const publication = await publishPathLearningRowsDelta(
    modelNetwork,
    datasets,
    calibration,
    {
      inputHash,
      modelHash,
      privacyGeneration: Number(run.privacyGeneration),
      windowStart: run.windowStart,
      windowEnd: run.windowEnd,
    },
    run,
    heartbeat,
  );

  console.log(
    `[path-learning] model=${modelNetwork} source=${sourceNetwork ?? 'all'} packets=${evaluatedPackets} ` +
    `top1=${top1Accuracy.toFixed(3)} scale=${confidenceScale.toFixed(3)} `
      + `posPrefix=${positionPrefixRows.length} corridors=${corridorRows.length} `
      + `posTransitions=${positionTransitionRows.length} edges=${edgeRows.length} motifs=${motifRows.length}`
      + ` deltaUpserts=${publication.upserted} deltaDeletes=${publication.deleted}`
      + ` unchanged=${publication.skipped}`,
  );
  return modelHash;
}
