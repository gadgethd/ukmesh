import { aggregateLearningEvidence } from './evidence.js';
import type { LearningNode, LearningPacket, LearningLink, PathLearningCalibration } from './model.js';
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
  PATH_LEARNING_ALGORITHM_VERSION,
} from './parameters.js';
import { createHash } from 'node:crypto';
import { readCurrentVisibilityGeneration as getPublicVisibilityGeneration } from '../privacy/authority.js';
import { analyticsPool, analyticsQuery } from '../db/runtime.js';
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
import { expandResolverScope } from '../networks.js';
import { publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import {
  PATH_LEARNING_DELTA_DEFINITIONS,
  publishPathLearningDelta,
} from './deltaPublication.js';
import { withHeavyWorkAdmission } from '../analysis/heavyWorkAdmission.js';

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
  return withHeavyWorkAdmission({
    pool: analyticsPool,
    workload: `path-learning:${network}`,
    signal: heartbeat.signal,
    task: () => publishPathLearningRowsDeltaUnderAdmission(
      network,
      datasets,
      calibration,
      metadata,
      analysisRun,
      heartbeat,
    ),
  });
}

async function publishPathLearningRowsDeltaUnderAdmission(
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
  const client = await analyticsPool.connect();
  try {
    await client.query('BEGIN');
    heartbeat.assertOwned();
    await assertAnalysisPublicationLease(client, analysisRun);
    const current = await client.query<{ model_hash: string | null }>(
      `SELECT model_hash FROM analytics.path_model_calibration WHERE network = $1 FOR UPDATE`,
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
      `INSERT INTO analytics.path_model_calibration
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

/** Networks with trainable packets in the window. Private packets must not
 * decide which models are built, so this selector carries the same predicate as
 * public packet reads. */
export function pathLearningNetworksSql(): string {
  return `SELECT DISTINCT network
     FROM mesh.packets
     WHERE network IS NOT NULL
       AND time > $1::timestamptz
       AND time <= $2::timestamptz
       AND rx_node_id IS NOT NULL
       AND path_hashes IS NOT NULL
       AND cardinality(path_hashes) > 0
       AND path_hash_size_bytes >= 2
       AND route_type IN (0, 1)
       AND mod(hashtext(packet_hash)::bigint + 2147483648, 100) < ${TRAINING_SPLIT_PERCENT}
       AND ${publicPacketPrivacySql()}
     ORDER BY network
     LIMIT 33`;
}

export async function rebuildPathLearningModels(): Promise<void> {
  const windowEnd = new Date();
  const windowStart = new Date(windowEnd.getTime() - 30 * 24 * 60 * 60_000);
  const networksResult = await analyticsQuery<{ network: string }>(
    pathLearningNetworksSql(),
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

/** Training rows must satisfy exactly the public-read privacy predicate, so a
 * packet the API hides can never influence a published path model. */
export function pathLearningTrainingPacketsSql(
  packetNetworkFilter: string,
  hasSourceNetwork: boolean,
): string {
  const windowStartParam = hasSourceNetwork ? 2 : 1;
  const windowEndParam = hasSourceNetwork ? 3 : 2;
  const limitParam = hasSourceNetwork ? 4 : 3;
  return `SELECT DISTINCT ON (packet_hash, rx_node_id, src_node_id, path_hashes)
            time, rx_node_id, src_node_id, path_hashes, payload->>'direction' AS direction
       FROM mesh.packets
      WHERE rx_node_id IS NOT NULL
        AND path_hashes IS NOT NULL
        AND cardinality(path_hashes) > 0
        AND path_hash_size_bytes >= 2
        AND route_type IN (0, 1)
        AND time > $${windowStartParam}::timestamptz
        AND time <= $${windowEndParam}::timestamptz
        AND mod(hashtext(packet_hash)::bigint + 2147483648, 100) < ${TRAINING_SPLIT_PERCENT}
        AND ${publicPacketPrivacySql()}
        ${packetNetworkFilter}
      ORDER BY packet_hash, rx_node_id, src_node_id, path_hashes, time DESC
      LIMIT $${limitParam}`;
}

/** `core.private_node_prefixes` stores 1..3-byte hashes of private node ids, so a raw
 * path prefix is private exactly when it is a leading substring of one of those
 * node ids. An empty prefix carries no identity and is never blocked. */
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

  const packetsResult = await analyticsQuery<LearningPacket>(
    pathLearningTrainingPacketsSql(packetNetworkFilter, sourceNetwork !== undefined),
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

  const nodesResult = await analyticsQuery<LearningNode>(
    `SELECT node_id, lat, lon, elevation_m, iata
     FROM core.nodes
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
  // Publication is lease-fenced on the privacy generation captured when the run
  // started, so a private-prefix snapshot taken inside the run cannot go stale
  // before the delta commits.
  const privatePrefixesResult = await analyticsQuery<{ node_id: string }>(
    'SELECT DISTINCT node_id FROM core.private_node_prefixes',
    [],
    heartbeat.signal,
  );
  const privateNodeIds = privatePrefixesResult.rows.map((row) => row.node_id);


  const linksResult = await analyticsQuery<LearningLink>(
    `SELECT nl.node_a_id, nl.node_b_id, nl.itm_path_loss_db, nl.count_a_to_b, nl.count_b_to_a
     FROM core.node_links nl
     JOIN core.nodes a ON a.node_id = nl.node_a_id
     JOIN core.nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       ${linkNetworkFilter}
     ORDER BY nl.node_a_id, nl.node_b_id
    LIMIT $${sourceNetwork ? 2 : 1}`,
    linkParams,
    heartbeat.signal,
  );
  if (linksResult.rows.length > MAX_LEARNING_LINKS) throw new Error('PATH_LEARNING_LINK_LIMIT');

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

  const { datasets, calibration } = aggregateLearningEvidence({
    nodes: nodesResult.rows, links: linksResult.rows, packets: packetsResult.rows,
    privateNodeIds, nowMs, maxAggregateKeys: MAX_AGGREGATE_KEYS,
    assertActive() {
      heartbeat.assertOwned();
      if (Date.now() >= deadline) throw new Error('PATH_LEARNING_TIMEOUT');
    },
  });
  const [, positionPrefixRows, corridorRows, , positionTransitionRows, edgeRows, motifRows] = datasets;
  const { evaluatedPackets, top1Accuracy, confidenceScale } = calibration;
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
