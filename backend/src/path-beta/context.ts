import { PATH_CACHE_POLICIES } from './cachePolicies.js';
import { edgeKey, regionalPrefixGroupKey, regionalPrefixKey, regionalTransitionKey, motifKey } from '../path-shared/priorKeys.js';
import { readCurrentVisibilityGeneration as getPublicVisibilityGeneration } from '../privacy/authority.js';
import { query } from '../db/runtime.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { PATH_CONTEXT_MAX_WEIGHT, pathContextWeight } from './contextWeight.js';
import { LinkMetricsIndex, type LinkMetricRow } from './linkMetricsIndex.js';
import { normalizePathHash } from '../path-hash/utils.js';
import { pathingConfig } from '../platform/config/pathing.js';
import { hasCoords } from './geometry.js';
import type { BetaResolveContext, MeshNode, PathLearningModel } from './types.js';
import { loadChampionPriors } from './championPriors.js';
import { expandResolverScope } from '../networks.js';

export function resolverScope(network: string): string[] | null {
  return network === 'all' ? null : expandResolverScope(network);
}

const contextCache = new BoundedTtlMap<string, BetaResolveContext>(PATH_CACHE_POLICIES.contextCache, { maxWeight: PATH_CONTEXT_MAX_WEIGHT, weightOf: pathContextWeight, ttlMs: pathingConfig.contextTtlMs });

export type PathPrefixPriorEvidence = {
  prefix: string;
  receiver_region: string;
  prev_prefix: string | null;
  node_id: string;
  count: number;
};

/** Collapse confirmed 2/3-byte training facts into calibrated one-byte priors. */
export function buildOneBytePrefixProbabilities(
  rows: readonly PathPrefixPriorEvidence[],
): Map<string, number> {
  const choiceCounts = new Map<string, number>();
  const groupTotals = new Map<string, number>();
  for (const row of rows) {
    const prefix = normalizePathHash(row.prefix).slice(0, 2);
    const previous = normalizePathHash(row.prev_prefix).slice(0, 2);
    const count = Math.max(0, Number(row.count) || 0);
    if (prefix.length !== 2 || !row.receiver_region || !row.node_id || count === 0) continue;
    const group = regionalPrefixGroupKey(row.receiver_region, prefix, previous);
    const choice = regionalPrefixKey(row.receiver_region, prefix, previous, row.node_id);
    choiceCounts.set(choice, (choiceCounts.get(choice) ?? 0) + count);
    groupTotals.set(group, (groupTotals.get(group) ?? 0) + count);
  }

  const probabilities = new Map<string, number>();
  for (const [choice, count] of choiceCounts) {
    const separator = choice.lastIndexOf('|');
    const group = separator >= 0 ? choice.slice(0, separator) : '';
    const total = groupTotals.get(group) ?? 0;
    if (total > 0) probabilities.set(choice, count / total);
  }
  return probabilities;
}

async function buildLearningModel(
  network: string,
  signal?: AbortSignal,
): Promise<PathLearningModel> {
  const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows, champion] = await Promise.all([
    query<PathPrefixPriorEvidence>(
      `SELECT prefix, receiver_region, prev_prefix, node_id, count
       FROM analytics.path_prefix_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, pathingConfig.modelLimit],
      signal,
    ),
    query<{
      from_node_id: string;
      to_node_id: string;
      receiver_region: string;
      probability: number;
    }>(
      `SELECT from_node_id, to_node_id, receiver_region, probability
       FROM analytics.path_transition_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, pathingConfig.modelLimit],
      signal,
    ),
    query<{
      from_node_id: string;
      to_node_id: string;
      receiver_region: string;
      hour_bucket: number;
      score: number;
    }>(
      `SELECT from_node_id, to_node_id, receiver_region, hour_bucket, score
       FROM analytics.path_edge_priors
       WHERE network = $1
       ORDER BY score DESC, observed_count DESC
       LIMIT $2`,
      [network, pathingConfig.modelLimit],
      signal,
    ),
    query<{
      receiver_region: string;
      hour_bucket: number;
      motif_len: number;
      node_ids: string;
      probability: number;
    }>(
      `SELECT receiver_region, hour_bucket, motif_len, node_ids, probability
       FROM analytics.path_motif_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, pathingConfig.modelLimit],
      signal,
    ),
    query<{ confidence_scale: number; confidence_bias: number }>(
      `SELECT confidence_scale, confidence_bias
       FROM analytics.path_model_calibration
       WHERE network = $1`,
      [network],
      signal,
    ),
    loadChampionPriors(network, query, signal),
  ]);

  const prefixProbabilities = buildOneBytePrefixProbabilities(prefixRows.rows);

  const transitionProbabilities = new Map<string, number>();
  for (const row of transitionRows.rows) {
    transitionProbabilities.set(
      regionalTransitionKey(row.receiver_region, row.from_node_id, row.to_node_id),
      Number(row.probability),
    );
  }

  const edgeScores = new Map<string, number>();
  const edgeTotals = new Map<string, { count: number; sum: number }>();
  for (const row of edgeRows.rows) {
    const score = Number(row.score);
    edgeScores.set(edgeKey(row.receiver_region, Number(row.hour_bucket), row.from_node_id, row.to_node_id), score);
    const aggregateKey = regionalTransitionKey(row.receiver_region, row.from_node_id, row.to_node_id);
    const aggregate = edgeTotals.get(aggregateKey) ?? { count: 0, sum: 0 };
    aggregate.count += 1;
    aggregate.sum += score;
    edgeTotals.set(aggregateKey, aggregate);
  }
  for (const [aggregateKey, aggregate] of edgeTotals) {
    const [region, fromNodeId, toNodeId] = aggregateKey.split('|');
    if (!region || !fromNodeId || !toNodeId || aggregate.count === 0) continue;
    edgeScores.set(edgeKey(region, -1, fromNodeId, toNodeId), aggregate.sum / aggregate.count);
  }

  const motifProbabilities = new Map<string, number>();
  const motifTotals = new Map<string, { count: number; sum: number }>();
  for (const row of motifRows.rows) {
    const probability = Number(row.probability);
    motifProbabilities.set(
      motifKey(row.receiver_region, Number(row.hour_bucket), Number(row.motif_len), row.node_ids),
      probability,
    );
    const aggregateKey = `${row.receiver_region}|${Number(row.motif_len)}|${row.node_ids}`;
    const aggregate = motifTotals.get(aggregateKey) ?? { count: 0, sum: 0 };
    aggregate.count += 1;
    aggregate.sum += probability;
    motifTotals.set(aggregateKey, aggregate);
  }
  for (const [aggregateKey, aggregate] of motifTotals) {
    const [region, motifLength, nodeIds] = aggregateKey.split('|');
    if (!region || !motifLength || !nodeIds || aggregate.count === 0) continue;
    motifProbabilities.set(motifKey(region, -1, Number(motifLength), nodeIds), aggregate.sum / aggregate.count);
  }

  const calibration = calibrationRows.rows[0];
  return {
    champion,
    prefixProbabilities,
    transitionProbabilities,
    edgeScores,
    motifProbabilities,
    confidenceScale: Number(calibration?.confidence_scale ?? 1),
    confidenceBias: Number(calibration?.confidence_bias ?? 0),
    bucketHours: 6,
  };
}

export function canReusePathContext(input: {
  cachedVisibilityGeneration: number;
  currentVisibilityGeneration: number;
  ageMs: number;
  pinForBatch: boolean;
}): boolean {
  return input.cachedVisibilityGeneration === input.currentVisibilityGeneration
    && (input.pinForBatch || input.ageMs < pathingConfig.contextTtlMs);
}

export type PathContextLoadOptions = {
  pinForBatch?: boolean;
  requiredVisibilityGeneration?: number;
  currentVisibilityGeneration?: number;
  signal?: AbortSignal;
};

export async function loadPathContext(
  network: string,
  options?: PathContextLoadOptions,
): Promise<BetaResolveContext> {
  for (let visibilityRetry = 0; ; visibilityRetry += 1) {
    options?.signal?.throwIfAborted();
    const now = Date.now();
    const visibilityGeneration = options?.currentVisibilityGeneration
      ?? await getPublicVisibilityGeneration(options?.signal);
    if (options?.requiredVisibilityGeneration != null
        && visibilityGeneration !== options.requiredVisibilityGeneration) {
      throw new Error('PUBLIC_VISIBILITY_CHANGED_DURING_RESOLUTION');
    }
    const cached = contextCache.get(network);
    if (cached && canReusePathContext({
      cachedVisibilityGeneration: cached.visibilityGeneration,
      currentVisibilityGeneration: visibilityGeneration,
      ageMs: now - cached.loadedAt,
      pinForBatch: options?.pinForBatch === true,
    })) return cached;

    const [nodeRows, linkRows, learningModel] = await Promise.all([
      query<MeshNode>(
        `SELECT node_id, name, lat, lon, iata, role, elevation_m, last_seen::text AS last_seen
       FROM core.nodes
       WHERE ($1::text[] IS NULL OR network = ANY($1))
         AND (name IS NULL OR name NOT LIKE '%🚫%')`,
        [resolverScope(network)],
        options?.signal,
      ),
      query<LinkMetricRow>(
        `SELECT nl.node_a_id, nl.node_b_id, nl.observed_count, nl.multibyte_observed_count,
              nl.itm_path_loss_db, nl.itm_viable, nl.count_a_to_b, nl.count_b_to_a
       FROM core.node_links nl
       JOIN core.nodes a ON a.node_id = nl.node_a_id
       JOIN core.nodes b ON b.node_id = nl.node_b_id
       WHERE (nl.itm_viable IS NOT NULL OR nl.force_viable = true)
         AND (a.name IS NULL OR a.name NOT LIKE '%🚫%')
         AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')
         AND ($1::text[] IS NULL OR (a.network = ANY($1) AND b.network = ANY($1)))`,
        [resolverScope(network)],
        options?.signal,
      ),
      buildLearningModel(network, options?.signal),
    ]);
    options?.signal?.throwIfAborted();

    const nodesById = new Map<string, MeshNode>();
    for (const row of nodeRows.rows) nodesById.set(row.node_id, row);

    const linkMetrics = new LinkMetricsIndex(linkRows.rows);
    const context: BetaResolveContext = {
      loadedAt: now,
      visibilityGeneration,
      nodesById,
      repeaterNodes: [...nodesById.values()].filter(
        (node) => hasCoords(node) && (node.role === null || node.role === 2),
      ),
      linkMetrics,
      learningModel,
    };
    const confirmedGeneration = options?.currentVisibilityGeneration
      ?? await getPublicVisibilityGeneration(options?.signal);
    if (confirmedGeneration !== visibilityGeneration) {
      if (visibilityRetry >= 1) throw new Error('PUBLIC_VISIBILITY_CHANGED_DURING_CONTEXT_LOAD');
      continue;
    }
    contextCache.set(network, context);
    return context;
  }
}
