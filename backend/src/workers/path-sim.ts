import 'node:process';
import { initDb, query } from '../db/index.js';
import {
  buildNodePathHashIndex,
  countNodesForPathHash,
  getNodesForPathHash,
  nodeMatchesPathHash,
  normalizePathHash,
} from '../path-hash/utils.js';

const RUN_INTERVAL_MS = Number(process.env['PATH_SIM_INTERVAL_MS'] ?? 6 * 60 * 60 * 1000);
const PERM_BUCKET_CAP = Number(process.env['PATH_SIM_PERM_BUCKET_CAP'] ?? 50);
const NETWORK = (process.env['PATH_SIM_NETWORK'] ?? 'all').trim().toLowerCase();
const LOG_EVERY = Math.max(1, Number(process.env['PATH_SIM_LOG_EVERY'] ?? 1));
const POPULATION_SIZE = Math.max(2, Number(process.env['PATH_SIM_POPULATION_SIZE'] ?? 10));
const WORKER_ID = String(process.env['PATH_SIM_WORKER_ID'] ?? 'solo').trim();

const BASE_MAX_SEARCH_STATES = Number(process.env['PATH_SIM_MAX_STATES'] ?? 200_000);
const BASE_MAX_CANDIDATES = Math.max(4, Number(process.env['PATH_SIM_MAX_CANDIDATES'] ?? 24));
const BASE_HOP_MILES = Number(process.env['PATH_SIM_HOP_MILES'] ?? 75);
const BASE_WEAK_PATHLOSS_DB = Number(process.env['PATH_SIM_WEAK_PATHLOSS_DB'] ?? 135);
const BASE_MODEL_BUCKET_HOURS = Math.max(1, Number(process.env['PATH_SIM_MODEL_BUCKET_HOURS'] ?? 6));
const BASE_WINDOW_DAYS = Math.max(1, Number(process.env['PATH_SIM_WINDOW_DAYS'] ?? 60));

type SimConfig = {
  maxSearchStates: number;
  maxCandidates: number;
  hopMiles: number;
  weakPathLossDb: number;
  modelBucketHours: number;
  windowDays: number;
};

type SimNode = {
  node_id: string;
  lat: number;
  lon: number;
  iata: string | null;
  elevation_m: number | null;
};

type PacketRow = {
  time: string;
  packet_hash: string;
  rx_node_id: string | null;
  src_node_id: string | null;
  packet_type: number | null;
  hop_count: number | null;
  path_hashes: string[] | null;
  network: string | null;
};

type LinkRow = {
  node_a_id: string;
  node_b_id: string;
  itm_path_loss_db: number | null;
  observed_count: number;
  count_a_to_b: number;
  count_b_to_a: number;
  itm_viable: boolean | null;
  force_viable: boolean | null;
};

type Strategy = {
  name: string;
  maxHopKm: number;
  requireExplicitEnd: boolean;
};

type ContinuationStats = {
  fullCount: number;
  partialCount: number;
  longestPrefixDepth: number;
  truncated: boolean;
  bestConfidence: number;
};

type StrategyStats = {
  packetsEligible: number;
  packetsFullyResolved: number;
  packetsUnresolved: number;
  unresolvedWithPermutations: number;
  unresolvedWithoutPermutations: number;
  truncatedSearches: number;
  noProgressPackets: number;
  totalPermutations: number;
  totalRemainingHops: number;
  totalBestConfidence: number;
  permutationHistogram: Map<string, number>;
  remainingHopsHistogram: Map<string, number>;
};

type CandidateScoreContext = {
  packetNetwork: string;
  receiverRegion: string;
  hourBucket: number;
  current: SimNode;
  candidate: SimNode;
  prevPrefix: string;
  prefix: string;
  nextTowardRx: string | null;
  linksByPair: Map<string, LinkRow>;
  weakAdjacency: Map<string, Set<string>>;
  model: LearningModel;
  config: SimConfig;
};

type LearningModel = {
  prefixProbabilities: Map<string, number>;
  transitionProbabilities: Map<string, number>;
  edgeScores: Map<string, number>;
  edgeScoresAnyHour: Map<string, number>;
  motifProbabilities: Map<string, number>;
  motifProbabilitiesAnyHour: Map<string, number>;
  calibration: Map<string, { scale: number; bias: number }>;
};

type RunContext = {
  byId: Map<string, SimNode>;
  byPrefix: Map<string, SimNode[]>;
  prefixCounts: Map<string, number>;
  linksByPair: Map<string, LinkRow>;
  weakAdjacency: Map<string, Set<string>>;
  model: LearningModel;
  config: SimConfig;
};

type RunSummary = {
  packetsEligible: number;
  packetsFullyResolved: number;
  packetsUnresolved: number;
  avgPermutationsPerEligible: number;
  avgRemainingHopsPerEligible: number;
  avgBestConfidencePerEligible: number;
};

type RunResult = {
  summary: RunSummary;
  runId: number;
};

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function linkKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}

function distKm(a: SimNode, b: SimNode): number {
  const midLat = ((a.lat + b.lat) / 2) * (Math.PI / 180);
  const dlat = (a.lat - b.lat) * 111;
  const dlon = (a.lon - b.lon) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

function bump(map: Map<string, number>, key: string, amount = 1): void {
  map.set(key, (map.get(key) ?? 0) + amount);
}

function permutationBucket(value: number): string {
  if (value >= PERM_BUCKET_CAP) return `${PERM_BUCKET_CAP}+`;
  return String(value);
}

function mapToObject(map: Map<string, number>): Record<string, number> {
  return Object.fromEntries(
    Array.from(map.entries()).sort((a, b) => {
      const aNum = Number(a[0].replace('+', ''));
      const bNum = Number(b[0].replace('+', ''));
      return aNum - bNum;
    }),
  );
}

function createStrategyStats(): StrategyStats {
  return {
    packetsEligible: 0,
    packetsFullyResolved: 0,
    packetsUnresolved: 0,
    unresolvedWithPermutations: 0,
    unresolvedWithoutPermutations: 0,
    truncatedSearches: 0,
    noProgressPackets: 0,
    totalPermutations: 0,
    totalRemainingHops: 0,
    totalBestConfidence: 0,
    permutationHistogram: new Map(),
    remainingHopsHistogram: new Map(),
  };
}

function directionalSupport(link: LinkRow | undefined, fromId: string, toId: string): number {
  if (!link) return 0.5;
  const a = fromId < toId ? fromId : toId;
  const forward = fromId === a ? link.count_a_to_b : link.count_b_to_a;
  const reverse = fromId === a ? link.count_b_to_a : link.count_a_to_b;
  const total = forward + reverse;
  if (total <= 0) return 0.5;
  return forward / total;
}

function currentHourBucket(ts: Date, config: SimConfig): number {
  return Math.floor(ts.getUTCHours() / Math.max(1, config.modelBucketHours));
}

function buildStrategies(config: SimConfig): Strategy[] {
  const hopKm = config.hopMiles * 1.609344;
  return [
    { name: 'ml_75mi', maxHopKm: hopKm, requireExplicitEnd: false },
    { name: 'ml_100mi', maxHopKm: 100 * 1.609344, requireExplicitEnd: false },
    { name: 'ml_75mi_require_end', maxHopKm: hopKm, requireExplicitEnd: true },
    { name: 'ml_100mi_require_end', maxHopKm: 100 * 1.609344, requireExplicitEnd: true },
  ];
}

function lookupPrefixPrior(model: LearningModel, network: string, region: string, prefix: string, prevPrefix: string, nodeId: string): number {
  const networks = network === 'all' ? ['teesside', 'ukmesh'] : [network, 'teesside', 'ukmesh'];
  const regions = [region, 'unknown'];
  for (const net of networks) {
    for (const reg of regions) {
      const exact = `${net}|${reg}|${prefix}|${prevPrefix}|${nodeId}`;
      const noPrev = `${net}|${reg}|${prefix}||${nodeId}`;
      const found = model.prefixProbabilities.get(exact) ?? model.prefixProbabilities.get(noPrev);
      if (found != null) return found;
    }
  }
  return 0;
}

function lookupTransitionPrior(model: LearningModel, network: string, region: string, fromNodeId: string, toNodeId: string): number {
  const networks = network === 'all' ? ['teesside', 'ukmesh'] : [network, 'teesside', 'ukmesh'];
  const regions = [region, 'unknown'];
  for (const net of networks) {
    for (const reg of regions) {
      const key = `${net}|${reg}|${fromNodeId}|${toNodeId}`;
      const found = model.transitionProbabilities.get(key);
      if (found != null) return found;
    }
  }
  return 0;
}

function lookupEdgePrior(model: LearningModel, network: string, region: string, bucket: number, fromNodeId: string, toNodeId: string): number {
  const networks = network === 'all' ? ['teesside', 'ukmesh'] : [network, 'teesside', 'ukmesh'];
  const regions = [region, 'unknown'];
  for (const net of networks) {
    for (const reg of regions) {
      const exact = `${net}|${reg}|${bucket}|${fromNodeId}|${toNodeId}`;
      const fallback = `${net}|${reg}|${fromNodeId}|${toNodeId}`;
      const found = model.edgeScores.get(exact) ?? model.edgeScoresAnyHour.get(fallback);
      if (found != null) return found;
    }
  }
  return 0;
}

function lookupMotifPrior(model: LearningModel, network: string, region: string, bucket: number, nodeIds: string[]): number {
  if (nodeIds.length < 2 || nodeIds.length > 3) return 0;
  const path = nodeIds.join('>');
  const networks = network === 'all' ? ['teesside', 'ukmesh'] : [network, 'teesside', 'ukmesh'];
  const regions = [region, 'unknown'];
  for (const net of networks) {
    for (const reg of regions) {
      const exact = `${net}|${reg}|${bucket}|${path}`;
      const fallback = `${net}|${reg}|${path}`;
      const found = model.motifProbabilities.get(exact) ?? model.motifProbabilitiesAnyHour.get(fallback);
      if (found != null) return found;
    }
  }
  return 0;
}

function lookupCalibration(model: LearningModel, network: string): { scale: number; bias: number } {
  return model.calibration.get(network)
    ?? model.calibration.get('teesside')
    ?? model.calibration.get('ukmesh')
    ?? { scale: 1, bias: 0 };
}

function localHexClashPenalty(
  candidate: SimNode,
  current: SimNode,
  pathHash: string,
  prefixCounts: Map<string, number>,
  byPrefix: Map<string, SimNode[]>,
  weakAdjacency: Map<string, Set<string>>,
): number {
  const total = prefixCounts.get(pathHash) ?? 0;
  if (total <= 1) return 0;
  const peers = byPrefix.get(pathHash) ?? [];
  let raw = 0;
  const inRangeKm = 75;
  for (const peer of peers) {
    if (peer.node_id === candidate.node_id) continue;
    const d = distKm(peer, candidate);
    if (d > inRangeKm) continue;
    const nearCurrent = distKm(peer, current) <= inRangeKm;
    const weakLinked = weakAdjacency.get(candidate.node_id)?.has(peer.node_id) ?? false;
    if (!nearCurrent && !weakLinked) continue;
    raw += weakLinked ? 1 : 0.45;
  }
  return clamp(raw * 0.07, 0, 0.24);
}

function scoreCandidate(ctx: CandidateScoreContext, prefixCounts: Map<string, number>, byPrefix: Map<string, SimNode[]>): number {
  const { candidate, current, linksByPair, weakAdjacency, model, config } = ctx;
  const pair = linksByPair.get(linkKey(candidate.node_id, current.node_id));
  const d = distKm(candidate, current);
  const distanceScore = Math.exp(-d / 26);
  const pathLoss = pair?.itm_path_loss_db;
  const pathLossScore = pathLoss == null ? 0.45 : clamp((145 - pathLoss) / 22, 0, 1);
  const observedScore = clamp(Math.log1p(pair?.observed_count ?? 0) / 5, 0, 1);
  const directionScore = directionalSupport(pair, candidate.node_id, current.node_id);
  const viableEdge = Boolean(pathLoss != null && pathLoss <= config.weakPathLossDb);
  const elevationScore = clamp((((candidate.elevation_m ?? 0) + (current.elevation_m ?? 0)) / 2 + 60) / 320, 0, 1);

  const prefixPrior = lookupPrefixPrior(model, ctx.packetNetwork, ctx.receiverRegion, ctx.prefix, ctx.prevPrefix, candidate.node_id);
  const transitionPrior = lookupTransitionPrior(model, ctx.packetNetwork, ctx.receiverRegion, candidate.node_id, current.node_id);
  const edgePrior = lookupEdgePrior(model, ctx.packetNetwork, ctx.receiverRegion, ctx.hourBucket, candidate.node_id, current.node_id);
  const motif2 = lookupMotifPrior(model, ctx.packetNetwork, ctx.receiverRegion, ctx.hourBucket, [candidate.node_id, current.node_id]);
  const motif3 = ctx.nextTowardRx
    ? lookupMotifPrior(model, ctx.packetNetwork, ctx.receiverRegion, ctx.hourBucket, [candidate.node_id, current.node_id, ctx.nextTowardRx])
    : 0;

  const ambiguityPenalty = localHexClashPenalty(candidate, current, ctx.prefix, prefixCounts, byPrefix, weakAdjacency);
  const weakAdjacencyBoost = weakAdjacency.get(candidate.node_id)?.has(current.node_id) ? 0.04 : 0;
  const veryHighLossPenalty = pathLoss != null && pathLoss > 145 ? 0.12 : 0;

  const raw = 0.2 * distanceScore
    + 0.12 * pathLossScore
    + 0.08 * observedScore
    + 0.06 * directionScore
    + 0.06 * elevationScore
    + 0.16 * prefixPrior
    + 0.14 * transitionPrior
    + 0.14 * edgePrior
    + 0.07 * motif2
    + 0.05 * motif3
    + weakAdjacencyBoost
    + (viableEdge ? 0.03 : 0)
    - ambiguityPenalty
    - veryHighLossPenalty;
  return clamp(raw, 0.01, 0.99);
}

function getCandidatesForPrefix(
  prefix: string,
  prevPrefix: string,
  current: SimNode,
  visited: Set<string>,
  srcNodeId: string,
  nextTowardRx: string | null,
  strategy: Strategy,
  context: RunContext,
  packetNetwork: string,
  receiverRegion: string,
  hourBucket: number,
): Array<{ node: SimNode; conf: number }> {
  const pool = getNodesForPathHash(context.byPrefix, prefix);
  const scored: Array<{ node: SimNode; conf: number }> = [];
  for (const node of pool) {
    if (visited.has(node.node_id)) continue;
    if (node.node_id === srcNodeId && !nodeMatchesPathHash(srcNodeId, prefix)) continue;
    const d = distKm(node, current);
    if (d > strategy.maxHopKm) continue;
    const conf = scoreCandidate({
      packetNetwork,
      receiverRegion,
      hourBucket,
      current,
      candidate: node,
      prevPrefix,
      prefix,
      nextTowardRx,
      linksByPair: context.linksByPair,
      weakAdjacency: context.weakAdjacency,
      model: context.model,
      config: context.config,
    }, context.prefixCounts, context.byPrefix);
    scored.push({ node, conf });
  }
  scored.sort((a, b) => b.conf - a.conf);
  const limit = Math.max(6, Math.min(context.config.maxCandidates, Math.floor(6 + Math.sqrt(scored.length) * 4)));
  return scored.slice(0, limit);
}

function enumerateContinuationsMl(
  srcNodeId: string,
  pathHashes: string[],
  rxNodeId: string,
  packetTime: Date,
  packetNetwork: string,
  strategy: Strategy,
  context: RunContext,
): ContinuationStats {
  if (pathHashes.length === 0 || srcNodeId === rxNodeId) {
    return { fullCount: 0, partialCount: 0, longestPrefixDepth: 0, truncated: false, bestConfidence: 0 };
  }
  const rx = context.byId.get(rxNodeId);
  const src = context.byId.get(srcNodeId);
  if (!rx || !src) {
    return { fullCount: 0, partialCount: 0, longestPrefixDepth: 0, truncated: false, bestConfidence: 0 };
  }

  const prefixes = pathHashes.map(normalizePathHash).filter(Boolean);
  const receiverRegion = rx.iata ?? 'unknown';
  const hourBucket = currentHourBucket(packetTime, context.config);
  const calibration = lookupCalibration(context.model, packetNetwork);

  let fullCount = 0;
  let partialCount = 0;
  let longestPrefixDepth = 0;
  let bestPartialDepth = 0;
  let bestConfidence = 0;
  let states = 0;
  let truncated = false;

  const updatePartial = (depth: number) => {
    if (depth > bestPartialDepth) {
      bestPartialDepth = depth;
      partialCount = 0;
    }
    if (depth === bestPartialDepth) partialCount += 1;
  };

  const dfs = (
    idx: number,
    current: SimNode,
    visited: Set<string>,
    confSum: number,
    nextTowardRx: string | null,
  ) => {
    const consumed = prefixes.length - Math.max(0, idx + 1);
    if (consumed > longestPrefixDepth) longestPrefixDepth = consumed;
    if (states++ >= context.config.maxSearchStates) {
      truncated = true;
      return;
    }

    if (idx < 0) {
      const atSrc = current.node_id === src.node_id;
      const appendSrcAllowed = !strategy.requireExplicitEnd && !visited.has(src.node_id);
      if (atSrc || appendSrcAllowed) {
        fullCount += 1;
        const raw = confSum / Math.max(1, prefixes.length);
        const calibrated = clamp(raw * calibration.scale + calibration.bias, 0, 1);
        if (calibrated > bestConfidence) bestConfidence = calibrated;
      } else {
        updatePartial(prefixes.length);
      }
      return;
    }

    const prefix = prefixes[idx]!;
    const prevPrefix = idx > 0 ? prefixes[idx - 1]! : '';
    const options = getCandidatesForPrefix(
      prefix,
      prevPrefix,
      current,
      visited,
      src.node_id,
      nextTowardRx,
      strategy,
      context,
      packetNetwork,
      receiverRegion,
      hourBucket,
    );
    if (options.length === 0) {
      updatePartial(consumed);
      return;
    }

    for (const option of options) {
      visited.add(option.node.node_id);
      dfs(idx - 1, option.node, visited, confSum + option.conf, current.node_id);
      visited.delete(option.node.node_id);
      if (truncated) return;
    }
  };

  const visited = new Set<string>([rx.node_id]);
  dfs(prefixes.length - 1, rx, visited, 0, null);
  return { fullCount, partialCount, longestPrefixDepth, truncated, bestConfidence };
}

async function loadNodes(): Promise<{ byId: Map<string, SimNode>; byPrefix: Map<string, SimNode[]>; prefixCounts: Map<string, number> }> {
  const hasNetwork = NETWORK !== 'all';
  const networkFilter = hasNetwork ? 'AND n.network = $1' : '';
  const packetNetworkFilter = hasNetwork ? 'AND p.network = $1' : '';
  const params: unknown[] = NETWORK === 'all' ? [] : [NETWORK];
  const nodeRows = await query<SimNode>(
    `SELECT n.node_id, n.lat, n.lon, n.iata, n.elevation_m
     FROM nodes n
     WHERE n.lat IS NOT NULL
       AND n.lon IS NOT NULL
       AND (n.role IS NULL OR n.role = 2)
       ${networkFilter}
       AND EXISTS (
         SELECT 1
         FROM packets p
         WHERE (p.src_node_id = n.node_id OR p.rx_node_id = n.node_id)
           ${packetNetworkFilter}
       )`,
    params,
  );

  const byId = new Map<string, SimNode>();
  for (const row of nodeRows.rows) {
    byId.set(row.node_id, row);
  }
  const byPrefix = buildNodePathHashIndex(nodeRows.rows);
  const prefixCounts = new Map<string, number>(
    Array.from(byPrefix.entries()).map(([prefix, nodes]) => [prefix, nodes.length]),
  );
  return { byId, byPrefix, prefixCounts };
}

async function loadLinks(config: SimConfig): Promise<{ linksByPair: Map<string, LinkRow>; weakAdjacency: Map<string, Set<string>> }> {
  const hasNetwork = NETWORK !== 'all';
  const networkFilter = hasNetwork ? 'AND a.network = $1 AND b.network = $1' : '';
  const params: unknown[] = hasNetwork ? [NETWORK] : [];
  const result = await query<LinkRow>(
    `SELECT nl.node_a_id, nl.node_b_id, nl.itm_path_loss_db, nl.observed_count,
            nl.count_a_to_b, nl.count_b_to_a, nl.itm_viable, nl.force_viable
     FROM node_links nl
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE 1 = 1
       ${networkFilter}`,
    params,
  );

  const linksByPair = new Map<string, LinkRow>();
  const weakAdjacency = new Map<string, Set<string>>();
  for (const row of result.rows) {
    const key = linkKey(row.node_a_id, row.node_b_id);
    linksByPair.set(key, row);
    const pathLoss = row.itm_path_loss_db;
    const weakOrBetter = pathLoss != null && pathLoss <= config.weakPathLossDb;
    if (!weakOrBetter) continue;
    if (!weakAdjacency.has(row.node_a_id)) weakAdjacency.set(row.node_a_id, new Set());
    if (!weakAdjacency.has(row.node_b_id)) weakAdjacency.set(row.node_b_id, new Set());
    weakAdjacency.get(row.node_a_id)!.add(row.node_b_id);
    weakAdjacency.get(row.node_b_id)!.add(row.node_a_id);
  }
  return { linksByPair, weakAdjacency };
}

async function loadLearningModel(): Promise<LearningModel> {
  const hasNetwork = NETWORK !== 'all';
  const networkFilter = hasNetwork ? 'WHERE network = $1' : '';
  const params: unknown[] = hasNetwork ? [NETWORK] : [];
  const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
    query<{ network: string; receiver_region: string; prefix: string; prev_prefix: string | null; node_id: string; probability: number }>(
      `SELECT network, receiver_region, prefix, prev_prefix, node_id, probability FROM path_prefix_priors ${networkFilter}`,
      params,
    ),
    query<{ network: string; receiver_region: string; from_node_id: string; to_node_id: string; probability: number }>(
      `SELECT network, receiver_region, from_node_id, to_node_id, probability FROM path_transition_priors ${networkFilter}`,
      params,
    ),
    query<{ network: string; receiver_region: string; hour_bucket: number; from_node_id: string; to_node_id: string; score: number }>(
      `SELECT network, receiver_region, hour_bucket, from_node_id, to_node_id, score FROM path_edge_priors ${networkFilter}`,
      params,
    ),
    query<{ network: string; receiver_region: string; hour_bucket: number; node_ids: string; probability: number }>(
      `SELECT network, receiver_region, hour_bucket, node_ids, probability FROM path_motif_priors ${networkFilter}`,
      params,
    ),
    query<{ network: string; confidence_scale: number; confidence_bias: number }>(
      `SELECT network, confidence_scale, confidence_bias FROM path_model_calibration ${networkFilter}`,
      params,
    ),
  ]);

  const model: LearningModel = {
    prefixProbabilities: new Map(),
    transitionProbabilities: new Map(),
    edgeScores: new Map(),
    edgeScoresAnyHour: new Map(),
    motifProbabilities: new Map(),
    motifProbabilitiesAnyHour: new Map(),
    calibration: new Map(),
  };

  for (const row of prefixRows.rows) model.prefixProbabilities.set(`${row.network}|${row.receiver_region}|${row.prefix}|${row.prev_prefix ?? ''}|${row.node_id}`, Number(row.probability));
  for (const row of transitionRows.rows) model.transitionProbabilities.set(`${row.network}|${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`, Number(row.probability));
  for (const row of edgeRows.rows) {
    const score = Number(row.score);
    model.edgeScores.set(`${row.network}|${row.receiver_region}|${row.hour_bucket}|${row.from_node_id}|${row.to_node_id}`, score);
    const anyKey = `${row.network}|${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`;
    const existing = model.edgeScoresAnyHour.get(anyKey);
    if (existing == null || score > existing) model.edgeScoresAnyHour.set(anyKey, score);
  }
  for (const row of motifRows.rows) {
    const prob = Number(row.probability);
    model.motifProbabilities.set(`${row.network}|${row.receiver_region}|${row.hour_bucket}|${row.node_ids}`, prob);
    const anyKey = `${row.network}|${row.receiver_region}|${row.node_ids}`;
    const existing = model.motifProbabilitiesAnyHour.get(anyKey);
    if (existing == null || prob > existing) model.motifProbabilitiesAnyHour.set(anyKey, prob);
  }
  for (const row of calibrationRows.rows) model.calibration.set(row.network, { scale: Number(row.confidence_scale), bias: Number(row.confidence_bias) });
  return model;
}

async function loadPackets(windowDays: number): Promise<PacketRow[]> {
  const hasNetwork = NETWORK !== 'all';
  const params: unknown[] = hasNetwork ? [NETWORK, windowDays] : [windowDays];
  const networkFilter = hasNetwork ? 'AND network = $1' : '';
  const windowPos = hasNetwork ? '$2' : '$1';
  const result = await query<PacketRow>(
    `SELECT DISTINCT ON (packet_hash)
       time::text AS time, packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, network
     FROM packets
     WHERE packet_hash IS NOT NULL
       AND packet_hash <> ''
       AND time > NOW() - (${windowPos}::int * INTERVAL '1 day')
       ${networkFilter}
     ORDER BY packet_hash, time DESC`,
    params,
  );
  return result.rows;
}

function normalizeConfig(config: Partial<SimConfig>): SimConfig {
  return {
    maxSearchStates: Math.max(30_000, Math.round(config.maxSearchStates ?? BASE_MAX_SEARCH_STATES)),
    maxCandidates: Math.max(4, Math.min(64, Math.round(config.maxCandidates ?? BASE_MAX_CANDIDATES))),
    hopMiles: clamp(Number(config.hopMiles ?? BASE_HOP_MILES), 45, 140),
    weakPathLossDb: clamp(Number(config.weakPathLossDb ?? BASE_WEAK_PATHLOSS_DB), 125, 145),
    modelBucketHours: Math.max(1, Math.min(24, Math.round(config.modelBucketHours ?? BASE_MODEL_BUCKET_HOURS))),
    windowDays: Math.max(1, Math.min(365, Math.round(config.windowDays ?? BASE_WINDOW_DAYS))),
  };
}

function seededNoise(seed: number): number {
  let x = Math.sin(seed * 12.9898) * 43758.5453;
  x = x - Math.floor(x);
  return x * 2 - 1;
}

function mutateConfig(base: SimConfig, generation: number, slotIndex: number): SimConfig {
  if (slotIndex <= 1) return { ...base };
  const n1 = seededNoise(generation * 100 + slotIndex * 11);
  const n2 = seededNoise(generation * 100 + slotIndex * 13);
  const n3 = seededNoise(generation * 100 + slotIndex * 17);
  const n4 = seededNoise(generation * 100 + slotIndex * 19);
  const n5 = seededNoise(generation * 100 + slotIndex * 23);
  return normalizeConfig({
    hopMiles: base.hopMiles * (1 + n1 * 0.14),
    maxCandidates: Math.round(base.maxCandidates * (1 + n2 * 0.20)),
    maxSearchStates: Math.round(base.maxSearchStates * (1 + n3 * 0.25)),
    weakPathLossDb: base.weakPathLossDb + n4 * 2.5,
    modelBucketHours: Math.round(base.modelBucketHours + n5 * 2),
    windowDays: base.windowDays,
  });
}

function runFitness(summary: RunSummary): number {
  const eligible = Math.max(1, summary.packetsEligible);
  const resolvedRate = summary.packetsFullyResolved / eligible;
  const remainingPenalty = clamp(summary.avgRemainingHopsPerEligible / 10, 0, 1);
  const unresolvedPenalty = summary.packetsUnresolved / eligible;
  return resolvedRate + summary.avgBestConfidencePerEligible * 0.12 - remainingPenalty * 0.25 - unresolvedPenalty * 0.08;
}

async function ensureEvolutionSeed(): Promise<void> {
  await query(
    `INSERT INTO path_sim_evolution_state (id, current_generation, evolved_generation, updated_at)
     VALUES (1, 1, 0, NOW())
     ON CONFLICT (id) DO NOTHING`,
  );
  const seeded = await query<{ c: number }>(`SELECT COUNT(*)::int AS c FROM path_sim_population WHERE generation = 1`);
  if (Number(seeded.rows[0]?.c ?? 0) >= POPULATION_SIZE) return;
  for (let i = 1; i <= POPULATION_SIZE; i++) {
    const variantId = `v${String(i).padStart(2, '0')}`;
    const seedCfg = mutateConfig(
      normalizeConfig({
        maxSearchStates: BASE_MAX_SEARCH_STATES,
        maxCandidates: BASE_MAX_CANDIDATES,
        hopMiles: BASE_HOP_MILES,
        weakPathLossDb: BASE_WEAK_PATHLOSS_DB,
        modelBucketHours: BASE_MODEL_BUCKET_HOURS,
        windowDays: BASE_WINDOW_DAYS,
      }),
      1,
      i,
    );
    await query(
      `INSERT INTO path_sim_population (generation, variant_id, params, updated_at)
       VALUES (1, $1, $2::jsonb, NOW())
       ON CONFLICT (generation, variant_id) DO NOTHING`,
      [variantId, JSON.stringify(seedCfg)],
    );
  }
}

async function loadCurrentGeneration(): Promise<number> {
  const state = await query<{ current_generation: number }>(`SELECT current_generation FROM path_sim_evolution_state WHERE id = 1`);
  return Number(state.rows[0]?.current_generation ?? 1);
}

async function loadVariantConfig(generation: number, variantId: string): Promise<SimConfig> {
  const row = await query<{ params: Record<string, unknown> }>(
    `SELECT params FROM path_sim_population WHERE generation = $1 AND variant_id = $2`,
    [generation, variantId],
  );
  if (row.rows.length > 0) return normalizeConfig(row.rows[0]?.params as Partial<SimConfig>);
  const fallback = normalizeConfig({
    maxSearchStates: BASE_MAX_SEARCH_STATES,
    maxCandidates: BASE_MAX_CANDIDATES,
    hopMiles: BASE_HOP_MILES,
    weakPathLossDb: BASE_WEAK_PATHLOSS_DB,
    modelBucketHours: BASE_MODEL_BUCKET_HOURS,
    windowDays: BASE_WINDOW_DAYS,
  });
  await query(
    `INSERT INTO path_sim_population (generation, variant_id, params, updated_at)
     VALUES ($1, $2, $3::jsonb, NOW())
     ON CONFLICT (generation, variant_id) DO NOTHING`,
    [generation, variantId, JSON.stringify(fallback)],
  );
  return fallback;
}

async function tryEvolveGeneration(currentGeneration: number): Promise<void> {
  const state = await query<{ current_generation: number; evolved_generation: number }>(
    `SELECT current_generation, evolved_generation FROM path_sim_evolution_state WHERE id = 1`,
  );
  const row = state.rows[0];
  if (!row) return;
  const cur = Number(row.current_generation);
  const evolved = Number(row.evolved_generation);
  if (cur !== currentGeneration || evolved >= currentGeneration) return;

  const done = await query<{ c: number }>(
    `SELECT COUNT(*)::int AS c
     FROM path_sim_population
     WHERE generation = $1
       AND fitness IS NOT NULL`,
    [currentGeneration],
  );
  const doneCount = Number(done.rows[0]?.c ?? 0);
  if (doneCount < POPULATION_SIZE) return;

  const best = await query<{ variant_id: string; params: Record<string, unknown>; fitness: number }>(
    `SELECT variant_id, params, fitness
     FROM path_sim_population
     WHERE generation = $1
     ORDER BY fitness DESC NULLS LAST, updated_at DESC
     LIMIT 1`,
    [currentGeneration],
  );
  const bestRow = best.rows[0];
  if (!bestRow) return;
  const nextGeneration = currentGeneration + 1;
  const bestParams = normalizeConfig(bestRow.params as Partial<SimConfig>);

  for (let i = 1; i <= POPULATION_SIZE; i++) {
    const variantId = `v${String(i).padStart(2, '0')}`;
    const candidate = mutateConfig(bestParams, nextGeneration, i);
    await query(
      `INSERT INTO path_sim_population (generation, variant_id, params, fitness, run_id, updated_at)
       VALUES ($1, $2, $3::jsonb, NULL, NULL, NOW())
       ON CONFLICT (generation, variant_id) DO UPDATE SET
         params = EXCLUDED.params,
         fitness = NULL,
         run_id = NULL,
         updated_at = NOW()`,
      [nextGeneration, variantId, JSON.stringify(candidate)],
    );
  }

  await query(
    `UPDATE path_sim_evolution_state
     SET current_generation = $1,
         evolved_generation = $2,
         best_variant_id = $3,
         best_fitness = $4,
         updated_at = NOW()
     WHERE id = 1`,
    [nextGeneration, currentGeneration, bestRow.variant_id, Number(bestRow.fitness ?? 0)],
  );

  console.log(`[path-sim] evolved generation ${currentGeneration} -> ${nextGeneration} winner=${bestRow.variant_id} fitness=${Number(bestRow.fitness ?? 0).toFixed(5)}`);
}

async function runOnce(
  tag: 'initial' | 'scheduled',
  generation: number,
  config: SimConfig,
  variantId: string,
): Promise<RunResult> {
  const startedAt = new Date();
  console.log(`[path-sim] ${tag} run started worker=${WORKER_ID} variant=${variantId} gen=${generation} at ${startedAt.toISOString()}`);

  const [nodes, links, model, packets] = await Promise.all([
    loadNodes(),
    loadLinks(config),
    loadLearningModel(),
    loadPackets(config.windowDays),
  ]);
  const context: RunContext = {
    byId: nodes.byId,
    byPrefix: nodes.byPrefix,
    prefixCounts: nodes.prefixCounts,
    linksByPair: links.linksByPair,
    weakAdjacency: links.weakAdjacency,
    model,
    config,
  };
  const strategies = buildStrategies(config);

  const skipReasons = new Map<string, number>();
  const byStrategy = new Map<string, StrategyStats>();
  const packetTypeTotals = new Map<string, number>();
  const eligiblePacketTypes = new Map<string, number>();
  let directNoPathResolved = 0;
  for (const strategy of strategies) byStrategy.set(strategy.name, createStrategyStats());

  for (let idx = 0; idx < packets.length; idx++) {
    const row = packets[idx]!;
    if ((idx + 1) % LOG_EVERY === 0 || idx === 0 || idx === packets.length - 1) {
      console.log(`[path-sim] packet ${idx + 1}/${packets.length} hash=${row.packet_hash}`);
    }
    bump(packetTypeTotals, String(row.packet_type ?? -1));
    const rx = row.rx_node_id ? context.byId.get(row.rx_node_id) : undefined;
    const src = row.src_node_id ? context.byId.get(row.src_node_id) : undefined;
    const hashes = row.path_hashes?.map(normalizePathHash).filter(Boolean) ?? [];
    const hops = row.hop_count != null ? hashes.slice(0, Math.max(0, row.hop_count)) : hashes;
    const isDirectNoPath = hops.length === 0 && row.hop_count === 0;
    const packetNetwork = (row.network ?? NETWORK ?? 'teesside').trim().toLowerCase() || 'teesside';
    const packetTime = new Date(row.time);

    if (!src || !rx) {
      bump(skipReasons, 'missing_src_or_rx_node');
      continue;
    }
    if (hops.length < 1 && !isDirectNoPath) {
      bump(skipReasons, 'no_path_hashes_for_multihop_or_unknown_hops');
      continue;
    }
    if (hops.length > 0 && hops.some((h) => countNodesForPathHash(context.byPrefix, h) < 1)) {
      bump(skipReasons, 'unmapped_prefix_no_logged_repeater');
      continue;
    }
    bump(eligiblePacketTypes, String(row.packet_type ?? -1));

    for (const strategy of strategies) {
      const s = byStrategy.get(strategy.name)!;
      s.packetsEligible += 1;
      if (isDirectNoPath) {
        s.totalPermutations += 1;
        s.totalBestConfidence += 1;
        bump(s.permutationHistogram, permutationBucket(1));
        bump(s.remainingHopsHistogram, '0');
        s.packetsFullyResolved += 1;
        continue;
      }
      const stats = enumerateContinuationsMl(src.node_id, hops, rx.node_id, packetTime, packetNetwork, strategy, context);
      if (stats.truncated) s.truncatedSearches += 1;
      const permutations = stats.fullCount > 0 ? stats.fullCount : stats.partialCount;
      const remainingHops = Math.max(0, hops.length - stats.longestPrefixDepth);
      s.totalPermutations += permutations;
      s.totalRemainingHops += remainingHops;
      s.totalBestConfidence += stats.bestConfidence;
      bump(s.permutationHistogram, permutationBucket(permutations));
      bump(s.remainingHopsHistogram, String(remainingHops));
      if (stats.longestPrefixDepth === 0) s.noProgressPackets += 1;
      if (stats.fullCount > 0 && remainingHops === 0) s.packetsFullyResolved += 1;
      else {
        s.packetsUnresolved += 1;
        if (permutations > 0) s.unresolvedWithPermutations += 1;
        else s.unresolvedWithoutPermutations += 1;
      }
    }
    if (isDirectNoPath) directNoPathResolved += 1;
  }

  const strategyResults = Object.fromEntries(Array.from(byStrategy.entries()).map(([name, s]) => [
    name,
    {
      packetsEligible: s.packetsEligible,
      packetsFullyResolved: s.packetsFullyResolved,
      packetsUnresolved: s.packetsUnresolved,
      unresolvedWithPermutations: s.unresolvedWithPermutations,
      unresolvedWithoutPermutations: s.unresolvedWithoutPermutations,
      truncatedSearches: s.truncatedSearches,
      noProgressPackets: s.noProgressPackets,
      avgPermutationsPerEligible: s.packetsEligible > 0 ? Number((s.totalPermutations / s.packetsEligible).toFixed(3)) : 0,
      avgRemainingHopsPerEligible: s.packetsEligible > 0 ? Number((s.totalRemainingHops / s.packetsEligible).toFixed(3)) : 0,
      avgBestConfidencePerEligible: s.packetsEligible > 0 ? Number((s.totalBestConfidence / s.packetsEligible).toFixed(3)) : 0,
      permutationHistogram: mapToObject(s.permutationHistogram),
      remainingHopsHistogram: mapToObject(s.remainingHopsHistogram),
    },
  ]));

  const baseline = byStrategy.get('ml_75mi') ?? createStrategyStats();
  const completedAt = new Date();
  const durationMs = completedAt.getTime() - startedAt.getTime();
  const summary = {
    variantId,
    generation,
    workerId: WORKER_ID,
    network: NETWORK,
    config,
    startedAt: startedAt.toISOString(),
    completedAt: completedAt.toISOString(),
    durationMs,
    packetsTotal: packets.length,
    packetsEligible: baseline.packetsEligible,
    packetsFullyResolved: baseline.packetsFullyResolved,
    packetsUnresolved: baseline.packetsUnresolved,
    unresolvedWithPermutations: baseline.unresolvedWithPermutations,
    unresolvedWithoutPermutations: baseline.unresolvedWithoutPermutations,
    truncatedSearches: baseline.truncatedSearches,
    noProgressPackets: baseline.noProgressPackets,
    avgPermutationsPerEligible: baseline.packetsEligible > 0 ? Number((baseline.totalPermutations / baseline.packetsEligible).toFixed(3)) : 0,
    avgRemainingHopsPerEligible: baseline.packetsEligible > 0 ? Number((baseline.totalRemainingHops / baseline.packetsEligible).toFixed(3)) : 0,
    avgBestConfidencePerEligible: baseline.packetsEligible > 0 ? Number((baseline.totalBestConfidence / baseline.packetsEligible).toFixed(3)) : 0,
    permutationHistogram: mapToObject(baseline.permutationHistogram),
    remainingHopsHistogram: mapToObject(baseline.remainingHopsHistogram),
    packetTypeTotals: mapToObject(packetTypeTotals),
    eligiblePacketTypes: mapToObject(eligiblePacketTypes),
    directNoPathResolved,
    skipReasons: mapToObject(skipReasons),
    strategyResults,
  };

  const insert = await query<{ id: number }>(
    `INSERT INTO path_simulation_runs
       (started_at, completed_at, network, packets_total, packets_eligible, packets_fully_resolved,
        packets_unresolved, truncated_searches, permutation_histogram, remaining_hops_histogram, summary)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb)
     RETURNING id`,
    [
      startedAt.toISOString(),
      completedAt.toISOString(),
      NETWORK,
      packets.length,
      baseline.packetsEligible,
      baseline.packetsFullyResolved,
      baseline.packetsUnresolved,
      baseline.truncatedSearches,
      JSON.stringify(summary.permutationHistogram),
      JSON.stringify(summary.remainingHopsHistogram),
      JSON.stringify(summary),
    ],
  );
  const runId = Number(insert.rows[0]?.id ?? 0);
  summary.variantId = variantId;
  console.log(`[path-sim] ${tag} summary worker=${WORKER_ID} variant=${variantId} gen=${generation}:`, JSON.stringify(summary));
  return {
    summary: {
      packetsEligible: summary.packetsEligible,
      packetsFullyResolved: summary.packetsFullyResolved,
      packetsUnresolved: summary.packetsUnresolved,
      avgPermutationsPerEligible: summary.avgPermutationsPerEligible,
      avgRemainingHopsPerEligible: summary.avgRemainingHopsPerEligible,
      avgBestConfidencePerEligible: summary.avgBestConfidencePerEligible,
    },
    runId,
  };
}

async function runCycle(tag: 'initial' | 'scheduled'): Promise<void> {
  await ensureEvolutionSeed();
  const generation = await loadCurrentGeneration();
  for (let i = 1; i <= POPULATION_SIZE; i++) {
    const variantId = `v${String(i).padStart(2, '0')}`;
    const config = await loadVariantConfig(generation, variantId);
    console.log(
      `[path-sim] configuration worker=${WORKER_ID} variant=${variantId} gen=${generation} network=${NETWORK} ` +
      `windowDays=${config.windowDays} hopMiles=${config.hopMiles} maxStates=${config.maxSearchStates} ` +
      `maxCandidates=${config.maxCandidates} weakPathLoss=${config.weakPathLossDb} bucketHours=${config.modelBucketHours}`,
    );
    const result = await runOnce(tag, generation, config, variantId);
    const fitness = runFitness(result.summary);
    await query(
      `UPDATE path_sim_population
       SET fitness = $3, run_id = $4, params = $5::jsonb, updated_at = NOW()
       WHERE generation = $1 AND variant_id = $2`,
      [generation, variantId, fitness, result.runId || null, JSON.stringify(config)],
    );
    console.log(`[path-sim] fitness worker=${WORKER_ID} variant=${variantId} gen=${generation} value=${fitness.toFixed(5)}`);
  }
  await tryEvolveGeneration(generation);
}

async function main() {
  await initDb();
  await runCycle('initial');
  setInterval(() => {
    void runCycle('scheduled').catch((err) => {
      console.error('[path-sim] scheduled run failed:', (err as Error).message);
    });
  }, RUN_INTERVAL_MS);
}

main().catch((err) => {
  console.error('[path-sim] fatal startup error:', err);
  process.exit(1);
});
