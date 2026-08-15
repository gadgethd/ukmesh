import {
  getPublicVisibilityGeneration,
  namedQuery,
  query,
  touchNodesPredictedOnline,
} from '../db/index.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { privateNodePacketNetworkMatchSql } from '../privacy/networkScope.js';
import { normalizePathHash, nodePathHash } from '../path-hash/utils.js';
import type { DecodedHop } from '../path-core/decoder.js';
import {
  BETA_PURPLE_THRESHOLD,
  CONTEXT_TTL_MS,
  MAX_BETA_HOPS,
  MODEL_LIMIT,
} from './constants.js';
import { hasCoords, linkKey } from './geometry.js';
import {
  decodeBetaCanonicalGroup,
  decodeBetaCanonicalGroupWithHeldPath,
  groupCompatibleObservations,
  mergeCompatiblePathHashes,
  projectCanonicalPathForObserver,
  type BetaObserverEntry,
  type BetaPathProjection,
  type BetaSharedDecode,
} from './sharedDecoder.js';
import type {
  BetaResolveContext,
  LinkMetrics,
  MeshNode,
  PathLearningModel,
  PathPacket,
} from './types.js';
import type { HeldPathEntry } from './resolveCache.js';

const contextCache = new BoundedTtlMap<string, BetaResolveContext>({
  name: 'path_context',
  maxEntries: 16,
  maxWeight: 128 * 1024 * 1024,
  ttlMs: CONTEXT_TTL_MS,
});
const PATH_MULTI_HISTORY_WINDOW_HOURS = Math.min(
  24 * 365,
  Math.max(1, Number(process.env['PATH_MULTI_HISTORY_WINDOW_HOURS'] ?? 168) || 168),
);
const PATH_MULTI_MAX_SCAN_ROWS = Math.min(
  10_000,
  Math.max(32, Number(process.env['PATH_MULTI_MAX_SCAN_ROWS'] ?? 2_048) || 2_048),
);
const PATH_MULTI_MAX_OBSERVERS = Math.min(
  512,
  Math.max(1, Number(process.env['PATH_MULTI_MAX_OBSERVERS'] ?? 128) || 128),
);

function edgeKey(receiverRegion: string, bucket: number, fromId: string, toId: string): string {
  return `${receiverRegion}|${bucket}|${fromId}|${toId}`;
}

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
    const group = `${row.receiver_region}|${prefix}|${previous}`;
    const choice = `${group}|${row.node_id}`;
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
  const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
    query<PathPrefixPriorEvidence>(
      `SELECT prefix, receiver_region, prev_prefix, node_id, count
       FROM path_prefix_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
      signal,
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
       FROM path_edge_priors
       WHERE network = $1
       ORDER BY score DESC, observed_count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
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
       FROM path_motif_priors
       WHERE network = $1
       ORDER BY count DESC
       LIMIT $2`,
      [network, MODEL_LIMIT],
      signal,
    ),
    query<{ confidence_scale: number; confidence_bias: number }>(
      `SELECT confidence_scale, confidence_bias
       FROM path_model_calibration
       WHERE network = $1`,
      [network],
      signal,
    ),
  ]);

  const prefixProbabilities = buildOneBytePrefixProbabilities(prefixRows.rows);

  const transitionProbabilities = new Map<string, number>();
  for (const row of transitionRows.rows) {
    transitionProbabilities.set(
      `${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`,
      Number(row.probability),
    );
  }

  const edgeScores = new Map<string, number>();
  const edgeTotals = new Map<string, { count: number; sum: number }>();
  for (const row of edgeRows.rows) {
    const score = Number(row.score);
    edgeScores.set(edgeKey(row.receiver_region, Number(row.hour_bucket), row.from_node_id, row.to_node_id), score);
    const aggregateKey = `${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`;
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
      `${row.receiver_region}|${Number(row.hour_bucket)}|${Number(row.motif_len)}|${row.node_ids}`,
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
    motifProbabilities.set(`${region}|-1|${motifLength}|${nodeIds}`, aggregate.sum / aggregate.count);
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

export function canReusePathContext(input: {
  cachedVisibilityGeneration: number;
  currentVisibilityGeneration: number;
  ageMs: number;
  pinForBatch: boolean;
}): boolean {
  return input.cachedVisibilityGeneration === input.currentVisibilityGeneration
    && (input.pinForBatch || input.ageMs < CONTEXT_TTL_MS);
}

async function loadContext(
  network: string,
  visibilityRetry = 0,
  options?: {
    pinForBatch?: boolean;
    requiredVisibilityGeneration?: number;
    currentVisibilityGeneration?: number;
    signal?: AbortSignal;
  },
): Promise<BetaResolveContext> {
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
       FROM nodes
       WHERE ($1 = 'all' OR network = $1)
         AND (name IS NULL OR name NOT LIKE '%🚫%')`,
      [network],
      options?.signal,
    ),
    query<{
      node_a_id: string;
      node_b_id: string;
      observed_count: number;
      multibyte_observed_count: number;
      itm_path_loss_db: number | null;
      itm_viable: boolean | null;
      count_a_to_b: number | null;
      count_b_to_a: number | null;
    }>(
      `SELECT nl.node_a_id, nl.node_b_id, nl.observed_count, nl.multibyte_observed_count,
              nl.itm_path_loss_db, nl.itm_viable, nl.count_a_to_b, nl.count_b_to_a
       FROM node_links nl
       JOIN nodes a ON a.node_id = nl.node_a_id
       JOIN nodes b ON b.node_id = nl.node_b_id
       WHERE (nl.itm_viable IS NOT NULL OR nl.force_viable = true)
         AND (a.name IS NULL OR a.name NOT LIKE '%🚫%')
         AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')
         AND ($1 = 'all' OR (a.network = $1 AND b.network = $1))`,
      [network],
      options?.signal,
    ),
    buildLearningModel(network, options?.signal),
  ]);
  options?.signal?.throwIfAborted();

  const nodesById = new Map<string, MeshNode>();
  for (const row of nodeRows.rows) nodesById.set(row.node_id, row);

  const linkMetrics = new Map<string, LinkMetrics>();
  for (const row of linkRows.rows) {
    linkMetrics.set(linkKey(row.node_a_id, row.node_b_id), {
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
    return loadContext(network, visibilityRetry + 1, options);
  }
  contextCache.set(network, context);
  return context;
}

export type PreparedPacketObservation = {
  packet: PathPacket;
  rx: MeshNode | null;
  hashes: string[];
  rawHops: string[];
  hops: string[];
  ignoreForPathing: boolean;
};

function matchesObserverPathHash(rx: MeshNode | null, hash: string | undefined): boolean {
  if (!rx || !hash) return false;
  const normalized = normalizePathHash(hash);
  return Boolean(normalized && nodePathHash(rx.node_id, normalized) === normalized);
}

function trimObserverTerminalHop(hops: string[], rx: MeshNode | null): string[] {
  if (!rx || rx.role !== 2 || hops.length <= 1) return hops;
  return matchesObserverPathHash(rx, hops[hops.length - 1]) ? hops.slice(0, -1) : hops;
}

function preparePacketObservation(packet: PathPacket, rx: MeshNode | null): PreparedPacketObservation {
  const hashes = (packet.path_hashes ?? []).map(normalizePathHash).filter(Boolean);
  const expectedHexLength = packet.path_hash_size_bytes == null ? null : packet.path_hash_size_bytes * 2;
  const validatedHashes = expectedHexLength == null
    ? hashes
    : hashes.filter((hash) => hash.length === expectedHexLength);
  const rawHops = packet.hop_count == null
    ? validatedHashes
    : validatedHashes.slice(0, Math.max(0, packet.hop_count));
  const ignoreForPathing = Boolean(
    rx?.role === 2
    && rawHops.length >= 3
    && matchesObserverPathHash(rx, rawHops[0])
    && matchesObserverPathHash(rx, rawHops[rawHops.length - 1]),
  );
  return {
    packet,
    rx,
    hashes,
    rawHops,
    hops: trimObserverTerminalHop(rawHops, rx),
    ignoreForPathing,
  };
}

function compareCanonicalObservation(a: PreparedPacketObservation, b: PreparedPacketObservation): number {
  return Number(b.packet.path_hash_size_bytes ?? 0) - Number(a.packet.path_hash_size_bytes ?? 0)
    || b.hops.length - a.hops.length
    || b.rawHops.length - a.rawHops.length
    || Number(Boolean(b.packet.src_node_id)) - Number(Boolean(a.packet.src_node_id))
    || Number(b.packet.hop_count ?? 0) - Number(a.packet.hop_count ?? 0);
}

function comparePreferredObservation(a: PreparedPacketObservation, b: PreparedPacketObservation): number {
  return b.hops.length - a.hops.length
    || Number(b.packet.path_hash_size_bytes ?? 0) - Number(a.packet.path_hash_size_bytes ?? 0)
    || Number(Boolean(b.packet.src_node_id)) - Number(Boolean(a.packet.src_node_id))
    || a.rawHops.length - b.rawHops.length
    || Number(a.packet.hop_count ?? Number.MAX_SAFE_INTEGER)
      - Number(b.packet.hop_count ?? Number.MAX_SAFE_INTEGER);
}

export function mergeObserverEvidence(
  previous: PreparedPacketObservation,
  current: PreparedPacketObservation,
): PreparedPacketObservation {
  const mergedHops = mergeCompatiblePathHashes(previous.hops, current.hops);
  if (!mergedHops) {
    return compareCanonicalObservation(current, previous) < 0 ? current : previous;
  }
  const preferred = comparePreferredObservation(current, previous) < 0 ? current : previous;
  return { ...preferred, hops: mergedHops };
}

function toObserverEntry(prepared: PreparedPacketObservation): BetaObserverEntry | null {
  if (!hasCoords(prepared.rx)) return null;
  return {
    observerId: prepared.packet.rx_node_id ?? prepared.rx.node_id,
    packet: prepared.packet,
    rx: prepared.rx,
    hashes: prepared.hashes,
    hops: prepared.hops,
  };
}

function findSharedPathPrefix(pathsByObserver: readonly string[][]): string[] {
  if (pathsByObserver.length === 0) return [];
  const first = pathsByObserver[0]!;
  let length = first.length;
  for (let index = 1; index < pathsByObserver.length; index++) {
    const other = pathsByObserver[index]!;
    length = Math.min(length, other.length);
    for (let position = 0; position < length; position++) {
      if (normalizePathHash(first[position]) !== normalizePathHash(other[position])) {
        length = position;
        break;
      }
    }
  }
  return first.slice(0, length);
}

export type BetaCanonicalPathNode = {
  position: number;
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  ambiguous: boolean;
  confidence: number | null;
};

export type BetaPathObserver = { observerId: string };

export type BetaResolvedPayload = {
  ok: boolean;
  packetHash: string;
  mode: 'resolved' | 'none';
  confidence: number | null;
  canonicalPath: BetaCanonicalPathNode[];
  observers: BetaPathObserver[];
  network: string;
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

function roundCoord(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function hiddenCoordKey(lat: number, lon: number): string {
  return `${roundCoord(lat)},${roundCoord(lon)}`;
}

function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let index = 0; index < input.length; index++) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function stablePointWithinMiles(
  lat: number,
  lon: number,
  seed: string,
  radiusMiles = HIDDEN_NODE_MASK_RADIUS_MILES,
): [number, number] {
  const radiusKm = radiusMiles * 1.609344;
  const distanceUnit = hashSeed(`${seed}:distance`) / 0xffffffff;
  const bearingUnit = hashSeed(`${seed}:bearing`) / 0xffffffff;
  const distanceKm = Math.sqrt(distanceUnit) * radiusKm;
  const bearing = bearingUnit * Math.PI * 2;
  const latitudeRadians = lat * (Math.PI / 180);
  return [
    lat + (distanceKm / 111) * Math.cos(bearing),
    lon + (distanceKm / (111 * Math.max(0.01, Math.cos(latitudeRadians)))) * Math.sin(bearing),
  ];
}

function buildHiddenCoordMask(nodesById: ReadonlyMap<string, MeshNode>): Map<string, [number, number]> {
  const mask = new Map<string, [number, number]>();
  for (const node of nodesById.values()) {
    if (!hasCoords(node) || !node.name?.includes(PROHIBITED_NODE_MARKER)) continue;
    const seed = `${node.node_id}|${node.last_seen ?? 'unknown'}`;
    mask.set(hiddenCoordKey(node.lat!, node.lon!), stablePointWithinMiles(node.lat!, node.lon!, seed));
  }
  return mask;
}

function maskPath(
  path: [number, number][] | null,
  hiddenCoordMask: ReadonlyMap<string, [number, number]>,
): [number, number][] | null {
  if (!path || hiddenCoordMask.size === 0) return path;
  return path.map((point) => hiddenCoordMask.get(hiddenCoordKey(point[0], point[1])) ?? point);
}

function buildCanonicalPath(decoded: BetaSharedDecode): BetaCanonicalPathNode[] {
  const nodes: BetaCanonicalPathNode[] = [];
  for (let position = 0; position < decoded.canonicalHashes.length; position++) {
    const hop = decoded.hops.get(position);
    const confidence = decoded.hopConfidences.get(position) ?? null;
    nodes.push({
      position,
      hash: decoded.canonicalHashes[position] ?? hop?.hash ?? '',
      nodeId: hop?.nodeId ?? null,
      name: hop?.name ?? null,
      lat: hop?.lat ?? null,
      lon: hop?.lon ?? null,
      ambiguous: hop?.ambiguous ?? (confidence == null || confidence < BETA_PURPLE_THRESHOLD),
      confidence,
    });
  }
  return nodes;
}

function maskResolvedPayload(
  payload: BetaResolvedPayload,
  hiddenCoordMask: ReadonlyMap<string, [number, number]>,
): BetaResolvedPayload {
  if (hiddenCoordMask.size === 0) return payload;
  return {
    ...payload,
    purplePath: maskPath(payload.purplePath, hiddenCoordMask),
    extraPurplePaths: payload.extraPurplePaths.map((path) => maskPath(path, hiddenCoordMask) ?? path),
  };
}

export type PathResolutionOptions = {
  touchPredictedOnline?: boolean;
  log?: boolean;
  pinContextForBatch?: boolean;
  requiredVisibilityGeneration?: number;
  signal?: AbortSignal;
  heldPath?: HeldPathEntry;
};

function throwIfResolutionAborted(options?: PathResolutionOptions): void {
  options?.signal?.throwIfAborted();
}

function shouldTouchPredictedOnline(options?: PathResolutionOptions): boolean {
  return options?.touchPredictedOnline !== false;
}

function logPathResolution(options: PathResolutionOptions | undefined, message: string): void {
  if (options?.log !== false) console.log(message);
}

async function recordPredictedOnline(nodeIds: readonly string[]): Promise<void> {
  if (nodeIds.length > 0) await touchNodesPredictedOnline([...new Set(nodeIds)]);
}

function emptyPayload(
  packetHash: string,
  packet: PathPacket,
  hopsRequested: number,
  remainingHops: number | null,
): BetaResolvedPayload {
  return {
    ok: true,
    packetHash,
    mode: 'none',
    confidence: null,
    canonicalPath: [],
    observers: [],
    network: '',
    permutationCount: 0,
    remainingHops,
    purplePath: null,
    extraPurplePaths: [],
    redPath: null,
    redSegments: [],
    completionPaths: [],
    threshold: BETA_PURPLE_THRESHOLD,
    debug: {
      hopsRequested,
      hopsUsed: 0,
      rxNodeId: packet.rx_node_id,
      srcNodeId: packet.src_node_id,
      computedAt: new Date().toISOString(),
    },
  };
}

function payloadFromProjection(
  packetHash: string,
  entry: BetaObserverEntry,
  projection: BetaPathProjection,
  canonical: { canonicalPath: BetaCanonicalPathNode[]; observers: BetaPathObserver[]; network: string },
): BetaResolvedPayload {
  if (!projection.purplePath) {
    return emptyPayload(packetHash, entry.packet, entry.hashes.length, projection.remainingHops);
  }
  return {
    ok: true,
    packetHash,
    mode: 'resolved',
    confidence: projection.confidence,
    canonicalPath: canonical.canonicalPath,
    observers: canonical.observers,
    network: canonical.network,
    permutationCount: 0,
    remainingHops: projection.remainingHops,
    purplePath: projection.purplePath,
    extraPurplePaths: projection.extraPurplePaths,
    redPath: null,
    redSegments: [],
    completionPaths: [],
    threshold: BETA_PURPLE_THRESHOLD,
    debug: {
      hopsRequested: entry.hashes.length,
      hopsUsed: projection.resolvedHopCount,
      rxNodeId: entry.observerId,
      srcNodeId: entry.packet.src_node_id,
      computedAt: new Date().toISOString(),
    },
  };
}

export async function resolveBetaPathForPacketHash(
  packetHash: string,
  network: string,
  observer?: string,
  stickyMap?: Map<string, string>,
  stickyAgeFraction?: number,
  options?: PathResolutionOptions,
): Promise<BetaResolvedPayload | null> {
  throwIfResolutionAborted(options);
  const packetResult = await query<PathPacket>(
    `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, path_hash_size_bytes
     FROM packets
     WHERE packet_hash = $1
       AND ($2 = 'all' OR network = $2)
       ${observer ? 'AND rx_node_id = $3' : ''}
       AND NOT EXISTS (
         SELECT 1 FROM nodes private_node
         WHERE private_node.name LIKE '%🚫%'
           AND ${privateNodePacketNetworkMatchSql('private_node', 'packets')}
           AND (
             private_node.node_id IN (packets.rx_node_id, packets.src_node_id)
             OR EXISTS (
               SELECT 1
               FROM unnest(COALESCE(packets.path_hashes, ARRAY[]::text[])) AS path_hash
               WHERE packets.path_hash_size_bytes BETWEEN 1 AND 3
                 AND UPPER(private_node.node_id) LIKE UPPER(path_hash) || '%'
             )
           )
       )
     ORDER BY COALESCE(cardinality(path_hashes), 0) DESC,
              COALESCE(path_hash_size_bytes, 0) DESC,
              CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
              hop_count ASC NULLS LAST,
              time ASC
     LIMIT 32`,
    observer ? [packetHash, network, observer] : [packetHash, network],
    options?.signal,
  );
  if (packetResult.rows.length === 0) return null;

  const context = await loadContext(network, 0, {
    pinForBatch: options?.pinContextForBatch,
    requiredVisibilityGeneration: options?.requiredVisibilityGeneration,
    signal: options?.signal,
  });
  throwIfResolutionAborted(options);

  const preparedByObserver = new Map<string, PreparedPacketObservation>();
  for (const row of packetResult.rows) {
    const rx = row.rx_node_id ? context.nodesById.get(row.rx_node_id) ?? null : null;
    const prepared = preparePacketObservation(row, rx);
    if (prepared.ignoreForPathing) continue;
    const key = row.rx_node_id ?? '__no_observer__';
    const previous = preparedByObserver.get(key);
    preparedByObserver.set(key, previous ? mergeObserverEvidence(previous, prepared) : prepared);
  }
  const selected = [...preparedByObserver.values()].sort(comparePreferredObservation)[0];
  if (!selected) return null;
  const hiddenCoordMask = buildHiddenCoordMask(context.nodesById);
  const convertedSelectedEntry = toObserverEntry(selected);
  if (!convertedSelectedEntry
      || convertedSelectedEntry.hops.length === 0
      || convertedSelectedEntry.hops.length >= MAX_BETA_HOPS) {
    const result = emptyPayload(packetHash, selected.packet, selected.hashes.length, selected.hops.length);
    logPathResolution(options, `[path-beta] hash=${packetHash} network=${network} mode=none reason=no-decodable-hops`);
    return maskResolvedPayload(result, hiddenCoordMask);
  }

  const entries = [...preparedByObserver.values()]
    .map(toObserverEntry)
    .filter((entry): entry is BetaObserverEntry => Boolean(entry));
  const selectedEntry = entries.find((entry) => entry.packet === selected.packet) ?? convertedSelectedEntry;
  const group = groupCompatibleObservations(entries)
    .find((candidate) => candidate.members.includes(selectedEntry))
    ?? { canonicalHashes: selectedEntry.hops, members: [selectedEntry] };
  const decoded = decodeBetaCanonicalGroupWithHeldPath(
    group,
    context,
    options?.heldPath?.path,
    stickyMap,
    stickyAgeFraction,
  );
  const source = selectedEntry.packet.src_node_id
    ? context.nodesById.get(selectedEntry.packet.src_node_id) ?? null
    : null;
  const projection = projectCanonicalPathForObserver(selectedEntry, decoded, context, source);
  const payload = payloadFromProjection(packetHash, selectedEntry, projection, {
    canonicalPath: buildCanonicalPath(decoded),
    observers: [{ observerId: selectedEntry.observerId }],
    network,
  });
  payload.permutationCount = decoded.held ? Math.min(2, decoded.canonicalHashes.length) : 0;
  if (payload.mode === 'resolved' && shouldTouchPredictedOnline(options)) {
    await recordPredictedOnline(projection.nodeIds);
  }
  logPathResolution(
    options,
    `[path-beta] hash=${packetHash} network=${network} mode=${payload.mode} decoder=shared `
      + `resolvedHops=${projection.resolvedHopCount}/${selectedEntry.hops.length} `
      + `confidence=${payload.confidence?.toFixed(3) ?? 'null'}`,
  );
  return maskResolvedPayload(payload, hiddenCoordMask);
}

export type RegionLink = {
  fromIata: string;
  toIata: string;
  bridgeCoord: [number, number];
};

export type MultiObserverResolvedPayload = {
  ok: boolean;
  packetHash: string;
  network: string;
  observerCount: number;
  sharedPrefixLength: number;
  canonicalPath: BetaCanonicalPathNode[];
  observers: BetaPathObserver[];
  confidence: number | null;
  results: BetaResolvedPayload[];
  regionLinks?: RegionLink[];
  stickyUpdates?: Record<string, string>;
};

function addStickyUpdate(
  updates: Record<string, string>,
  conflicts: Set<string>,
  hash: string,
  nodeId: string,
): void {
  if (conflicts.has(hash)) return;
  const previous = updates[hash];
  if (previous && previous !== nodeId) {
    delete updates[hash];
    conflicts.add(hash);
    return;
  }
  updates[hash] = nodeId;
}

function buildRegionLinks(
  entries: readonly BetaObserverEntry[],
  decodes: ReadonlyMap<BetaObserverEntry, BetaSharedDecode>,
): RegionLink[] {
  if (entries.length < 2) return [];
  const anchor = entries.reduce((best, entry) => entry.hops.length > best.hops.length ? entry : best);
  const anchorRegion = anchor.rx.iata ?? 'unknown';
  const decoded = decodes.get(anchor);
  if (!decoded) return [];
  // Retain a request-local lookup memo for the cache-policy registry while
  // scanning a sparse decode from the observer side.
  const hopCache = new Map<number, DecodedHop | undefined>();
  const getHop = (position: number): DecodedHop | undefined => {
    if (!hopCache.has(position)) hopCache.set(position, decoded.hops.get(position));
    return hopCache.get(position);
  };
  let bridgeCoord: [number, number] | null = null;
  for (let position = anchor.hops.length - 1; position >= 0; position--) {
    const hop = getHop(position);
    if (hop?.lat != null && hop.lon != null) {
      bridgeCoord = [hop.lat, hop.lon];
      break;
    }
  }
  if (!bridgeCoord) return [];
  const linkedRegions = new Set<string>();
  const links: RegionLink[] = [];
  for (const entry of entries) {
    const region = entry.rx.iata ?? 'unknown';
    if (region === anchorRegion || linkedRegions.has(region)) continue;
    linkedRegions.add(region);
    links.push({ fromIata: anchorRegion, toIata: region, bridgeCoord });
  }
  return links;
}

async function resolveMultiObserverBetaPathFromRows(
  packetHash: string,
  network: string,
  rows: readonly PathPacket[],
  context: BetaResolveContext,
  stickyMap?: Map<string, string>,
  stickyAgeFraction?: number,
  options?: PathResolutionOptions,
): Promise<MultiObserverResolvedPayload | null> {
  const byObserver = new Map<string, PreparedPacketObservation>();
  for (const row of rows) {
    if (!row.rx_node_id) continue;
    const prepared = preparePacketObservation(row, context.nodesById.get(row.rx_node_id) ?? null);
    if (prepared.ignoreForPathing) continue;
    const previous = byObserver.get(row.rx_node_id);
    byObserver.set(row.rx_node_id, previous ? mergeObserverEvidence(previous, prepared) : prepared);
  }
  if (byObserver.size > PATH_MULTI_MAX_OBSERVERS) throw new Error('PATH_HISTORY_LIMIT');

  const entries = [...byObserver.values()]
    .map(toObserverEntry)
    .filter((entry): entry is BetaObserverEntry => (
      entry !== null && entry.hops.length > 0 && entry.hops.length < MAX_BETA_HOPS
    ));
  if (entries.length === 0) return null;

  const groups = groupCompatibleObservations(entries);
  const hiddenCoordMask = buildHiddenCoordMask(context.nodesById);
  const results: BetaResolvedPayload[] = [];
  let firstCanonicalPath: BetaCanonicalPathNode[] = [];
  const predictedOnlineNodeIds = new Set<string>();
  const stickyUpdates: Record<string, string> = {};
  const stickyConflicts = new Set<string>();
  const decodesByEntry = new Map<BetaObserverEntry, BetaSharedDecode>();

  for (const group of groups) {
    throwIfResolutionAborted(options);
    const heldStickyMap = new Map(stickyMap ?? []);
    if (options?.heldPath?.path.length === group.canonicalHashes.length) {
      for (let position = 0; position < group.canonicalHashes.length; position++) {
        const hash = normalizePathHash(group.canonicalHashes[position]);
        const nodeId = options.heldPath.path[position];
        if (hash && nodeId) heldStickyMap.set(hash, nodeId);
      }
    }
    const decoded = decodeBetaCanonicalGroup(
      group,
      context,
      heldStickyMap.size > 0 ? heldStickyMap : undefined,
      stickyAgeFraction,
    );
    if (firstCanonicalPath.length === 0) firstCanonicalPath = buildCanonicalPath(decoded);
    for (let position = 0; position < decoded.canonicalHashes.length; position++) {
      const hop = decoded.hops.get(position);
      const confidence = decoded.hopConfidences.get(position) ?? 0;
      if (!hop?.nodeId || confidence < BETA_PURPLE_THRESHOLD) continue;
      addStickyUpdate(stickyUpdates, stickyConflicts, decoded.canonicalHashes[position]!, hop.nodeId);
    }

    for (const entry of group.members) {
      decodesByEntry.set(entry, decoded);
      const source = entry.packet.src_node_id
        ? context.nodesById.get(entry.packet.src_node_id) ?? null
        : null;
      const projection = projectCanonicalPathForObserver(entry, decoded, context, source);
      const payload = payloadFromProjection(packetHash, entry, projection, {
        canonicalPath: buildCanonicalPath(decoded),
        observers: group.members.map((member) => ({ observerId: member.observerId })),
        network,
      });
      results.push(maskResolvedPayload(payload, hiddenCoordMask));
      if (payload.mode === 'resolved') {
        for (const nodeId of projection.nodeIds) predictedOnlineNodeIds.add(nodeId);
      }
    }
  }

  if (shouldTouchPredictedOnline(options)) await recordPredictedOnline([...predictedOnlineNodeIds]);
  const sharedPrefixLength = entries.length <= 1
    ? 0
    : findSharedPathPrefix(entries.map((entry) => entry.hops)).length;
  const regionLinks = buildRegionLinks(entries, decodesByEntry);
  const resolvedCount = results.filter((result) => result.mode === 'resolved').length;
  logPathResolution(
    options,
    `[path-beta-multi] hash=${packetHash} network=${network} decoder=shared `
      + `groups=${groups.length} observers=${entries.length} resolved=${resolvedCount}/${entries.length} `
      + `sharedPrefix=${sharedPrefixLength}`,
  );
  return {
    ok: true,
    packetHash,
    network,
    observerCount: entries.length,
    sharedPrefixLength,
    canonicalPath: firstCanonicalPath,
    observers: entries.map((entry) => ({ observerId: entry.observerId })),
    confidence: results[0]?.confidence ?? null,
    results,
    ...(regionLinks.length > 0 ? { regionLinks } : {}),
    ...(Object.keys(stickyUpdates).length > 0 ? { stickyUpdates } : {}),
  };
}

export async function resolveMultiObserverBetaPath(
  packetHash: string,
  network: string,
  stickyMap?: Map<string, string>,
  stickyAgeFraction?: number,
  options?: PathResolutionOptions,
): Promise<MultiObserverResolvedPayload | null> {
  throwIfResolutionAborted(options);
  const allResult = await query<PathPacket>(
    `SELECT packet_hash, rx_node_id, src_node_id, packet_type, hop_count, path_hashes, path_hash_size_bytes
     FROM packets
     WHERE packet_hash = $1
       AND ($2 = 'all' OR network = $2)
       AND time >= NOW() - ($3::int * INTERVAL '1 hour')
       AND rx_node_id IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM nodes private_node
         WHERE private_node.name LIKE '%🚫%'
           AND ${privateNodePacketNetworkMatchSql('private_node', 'packets')}
           AND (
             private_node.node_id IN (packets.rx_node_id, packets.src_node_id)
             OR EXISTS (
               SELECT 1
               FROM unnest(COALESCE(packets.path_hashes, ARRAY[]::text[])) AS path_hash
               WHERE packets.path_hash_size_bytes BETWEEN 1 AND 3
                 AND UPPER(private_node.node_id) LIKE UPPER(path_hash) || '%'
             )
           )
       )
     ORDER BY COALESCE(cardinality(path_hashes), 0) DESC,
              COALESCE(path_hash_size_bytes, 0) DESC,
              CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
              hop_count ASC NULLS LAST,
              time ASC
     LIMIT $4`,
    [packetHash, network, PATH_MULTI_HISTORY_WINDOW_HOURS, PATH_MULTI_MAX_SCAN_ROWS + 1],
    options?.signal,
  );
  if (allResult.rows.length === 0) return null;
  if (allResult.rows.length > PATH_MULTI_MAX_SCAN_ROWS) throw new Error('PATH_HISTORY_LIMIT');

  const context = await loadContext(network, 0, {
    pinForBatch: options?.pinContextForBatch,
    requiredVisibilityGeneration: options?.requiredVisibilityGeneration,
    signal: options?.signal,
  });
  throwIfResolutionAborted(options);
  return resolveMultiObserverBetaPathFromRows(
    packetHash,
    network,
    allResult.rows,
    context,
    stickyMap,
    stickyAgeFraction,
    options,
  );
}

export type MultiObserverPathBatch = {
  results: Map<string, MultiObserverResolvedPayload | null>;
  limitedPacketHashes: Set<string>;
};

export async function createMultiObserverPathBatchResolver(
  network: string,
  visibilityGeneration: number,
  signal?: AbortSignal,
): Promise<{
  resolveBatch: (
    packetHashes: readonly string[],
    windowStart: Date,
    windowEnd: Date,
    signal?: AbortSignal,
  ) => Promise<MultiObserverPathBatch>;
}> {
  const context = await loadContext(network, 0, {
    pinForBatch: true,
    requiredVisibilityGeneration: visibilityGeneration,
    currentVisibilityGeneration: visibilityGeneration,
    signal,
  });

  return {
    resolveBatch: async (packetHashes, windowStart, windowEnd, batchSignal) => {
      batchSignal?.throwIfAborted();
      const results = new Map<string, MultiObserverResolvedPayload | null>();
      const limitedPacketHashes = new Set<string>();
      if (packetHashes.length === 0) return { results, limitedPacketHashes };

      const packetResult = await namedQuery<PathPacket>(
        'path-history-observations-v1',
        `SELECT observation.packet_hash, observation.rx_node_id, observation.src_node_id,
                observation.packet_type, observation.hop_count, observation.path_hashes,
                observation.path_hash_size_bytes
           FROM unnest($1::text[]) WITH ORDINALITY AS requested(packet_hash, selection_order)
           CROSS JOIN LATERAL (
             SELECT p.packet_hash, p.rx_node_id, p.src_node_id, p.packet_type,
                    p.hop_count, p.path_hashes, p.path_hash_size_bytes, p.time
               FROM packets p
              WHERE p.packet_hash = requested.packet_hash
                AND ($2 = 'all' OR p.network = $2)
                AND p.time >= $3
                AND p.time <= $4
                AND p.rx_node_id IS NOT NULL
                AND NOT EXISTS (
                  SELECT 1 FROM nodes private_node
                   WHERE private_node.name LIKE '%🚫%'
                     AND ${privateNodePacketNetworkMatchSql('private_node', 'p')}
                     AND (
                       private_node.node_id IN (p.rx_node_id, p.src_node_id)
                       OR EXISTS (
                         SELECT 1
                           FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS path_hash
                          WHERE p.path_hash_size_bytes BETWEEN 1 AND 3
                            AND UPPER(private_node.node_id) LIKE UPPER(path_hash) || '%'
                       )
                     )
                )
              ORDER BY COALESCE(cardinality(p.path_hashes), 0) DESC,
                       COALESCE(p.path_hash_size_bytes, 0) DESC,
                       CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                       p.hop_count ASC NULLS LAST,
                       p.time ASC
              LIMIT $5
           ) observation
          ORDER BY requested.selection_order,
                   COALESCE(cardinality(observation.path_hashes), 0) DESC,
                   COALESCE(observation.path_hash_size_bytes, 0) DESC,
                   CASE WHEN observation.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                   observation.hop_count ASC NULLS LAST,
                   observation.time ASC`,
        [packetHashes, network, windowStart, windowEnd, PATH_MULTI_MAX_SCAN_ROWS + 1],
        batchSignal,
      );
      batchSignal?.throwIfAborted();

      const rowsByHash = new Map<string, PathPacket[]>();
      for (const row of packetResult.rows) {
        const rows = rowsByHash.get(row.packet_hash) ?? [];
        rows.push(row);
        rowsByHash.set(row.packet_hash, rows);
      }
      for (const packetHash of packetHashes) {
        batchSignal?.throwIfAborted();
        const rows = rowsByHash.get(packetHash) ?? [];
        if (rows.length > PATH_MULTI_MAX_SCAN_ROWS) {
          limitedPacketHashes.add(packetHash);
          continue;
        }
        try {
          results.set(packetHash, await resolveMultiObserverBetaPathFromRows(
            packetHash,
            network,
            rows,
            context,
            undefined,
            undefined,
            {
              touchPredictedOnline: false,
              log: false,
              pinContextForBatch: true,
              requiredVisibilityGeneration: visibilityGeneration,
              signal: batchSignal,
            },
          ));
        } catch (error) {
          if ((error as Error).message !== 'PATH_HISTORY_LIMIT') throw error;
          limitedPacketHashes.add(packetHash);
        }
      }
      return { results, limitedPacketHashes };
    },
  };
}
