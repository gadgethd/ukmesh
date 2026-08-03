/**
 * Gold-packet accuracy harness for all path-resolution jobs.
 *
 * Ground truth: multibyte packets (path_hash_size_bytes > 1) whose every relay
 * hash uniquely resolves to exactly one positioned node in the network scope.
 * We degrade those hashes to 1-byte (2 hex char) prefixes to simulate the hard
 * case, then compare the legacy greedy resolver, lazy Viterbi resolver, beta
 * single-observer resolver, and beta multi-observer resolver against the known
 * relay chain.
 *
 * LEAKAGE NOTE: the node-id-keyed priors (transition/edge/motif) are rebuilt
 * from the same 120-day window that contains the gold packets, so a path that
 * occurs only once has a prior that effectively memorised its own answer. We
 * therefore STRATIFY by prior support (min transition-prior count along the
 * truth chain). The route-length and corridor buckets are independent views
 * of the same predictions; corridor novelty is based on whether the source →
 * receiver pair occurred earlier in the preceding 120-day prior window.
 *
 * Usage: npx tsx src/path-lazy/evaluate.ts [network] [sampleSize]
 * Read-only: issues only SELECTs. Beta's predicted-online side effect is
 * disabled, and its packet SELECTs are degraded in-memory for this run.
 */
import pg from 'pg';
import { query } from '../db/index.js';
import {
  resolveBetaPathForPacketHash,
  resolveMultiObserverBetaPath,
  type BetaResolvedPayload,
  type MultiObserverResolvedPayload,
} from '../path-beta/resolver.js';
import { lazyResolvePath, type LazyPath, type LazyPathResult } from './lazyResolver.js';
import { lazyResolvePathLegacy } from './lazyResolverLegacy.js';
import { buildNodePathHashIndex, getNodesForPathHash, normalizePathHash } from '../path-hash/utils.js';
import { expandResolverScope } from '../networks.js';

type QueryFn = <T extends Record<string, unknown> = Record<string, unknown>>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type LazyResolver = (packetHash: string, network: string | null, q: QueryFn) => Promise<LazyPathResult | null>;

type CorridorNovelty = 'seen' | 'unseen';

type GoldPacket = {
  packetHash: string;
  network: string;
  rxNodeId: string;
  srcNodeId: string | null;
  observedAt: string;
  truth: string[];        // truth[pos] = relay node_id
  support: number;        // min transition-prior count along the truth chain
  corridor: CorridorNovelty;
};

type NodeCoordinateIndex = {
  byPoint: Map<string, string[]>;
  byNodeId: Map<string, [number, number]>;
};

type GoldDataset = {
  gold: GoldPacket[];
  coordinates: NodeCoordinateIndex;
};

const PRIOR_BUILDING_WINDOW_DAYS = 120;
const PRIOR_SUPPORT_BUCKETS = ['rare(≤1)', 'mid(2-7)', 'supp(≥8)'] as const;
const ROUTE_LENGTH_BUCKETS = ['≤3', '4', '5-6', '7-8', '9-12', '13+'] as const;
const CORRIDOR_BUCKETS: CorridorNovelty[] = ['seen', 'unseen'];

function trimTerminalHop(rxNodeId: string, hashes: string[]): string[] {
  if (hashes.length === 0) return hashes;
  const last = hashes[hashes.length - 1]!;
  return rxNodeId.toUpperCase().startsWith(last) ? hashes.slice(0, -1) : hashes;
}

function degradePacketRows(rows: Array<Record<string, unknown>>): void {
  for (const row of rows) {
    if (Array.isArray(row['path_hashes'])) {
      row['path_hashes'] = (row['path_hashes'] as unknown[]).map((h) => String(h).trim().slice(0, 2));
    }
    if ('path_hash_size_bytes' in row) row['path_hash_size_bytes'] = 1;
  }
}

/** Truncate packets-table path_hashes to 1-byte prefixes; force size = 1. */
function degradingQuery(real: QueryFn): QueryFn {
  return (async (text: string, params?: unknown[]) => {
    const res = await real(text, params);
    if (/\bfrom\s+packets\b/i.test(text)) {
      degradePacketRows(res.rows as Array<Record<string, unknown>>);
    }
    return res;
  }) as QueryFn;
}

function coordinateKey(lat: number, lon: number): string {
  return `${Math.round(lat * 1_000_000)},${Math.round(lon * 1_000_000)}`;
}

function buildCoordinateIndex(
  rows: Array<{ node_id: string; lat: number; lon: number }>,
): NodeCoordinateIndex {
  const byPoint = new Map<string, string[]>();
  const byNodeId = new Map<string, [number, number]>();
  for (const row of rows) {
    const lat = Number(row.lat);
    const lon = Number(row.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat === 0 || lon === 0) continue;
    const point = coordinateKey(lat, lon);
    const ids = byPoint.get(point) ?? [];
    ids.push(row.node_id);
    byPoint.set(point, ids);
    byNodeId.set(row.node_id.toUpperCase(), [lat, lon]);
  }
  return { byPoint, byNodeId };
}

function corridorKey(srcNodeId: string, rxNodeId: string): string {
  return `${srcNodeId.toUpperCase()}|${rxNodeId.toUpperCase()}`;
}

async function loadGoldPackets(network: string, sampleSize: number): Promise<GoldDataset> {
  const scope = expandResolverScope(network);

  const nodeRows = await query<{ node_id: string; lat: number; lon: number }>(
    `SELECT node_id, lat, lon FROM nodes
      WHERE network = ANY($1) AND lat IS NOT NULL AND lon IS NOT NULL AND lat != 0 AND lon != 0`,
    [scope],
  );
  const index = buildNodePathHashIndex(nodeRows.rows);
  const coordinates = buildCoordinateIndex(nodeRows.rows);

  const candidates = await query<{
    packet_hash: string;
    network: string;
    rx_node_id: string;
    src_node_id: string | null;
    path_hashes: string[] | null;
    observed_at: string;
  }>(
    `SELECT DISTINCT ON (packet_hash) packet_hash, network, rx_node_id, src_node_id, path_hashes,
            time::text AS observed_at
       FROM packets
      WHERE network = ANY($1) AND path_hash_size_bytes > 1 AND path_hashes IS NOT NULL
        AND cardinality(path_hashes) >= 2 AND rx_node_id IS NOT NULL
        AND time > NOW() - INTERVAL '45 days'
      ORDER BY packet_hash, cardinality(path_hashes) DESC, path_hash_size_bytes DESC, time DESC
      LIMIT $2`,
    [scope, Math.max(sampleSize * 4, sampleSize + 100)],
  );

  const goldWithoutSupport: Omit<GoldPacket, 'support' | 'corridor'>[] = [];
  for (const row of candidates.rows) {
    const hashes = trimTerminalHop(
      row.rx_node_id,
      (row.path_hashes ?? []).map(normalizePathHash).filter((h) => h.length >= 4),
    );
    if (hashes.length < 2) continue;
    const truth: string[] = [];
    let ok = true;
    const seen = new Set<string>();
    for (const h of hashes) {
      const matches = getNodesForPathHash(index, h);
      if (matches.length !== 1) { ok = false; break; }
      const id = matches[0]!.node_id;
      if (seen.has(id)) { ok = false; break; }
      seen.add(id);
      truth.push(id);
    }
    if (!ok) continue;
    goldWithoutSupport.push({
      packetHash: row.packet_hash,
      network: row.network,
      rxNodeId: row.rx_node_id,
      srcNodeId: row.src_node_id,
      observedAt: row.observed_at,
      truth,
    });
    if (goldWithoutSupport.length >= sampleSize) break;
  }

  // A corridor is source → receiver. A pair is "seen" only when it occurred
  // before the candidate packet in the same 120-day window used by rebuild.ts;
  // this keeps the novelty split useful even though the live priors include
  // the current window.
  const corridorPairs = [...new Set(
    goldWithoutSupport.flatMap((g) => g.srcNodeId ? [corridorKey(g.srcNodeId, g.rxNodeId)] : []),
  )].map((key) => {
    const [srcNodeId, rxNodeId] = key.split('|');
    return { srcNodeId: srcNodeId!, rxNodeId: rxNodeId! };
  });
  const firstCorridorSeenAt = new Map<string, number>();
  // There is an index on src_node_id but not on the source × receiver pair.
  // Query exact pairs in bounded batches to avoid the large Cartesian scan that
  // an `src = ANY(...) AND rx = ANY(...)` predicate would induce.
  for (let offset = 0; offset < corridorPairs.length; offset += 128) {
    const batch = corridorPairs.slice(offset, offset + 128);
    const pairClauses = batch.map((_, index) => {
      const srcParam = index * 2 + 2;
      const rxParam = srcParam + 1;
      return `(src_node_id = $${srcParam} AND rx_node_id = $${rxParam})`;
    });
    const pairParams = batch.flatMap((pair) => [pair.srcNodeId, pair.rxNodeId]);
    const priorCorridors = await query<{
      src_node_id: string;
      rx_node_id: string;
      first_seen: string;
    }>(
      `SELECT src_node_id, rx_node_id, MIN(time)::text AS first_seen
         FROM packets
        WHERE network = ANY($1)
          AND time > NOW() - INTERVAL '${PRIOR_BUILDING_WINDOW_DAYS} days'
          AND (${pairClauses.join(' OR ')})
        GROUP BY src_node_id, rx_node_id`,
      [scope, ...pairParams],
    );
    for (const row of priorCorridors.rows) {
      const firstSeen = Date.parse(row.first_seen);
      if (Number.isFinite(firstSeen)) firstCorridorSeenAt.set(corridorKey(row.src_node_id, row.rx_node_id), firstSeen);
    }
  }

  const gold = goldWithoutSupport.map((g) => ({
    ...g,
    corridor: (() => {
      if (!g.srcNodeId) return 'unseen' as const;
      const firstSeen = firstCorridorSeenAt.get(corridorKey(g.srcNodeId, g.rxNodeId));
      const candidateTime = Date.parse(g.observedAt);
      return firstSeen != null && Number.isFinite(candidateTime) && firstSeen < candidateTime
        ? 'seen' as const
        : 'unseen' as const;
    })(),
    support: 0,
  }));

  // Prior support: summed transition-prior count for each directed truth pair.
  const fromIds = new Set<string>();
  const toIds = new Set<string>();
  for (const g of gold) {
    for (let i = 0; i + 1 < g.truth.length; i++) {
      fromIds.add(g.truth[i]!);
      toIds.add(g.truth[i + 1]!);
    }
  }
  const pairCount = new Map<string, number>();
  if (fromIds.size > 0) {
    const rows = await query<{ from_node_id: string; to_node_id: string; c: string }>(
      `SELECT from_node_id, to_node_id, SUM(count)::text as c FROM path_transition_priors
        WHERE network = ANY($1) AND from_node_id = ANY($2) AND to_node_id = ANY($3)
        GROUP BY from_node_id, to_node_id`,
      [scope, [...fromIds], [...toIds]],
    );
    for (const r of rows.rows) pairCount.set(`${r.from_node_id}|${r.to_node_id}`, Number(r.c) || 0);
  }

  return {
    coordinates,
    gold: gold.map((g) => {
      let support = Infinity;
      for (let i = 0; i + 1 < g.truth.length; i++) {
        support = Math.min(support, pairCount.get(`${g.truth[i]}|${g.truth[i + 1]}`) ?? 0);
      }
      return { ...g, support: Number.isFinite(support) ? support : 0 };
    }),
  };
}

function bestPath(paths: LazyPath[], gold: GoldPacket): LazyPath | null {
  if (paths.length === 0) return null;
  return paths.find((p) => p.observerIds.includes(gold.rxNodeId))
    ?? [...paths].sort((a, b) => b.totalHops - a.totalHops)[0]!;
}

type Acc = {
  packets: number;
  hops: number;
  correct: number;
  wrong: number;
  unresolved: number;
  perfect: number;
  failures: number;
};

function newAcc(): Acc {
  return { packets: 0, hops: 0, correct: 0, wrong: 0, unresolved: 0, perfect: 0, failures: 0 };
}

type JobStats = {
  all: Acc;
  prior: Acc[];
  length: Acc[];
  corridor: Acc[];
};

function newJobStats(): JobStats {
  return {
    all: newAcc(),
    prior: PRIOR_SUPPORT_BUCKETS.map(() => newAcc()),
    length: ROUTE_LENGTH_BUCKETS.map(() => newAcc()),
    corridor: CORRIDOR_BUCKETS.map(() => newAcc()),
  };
}

function priorBucket(support: number): number {
  return support <= 1 ? 0 : support < 8 ? 1 : 2;
}

function lengthBucket(routeLength: number): number {
  if (routeLength <= 3) return 0;
  if (routeLength === 4) return 1;
  if (routeLength <= 6) return 2;
  if (routeLength <= 8) return 3;
  if (routeLength <= 12) return 4;
  return 5;
}

function scorePrediction(gold: GoldPacket, relayByPos: Map<number, string | null>, acc: Acc): void {
  acc.packets++;
  let chainCorrect = true;
  for (let pos = 0; pos < gold.truth.length; pos++) {
    acc.hops++;
    const got = relayByPos.get(pos) ?? null;
    if (got == null) {
      acc.unresolved++;
      chainCorrect = false;
    } else if (got.toUpperCase() === gold.truth[pos]!.toUpperCase()) {
      acc.correct++;
    } else {
      acc.wrong++;
      chainCorrect = false;
    }
  }
  if (chainCorrect) acc.perfect++;
}

function relevantAccumulators(stats: JobStats, gold: GoldPacket): Acc[] {
  return [
    stats.all,
    stats.prior[priorBucket(gold.support)]!,
    stats.length[lengthBucket(gold.truth.length)]!,
    stats.corridor[gold.corridor === 'seen' ? 0 : 1]!,
  ];
}

function recordPrediction(stats: JobStats, gold: GoldPacket, relayByPos: Map<number, string | null>): void {
  for (const acc of relevantAccumulators(stats, gold)) scorePrediction(gold, relayByPos, acc);
}

function recordFailure(stats: JobStats, gold: GoldPacket): void {
  for (const acc of relevantAccumulators(stats, gold)) {
    acc.failures++;
    scorePrediction(gold, new Map(), acc);
  }
}

function lazyPrediction(result: LazyPathResult | null, gold: GoldPacket): Map<number, string | null> {
  const relayByPos = new Map<number, string | null>();
  if (!result) return relayByPos;
  const path = bestPath(result.paths, gold);
  if (path) {
    for (const node of path.canonicalPath) {
      if (!node.isObserver) relayByPos.set(node.position, node.nodeId);
    }
  }
  return relayByPos;
}

async function scoreLazyPacket(
  resolver: LazyResolver,
  gold: GoldPacket,
  dq: QueryFn,
  stats: JobStats,
  jobName: string,
): Promise<void> {
  try {
    const result = await resolver(gold.packetHash, gold.network, dq);
    recordPrediction(stats, gold, lazyPrediction(result, gold));
  } catch (error) {
    recordFailure(stats, gold);
    console.warn(`[evaluate] ${jobName} failed packet=${gold.packetHash}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function samePoint(a: [number, number], b: [number, number], epsilon = 0.0001): boolean {
  return Math.abs(a[0] - b[0]) <= epsilon && Math.abs(a[1] - b[1]) <= epsilon;
}

function betaPathPoints(payload: BetaResolvedPayload): [number, number][] {
  const red = payload.redPath ?? [];
  const purple = payload.purplePath ?? [];
  if (red.length > 0 && purple.length > 0) {
    return samePoint(red[red.length - 1]!, purple[0]!)
      ? [...red, ...purple.slice(1)]
      : [...red, ...purple];
  }
  return red.length > 0 ? red : purple;
}

function betaNodeIdAtPoint(point: [number, number], coordinates: NodeCoordinateIndex): string | null {
  const ids = coordinates.byPoint.get(coordinateKey(point[0], point[1]));
  return ids?.length === 1 ? ids[0]! : null;
}

function betaPointMatchesNode(
  point: [number, number],
  nodeId: string | null,
  coordinates: NodeCoordinateIndex,
): boolean {
  if (!nodeId) return false;
  const nodePoint = coordinates.byNodeId.get(nodeId.toUpperCase());
  return nodePoint != null && samePoint(point, nodePoint);
}

function betaPrediction(
  payload: BetaResolvedPayload | null,
  gold: GoldPacket,
  coordinates: NodeCoordinateIndex,
): Map<number, string | null> {
  const relayByPos = new Map<number, string | null>();
  if (!payload) return relayByPos;
  const points = betaPathPoints(payload);
  if (points.length === 0) return relayByPos;

  const ids = points.map((point) => betaNodeIdAtPoint(point, coordinates));
  const sourceId = payload.debug.srcNodeId ?? gold.srcNodeId;
  const receiverId = payload.debug.rxNodeId ?? gold.rxNodeId;
  if (
    ids.length > 0
    && (ids[0]?.toUpperCase() === sourceId?.toUpperCase() || betaPointMatchesNode(points[0]!, sourceId, coordinates))
  ) {
    ids.shift();
    points.shift();
  }
  if (
    ids.length > 0
    && (ids[ids.length - 1]?.toUpperCase() === receiverId.toUpperCase()
      || betaPointMatchesNode(points[points.length - 1]!, receiverId, coordinates))
  ) {
    ids.pop();
    points.pop();
  }

  // A beta suffix-partial solve reports how many hops it used. Align that
  // suffix with the right side of the gold route; a full solve starts at 0.
  const hopsUsed = Math.max(0, Number(payload.debug.hopsUsed) || 0);
  const offset = hopsUsed < gold.truth.length ? Math.max(0, gold.truth.length - hopsUsed) : 0;
  for (let i = 0; i < ids.length && offset + i < gold.truth.length; i++) {
    relayByPos.set(offset + i, ids[i]);
  }
  return relayByPos;
}

const READ_ONLY_BETA_OPTIONS = {
  touchPredictedOnline: false,
  log: false,
} as const;

async function scoreBetaSinglePacket(
  gold: GoldPacket,
  coordinates: NodeCoordinateIndex,
  stats: JobStats,
): Promise<void> {
  try {
    const result = await resolveBetaPathForPacketHash(
      gold.packetHash,
      gold.network,
      gold.rxNodeId,
      undefined,
      undefined,
      READ_ONLY_BETA_OPTIONS,
    );
    recordPrediction(stats, gold, betaPrediction(result, gold, coordinates));
  } catch (error) {
    recordFailure(stats, gold);
    console.warn(`[evaluate] beta-single failed packet=${gold.packetHash}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function multiObserverPayload(
  result: MultiObserverResolvedPayload | null,
  gold: GoldPacket,
): BetaResolvedPayload | null {
  return result?.results.find((payload) => payload.debug.rxNodeId?.toUpperCase() === gold.rxNodeId.toUpperCase()) ?? null;
}

async function scoreBetaMultiPacket(
  gold: GoldPacket,
  coordinates: NodeCoordinateIndex,
  stats: JobStats,
): Promise<void> {
  try {
    const result = await resolveMultiObserverBetaPath(
      gold.packetHash,
      gold.network,
      undefined,
      undefined,
      READ_ONLY_BETA_OPTIONS,
    );
    recordPrediction(stats, gold, betaPrediction(multiObserverPayload(result, gold), gold, coordinates));
  } catch (error) {
    recordFailure(stats, gold);
    console.warn(`[evaluate] beta-multi failed packet=${gold.packetHash}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

type PgPoolQueryResult = { rows: Array<Record<string, unknown>> };
type PgPoolQuery = (this: unknown, text: string, params?: unknown[]) => Promise<PgPoolQueryResult>;

/**
 * The beta entry points import db.query directly instead of accepting a
 * QueryFn. Patch pg's Pool method only for their evaluation window so their
 * packet SELECTs receive the same one-byte degradation as lazy/legacy.
 */
async function withDegradedBetaPackets<T>(fn: () => Promise<T>): Promise<T> {
  const poolPrototype = pg.Pool.prototype as unknown as { query: PgPoolQuery };
  const originalQuery = poolPrototype.query;
  poolPrototype.query = async function(this: unknown, text: string, params?: unknown[]): Promise<PgPoolQueryResult> {
    const result = await originalQuery.call(this, text, params);
    if (/\bfrom\s+packets\b/i.test(text)) degradePacketRows(result.rows);
    return result;
  };
  try {
    return await fn();
  } finally {
    poolPrototype.query = originalQuery;
  }
}

function fmt(name: string, acc: Acc): string {
  const hopAccuracy = (acc.correct / Math.max(1, acc.hops)) * 100;
  const routeAccuracy = (acc.perfect / Math.max(1, acc.packets)) * 100;
  return `${name.padEnd(14)} packets=${String(acc.packets).padStart(4)} hops=${String(acc.hops).padStart(5)} `
    + `route=${routeAccuracy.toFixed(1).padStart(5)}% hop=${hopAccuracy.toFixed(1).padStart(5)}% `
    + `wrong=${((acc.wrong / Math.max(1, acc.hops)) * 100).toFixed(1).padStart(5)}% `
    + `unres=${((acc.unresolved / Math.max(1, acc.hops)) * 100).toFixed(1).padStart(5)}% `
    + `fail=${String(acc.failures).padStart(3)}`;
}

function printStratification(
  title: string,
  labels: readonly string[],
  jobs: ReadonlyMap<string, JobStats>,
  getAcc: (stats: JobStats, index: number) => Acc,
): void {
  console.log(`\n=== ${title} ===`);
  for (let i = 0; i < labels.length; i++) {
    console.log(`-- ${labels[i]} --`);
    for (const [name, stats] of jobs) console.log(fmt(name, getAcc(stats, i)));
  }
}

async function main() {
  const network = process.argv[2] ?? 'ukmesh';
  const sampleSize = Number(process.argv[3] ?? 400);

  const dataset = await loadGoldPackets(network, sampleSize);
  const gold = dataset.gold;
  console.log(`[evaluate] network=${network} gold packets=${gold.length}`);
  if (gold.length === 0) { console.log('no gold packets'); process.exit(0); }

  const corridorCounts = CORRIDOR_BUCKETS.map((bucket) => gold.filter((g) => g.corridor === bucket).length);
  const lengthCounts = ROUTE_LENGTH_BUCKETS.map((_, i) => gold.filter((g) => lengthBucket(g.truth.length) === i).length);
  console.log(`[evaluate] corridor packets seen=${corridorCounts[0]} unseen=${corridorCounts[1]}`);
  console.log(`[evaluate] route length packets ${ROUTE_LENGTH_BUCKETS.map((label, i) => `${label}=${lengthCounts[i]}`).join(' ')}`);

  // Prior key-format sanity: fraction of truth pairs with a non-zero
  // transition prior on every hop.
  const supported = gold.filter((g) => g.support > 0).length;
  console.log(`[evaluate] truth chains with a non-zero transition prior on every hop: ${supported}/${gold.length} `
    + `(if ~0, the directed key format is wrong)`);

  const jobs = new Map<string, JobStats>([
    ['legacy-greedy', newJobStats()],
    ['lazy-viterbi', newJobStats()],
    ['beta-single', newJobStats()],
    ['beta-multi', newJobStats()],
  ]);
  const legacy = jobs.get('legacy-greedy')!;
  const lazy = jobs.get('lazy-viterbi')!;
  const betaSingle = jobs.get('beta-single')!;
  const betaMulti = jobs.get('beta-multi')!;
  const dq = degradingQuery(query as unknown as QueryFn);

  for (let index = 0; index < gold.length; index++) {
    const goldPacket = gold[index]!;
    await Promise.all([
      scoreLazyPacket(lazyResolvePathLegacy, goldPacket, dq, legacy, 'legacy-greedy'),
      scoreLazyPacket(lazyResolvePath, goldPacket, dq, lazy, 'lazy-viterbi'),
    ]);
    if ((index + 1) % 25 === 0 || index + 1 === gold.length) {
      console.log(`[evaluate] lazy/legacy ${index + 1}/${gold.length}`);
    }
  }

  await withDegradedBetaPackets(async () => {
    for (let index = 0; index < gold.length; index++) {
      const goldPacket = gold[index]!;
      await scoreBetaSinglePacket(goldPacket, dataset.coordinates, betaSingle);
      await scoreBetaMultiPacket(goldPacket, dataset.coordinates, betaMulti);
      if ((index + 1) % 25 === 0 || index + 1 === gold.length) {
        console.log(`[evaluate] beta single/multi ${index + 1}/${gold.length}`);
      }
    }
  });

  console.log('\n=== OVERALL (blended — includes leakage) ===');
  for (const [name, stats] of jobs) console.log(fmt(name, stats.all));
  printStratification('BY PRIOR SUPPORT (supp bucket = leakage-resistant)', PRIOR_SUPPORT_BUCKETS, jobs,
    (stats, index) => stats.prior[index]!);
  printStratification('BY ROUTE LENGTH', ROUTE_LENGTH_BUCKETS, jobs,
    (stats, index) => stats.length[index]!);
  printStratification('BY CORRIDOR NOVELTY (earlier 120-day prior window)', CORRIDOR_BUCKETS, jobs,
    (stats, index) => stats.corridor[index]!);
  process.exit(0);
}

main().catch((err) => { console.error('[evaluate] fatal:', err); process.exit(1); });
