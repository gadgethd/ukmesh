/**
 * Gold-packet accuracy harness for the lazy path resolver.
 *
 * Ground truth: multibyte packets (path_hash_size_bytes > 1) whose every relay
 * hash uniquely resolves to exactly one positioned node in the network scope.
 * We degrade those hashes to 1-byte (2 hex char) prefixes to simulate the hard
 * case, run BOTH the legacy greedy resolver and the new Viterbi decoder against
 * the degraded input, and compare resolved relay nodes to the known truth.
 *
 * LEAKAGE NOTE: the node-id-keyed priors (transition/edge/motif) are rebuilt
 * from the same 120-day window that contains the gold packets, so a path that
 * occurs only once has a prior that effectively memorised its own answer. We
 * therefore STRATIFY by prior support (min transition-prior count along the
 * truth chain): the "supported" bucket is the leakage-resistant read.
 *
 * Usage: npx tsx src/path-lazy/evaluate.ts [network] [sampleSize]
 * Read-only: issues only SELECTs.
 */
import { query } from '../db/index.js';
import { lazyResolvePath, type LazyPath, type LazyPathResult } from './lazyResolver.js';
import { lazyResolvePathLegacy } from './lazyResolverLegacy.js';
import { buildNodePathHashIndex, getNodesForPathHash, normalizePathHash } from '../path-hash/utils.js';
import { expandResolverScope } from '../networks.js';

type QueryFn = <T extends Record<string, unknown> = Record<string, unknown>>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type Resolver = (packetHash: string, network: string | null, q: QueryFn) => Promise<LazyPathResult | null>;

function trimTerminalHop(rxNodeId: string, hashes: string[]): string[] {
  if (hashes.length === 0) return hashes;
  const last = hashes[hashes.length - 1]!;
  return rxNodeId.toUpperCase().startsWith(last) ? hashes.slice(0, -1) : hashes;
}

/** Truncate packets-table path_hashes to 1-byte prefixes; force size = 1. */
function degradingQuery(real: QueryFn): QueryFn {
  return (async (text: string, params?: unknown[]) => {
    const res = await real(text, params);
    if (/\bfrom\s+packets\b/i.test(text)) {
      for (const row of res.rows as Array<Record<string, unknown>>) {
        if (Array.isArray(row['path_hashes'])) {
          row['path_hashes'] = (row['path_hashes'] as unknown[]).map((h) => String(h).trim().slice(0, 2));
        }
        if ('path_hash_size_bytes' in row) row['path_hash_size_bytes'] = 1;
      }
    }
    return res;
  }) as QueryFn;
}

type GoldPacket = {
  packetHash: string;
  network: string;
  rxNodeId: string;
  truth: string[];        // truth[pos] = relay node_id
  support: number;        // min transition-prior count along the truth chain
};

async function loadGoldPackets(network: string, sampleSize: number): Promise<GoldPacket[]> {
  const scope = expandResolverScope(network);

  const nodeRows = await query<{ node_id: string }>(
    `SELECT node_id FROM nodes
      WHERE network = ANY($1) AND lat IS NOT NULL AND lon IS NOT NULL AND lat != 0 AND lon != 0`,
    [scope],
  );
  const index = buildNodePathHashIndex(nodeRows.rows);

  const candidates = await query<{
    packet_hash: string; network: string; rx_node_id: string; path_hashes: string[] | null;
  }>(
    `SELECT DISTINCT ON (packet_hash) packet_hash, network, rx_node_id, path_hashes
       FROM packets
      WHERE network = ANY($1) AND path_hash_size_bytes > 1 AND path_hashes IS NOT NULL
        AND cardinality(path_hashes) BETWEEN 2 AND 8 AND rx_node_id IS NOT NULL
        AND time > NOW() - INTERVAL '45 days'
      ORDER BY packet_hash, cardinality(path_hashes) DESC, path_hash_size_bytes DESC
      LIMIT $2`,
    [scope, sampleSize * 4],
  );

  const gold: Omit<GoldPacket, 'support'>[] = [];
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
    gold.push({ packetHash: row.packet_hash, network: row.network, rxNodeId: row.rx_node_id, truth });
    if (gold.length >= sampleSize) break;
  }

  // Prior support: summed transition-prior count for each directed truth pair.
  const fromIds = new Set<string>(), toIds = new Set<string>();
  for (const g of gold) for (let i = 0; i + 1 < g.truth.length; i++) { fromIds.add(g.truth[i]!); toIds.add(g.truth[i + 1]!); }
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

  return gold.map((g) => {
    let support = Infinity;
    for (let i = 0; i + 1 < g.truth.length; i++) {
      support = Math.min(support, pairCount.get(`${g.truth[i]}|${g.truth[i + 1]}`) ?? 0);
    }
    return { ...g, support: Number.isFinite(support) ? support : 0 };
  });
}

function bestPath(paths: LazyPath[], gold: GoldPacket): LazyPath | null {
  if (paths.length === 0) return null;
  return paths.find((p) => p.observerIds.includes(gold.rxNodeId))
    ?? [...paths].sort((a, b) => b.totalHops - a.totalHops)[0]!;
}

type Acc = { packets: number; hops: number; correct: number; wrong: number; unresolved: number; perfect: number };
function newAcc(): Acc { return { packets: 0, hops: 0, correct: 0, wrong: 0, unresolved: 0, perfect: 0 }; }

async function scorePacket(resolver: Resolver, g: GoldPacket, dq: QueryFn, acc: Acc): Promise<void> {
  acc.packets++;
  const result = await resolver(g.packetHash, g.network, dq);
  const relayByPos = new Map<number, string | null>();
  if (result) {
    const path = bestPath(result.paths, g);
    if (path) for (const n of path.canonicalPath) if (!n.isObserver) relayByPos.set(n.position, n.nodeId);
  }
  let chainCorrect = true;
  for (let pos = 0; pos < g.truth.length; pos++) {
    acc.hops++;
    const got = relayByPos.get(pos) ?? null;
    if (got == null) { acc.unresolved++; chainCorrect = false; }
    else if (got.toUpperCase() === g.truth[pos]!.toUpperCase()) acc.correct++;
    else { acc.wrong++; chainCorrect = false; }
  }
  if (chainCorrect) acc.perfect++;
}

function fmt(name: string, a: Acc): string {
  const p = (n: number) => `${((n / Math.max(1, a.hops)) * 100).toFixed(1)}%`;
  return `${name.padEnd(8)} pkts=${String(a.packets).padStart(4)} hops=${String(a.hops).padStart(4)} `
    + `acc=${p(a.correct).padStart(6)} wrong=${p(a.wrong).padStart(6)} unres=${p(a.unresolved).padStart(6)} `
    + `perfect=${((a.perfect / Math.max(1, a.packets)) * 100).toFixed(1)}%`;
}

async function main() {
  const network = process.argv[2] ?? 'ukmesh';
  const sampleSize = Number(process.argv[3] ?? 400);

  const gold = await loadGoldPackets(network, sampleSize);
  console.log(`[evaluate] network=${network} gold packets=${gold.length}`);
  if (gold.length === 0) { console.log('no gold packets'); process.exit(0); }

  // Prior key-format sanity: fraction of truth pairs with a non-zero transition prior.
  const supported = gold.filter((g) => g.support > 0).length;
  console.log(`[evaluate] truth chains with a non-zero transition prior on every hop: ${supported}/${gold.length} `
    + `(if ~0, the directed key format is wrong)`);

  const dq = degradingQuery(query as unknown as QueryFn);
  const buckets = ['rare(≤1)', 'mid(2-7)', 'supp(≥8)'] as const;
  const bucketOf = (s: number) => (s <= 1 ? 0 : s < 8 ? 1 : 2);
  const legacy = { all: newAcc(), b: [newAcc(), newAcc(), newAcc()] };
  const next = { all: newAcc(), b: [newAcc(), newAcc(), newAcc()] };

  for (const g of gold) {
    const bi = bucketOf(g.support);
    await scorePacket(lazyResolvePathLegacy, g, dq, legacy.all);
    await scorePacket(lazyResolvePathLegacy, g, dq, legacy.b[bi]!);
    await scorePacket(lazyResolvePath, g, dq, next.all);
    await scorePacket(lazyResolvePath, g, dq, next.b[bi]!);
  }

  console.log('\n=== OVERALL (blended — includes leakage) ===');
  console.log(fmt('legacy', legacy.all));
  console.log(fmt('viterbi', next.all));
  console.log('\n=== BY PRIOR SUPPORT (supp bucket = leakage-resistant) ===');
  for (let i = 0; i < buckets.length; i++) {
    console.log(`-- ${buckets[i]} --`);
    console.log(fmt('legacy', legacy.b[i]!));
    console.log(fmt('viterbi', next.b[i]!));
  }
  process.exit(0);
}

main().catch((err) => { console.error('[evaluate] fatal:', err); process.exit(1); });
