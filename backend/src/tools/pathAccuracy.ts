/** Read-only, frozen-snapshot comparison of live decoders on hidden three-byte routes.
 *
 * Capture: node --env-file=../.env --import tsx src/tools/pathAccuracy.ts capture /tmp/path-snapshot.json
 * Replay:  node --import tsx src/tools/pathAccuracy.ts replay /tmp/path-snapshot.json /tmp/result.json [decoder.ts] [dev|test]
 * A baseline decoder can be taken from a git archive with its relative imports intact.
 */
import { readFile, writeFile, mkdir, symlink } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { resolve, join } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import pg from 'pg';
import { buildChampionPriors, type ChampionPriorRows } from '../path-beta/championPriors.js';
import type { BetaResolveContext, MeshNode, PathPacket, LinkMetrics } from '../path-beta/types.js';
import type { BetaObserverEntry } from '../path-beta/sharedDecoder.js';
import { linkKey } from '../path-beta/geometry.js';
import { expandResolverScope } from '../networks.js';

type Observation = PathPacket & { time: string; split: number; network: string };
type Snapshot = {
  capturedAt: string;
  network: string;
  windowDays: number;
  calibration: Record<string, unknown>[];
  nodes: MeshNode[];
  links: (LinkMetrics & { node_a_id: string; node_b_id: string })[];
  prefixes: { prefix: string; prev_prefix: string; receiver_region: string; node_id: string; count: number }[];
  transitions: { from_node_id: string; to_node_id: string; receiver_region: string; count: number; probability: number }[];
  priors: ChampionPriorRows;
  observations: Observation[];
};

async function capture(file: string) {
  const client = new pg.Client({
    connectionString: process.env['DATABASE_URL'],
    host: process.env['PGHOST'] ?? '127.0.0.1',
    user: process.env['POSTGRES_USER'] ?? 'meshcore',
    password: process.env['POSTGRES_PASSWORD'],
    database: process.env['POSTGRES_DB'] ?? 'meshcore',
    application_name: 'path-accuracy-readonly',
    statement_timeout: 120_000,
  });
  await client.connect();
  try {
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const network = process.env['PATH_ACCURACY_NETWORK'] ?? 'ukmesh';
    const scope = network === 'all' ? null : expandResolverScope(network);
    const windowDays = Number(process.env['PATH_ACCURACY_DAYS'] ?? 14);
    if (!Number.isInteger(windowDays) || windowDays < 1 || windowDays > 45) throw new Error('invalid window days');
    const rows = async <T extends pg.QueryResultRow>(sql: string, params: unknown[] = [network]): Promise<T[]> =>
      (await client.query<T>(sql, params)).rows;
    const capturedAt = (await rows<{ t: string }>('SELECT now()::text AS t', []))[0]!.t;
    const calibration = await rows('SELECT * FROM path_model_calibration WHERE network = $1');
    const nodes = await rows<MeshNode>(`SELECT node_id, name, lat, lon, iata, role, elevation_m, last_seen::text
      FROM nodes WHERE ($1::text[] IS NULL OR network = ANY($1)) AND (name IS NULL OR name NOT LIKE '%🚫%') ORDER BY node_id`, [scope]);
    const links = await rows<Snapshot['links'][number]>(`SELECT nl.node_a_id, nl.node_b_id, nl.observed_count,
      nl.multibyte_observed_count, nl.itm_path_loss_db, nl.itm_viable, nl.count_a_to_b, nl.count_b_to_a
      FROM node_links nl JOIN nodes a ON a.node_id = nl.node_a_id JOIN nodes b ON b.node_id = nl.node_b_id
      WHERE ($1::text[] IS NULL OR (a.network = ANY($1) AND b.network = ANY($1))) AND (nl.itm_viable IS NOT NULL OR nl.force_viable = true)
      AND (a.name IS NULL OR a.name NOT LIKE '%🚫%') AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')`, [scope]);
    const prefixes = await rows<Snapshot['prefixes'][number]>(`SELECT prefix, prev_prefix, receiver_region, node_id, count
      FROM path_prefix_priors WHERE network = $1 ORDER BY count DESC LIMIT 6000`);
    const transitions = await rows<Snapshot['transitions'][number]>(`SELECT from_node_id, to_node_id, receiver_region, count, probability
      FROM path_transition_priors WHERE network = $1 ORDER BY count DESC`);
    const priors: ChampionPriorRows = {
      positionPrefixes: await rows('SELECT prefix, position, node_id, count FROM path_position_prefix_priors WHERE network = $1'),
      corridors: await rows('SELECT src_node_id, rx_node_id, position, node_id, count FROM path_corridor_priors WHERE network = $1'),
      transitions,
      positionTransitions: await rows('SELECT position, from_node_id, to_node_id, count FROM path_position_transition_priors WHERE network = $1'),
    };
    // The learner trains only on buckets 0..69. Split at packet hash so every
    // observation of a held-out packet is excluded from all learned priors.
    const gold = await rows<{ packet_hash: string }>(`SELECT DISTINCT packet_hash FROM packets
      WHERE ($1::text[] IS NULL OR network = ANY($1)) AND time > now() - $2 * interval '1 day'
        AND path_hash_size_bytes = 3 AND cardinality(path_hashes) >= 2
        AND mod(hashtext(packet_hash)::bigint + 2147483648, 100) >= 70`, [scope, windowDays]);
    console.log(`capturing ${gold.length} held-out packet hashes`);
    const observations = await rows<Observation>(`SELECT time::text, network, packet_hash, rx_node_id, src_node_id,
      packet_type, route_type, payload->>'direction' AS direction, hop_count, path_hashes, path_hash_size_bytes,
      mod(hashtext(packet_hash)::bigint + 2147483648, 100)::int AS split
      FROM packets WHERE ($1::text[] IS NULL OR network = ANY($1)) AND time > now() - $2 * interval '1 day'
        AND packet_hash = ANY($3::text[]) ORDER BY time, packet_hash, rx_node_id`,
    [scope, windowDays, gold.map((row) => row.packet_hash)]);
    const snapshot: Snapshot = { capturedAt, network, windowDays, calibration, nodes, links, prefixes, transitions, priors, observations };
    await writeFile(file, JSON.stringify(snapshot));
    await client.query('COMMIT');
    console.log(JSON.stringify({ file, nodes: nodes.length, links: links.length, observations: observations.length }));
  } finally {
    await client.end();
  }
}

async function archiveBaseline(directory: string, ref: string) {
  const cwd = fileURLToPath(new URL('../../../', import.meta.url));
  const revision = execFileSync('git', ['rev-parse', '--verify', `${ref}^{commit}`], { cwd, encoding: 'utf8' }).trim();
  const destination = resolve(directory);
  await mkdir(destination);
  const archive = execFileSync('git', ['archive', revision, 'backend/src', 'backend/package.json'], { cwd, maxBuffer: 64 * 1024 * 1024 });
  execFileSync('tar', ['-x', '-C', destination], { input: archive });
  await symlink(fileURLToPath(new URL('../../node_modules', import.meta.url)), join(destination, 'backend/node_modules'));
  const resolver = join(destination, 'backend/src/path-beta/resolver.ts');
  const original = await readFile(resolver, 'utf8');
  // Expose an existing pure function to the replay harness. Its body and all
  // scoring/configuration files remain byte-for-byte at the baseline commit.
  await writeFile(resolver, original.replace(/^function preparePacketObservation\(/m, 'export function preparePacketObservation('));
  const manifest = { revision, resolverSha256: createHash('sha256').update(original).digest('hex'), modification: 'export preparePacketObservation only' };
  await writeFile(join(destination, 'baseline.json'), JSON.stringify(manifest, null, 2));
  console.log(JSON.stringify({ directory: destination, ...manifest }));
}

function makeContext(snapshot: Snapshot): BetaResolveContext {
  const choiceCounts = new Map<string, number>();
  const totals = new Map<string, number>();
  for (const row of snapshot.prefixes) {
    const group = `${row.receiver_region}|${row.prefix.toUpperCase().slice(0, 2)}|${row.prev_prefix.toUpperCase().slice(0, 2)}`;
    const key = `${group}|${row.node_id}`;
    choiceCounts.set(key, (choiceCounts.get(key) ?? 0) + Number(row.count));
    totals.set(group, (totals.get(group) ?? 0) + Number(row.count));
  }
  const prefixProbabilities = new Map([...choiceCounts].map(([key, count]) => [key, count / totals.get(key.slice(0, key.lastIndexOf('|')))!]));
  return {
    loadedAt: Date.parse(snapshot.capturedAt), visibilityGeneration: 0,
    nodesById: new Map(snapshot.nodes.map((node) => [node.node_id, node])),
    repeaterNodes: snapshot.nodes.filter((n) => n.lat != null && n.lon != null && n.lat !== 0 && n.lon !== 0 && (n.role == null || n.role === 2)),
    linkMetrics: new Map(snapshot.links.map((link) => [linkKey(link.node_a_id, link.node_b_id), link])),
    learningModel: {
      champion: buildChampionPriors(snapshot.priors), prefixProbabilities,
      transitionProbabilities: new Map(snapshot.transitions.slice(0, 6000).map((r) => [`${r.receiver_region}|${r.from_node_id}|${r.to_node_id}`, Number(r.probability)])),
      edgeScores: new Map(), motifProbabilities: new Map(), confidenceScale: 1, confidenceBias: 0, bucketHours: 6,
    },
  };
}

/** Replay the former lazy SQL adapter against the same immutable evidence. */
function snapshotQuery(snapshot: Snapshot, observations: Observation[]) {
  const membership = new WeakMap<unknown[], Set<unknown>>();
  return async (sql: string, params: unknown[] = []) => {
    const includes = (ids: unknown, id: string) => {
      if (!Array.isArray(ids)) return false;
      let set = membership.get(ids);
      if (!set) { set = new Set(ids); membership.set(ids, set); }
      return set.has(id);
    };
    if (/FROM packets\b/i.test(sql)) {
      if (/SELECT DISTINCT rx_node_id, hop_count/i.test(sql)) return { rows: observations };
      const byObserver = new Map<string | null, Observation>();
      for (const row of [...observations].sort((a, b) => (b.path_hashes?.length ?? 0) - (a.path_hashes?.length ?? 0))) {
        if (!byObserver.has(row.rx_node_id)) byObserver.set(row.rx_node_id, row);
      }
      return { rows: [...byObserver.values()] };
    }
    if (/FROM nodes\b/i.test(sql)) return { rows: snapshot.nodes.filter((node) => /node_id = ANY\(\$1\)/i.test(sql)
      ? includes(params[0], node.node_id) && node.lat != null && node.lon != null && node.lat !== 0 && node.lon !== 0
      : params.slice(1, -1).some((prefix) => typeof prefix === 'string' && node.node_id.toUpperCase().startsWith(prefix.replace('%', '')))) };
    if (/FROM node_links\b/i.test(sql)) return { rows: snapshot.links.filter((link) => includes(params[0], link.node_a_id) && includes(params[0], link.node_b_id)) };
    if (/FROM path_position_prefix_priors\b/i.test(sql)) return { rows: snapshot.priors.positionPrefixes.filter((row) => includes(params[1], row.prefix)) };
    if (/FROM path_corridor_priors\b/i.test(sql)) return { rows: snapshot.priors.corridors.filter((row) => includes(params[1], row.src_node_id) && includes(params[2], row.rx_node_id)) };
    if (/FROM path_position_transition_priors\b/i.test(sql)) return { rows: snapshot.priors.positionTransitions.filter((row) => includes(params[1], row.from_node_id) && includes(params[1], row.to_node_id)) };
    if (/FROM path_transition_priors\b/i.test(sql)) {
      const counts = new Map<string, { from_node_id: string; to_node_id: string; count: number }>();
      for (const row of snapshot.transitions) {
        if (!includes(params[1], row.from_node_id) || !includes(params[1], row.to_node_id)) continue;
        const key = `${row.from_node_id}|${row.to_node_id}`;
        const previous = counts.get(key);
        counts.set(key, { from_node_id: row.from_node_id, to_node_id: row.to_node_id, count: (previous?.count ?? 0) + Number(row.count) });
      }
      return { rows: [...counts.values()] };
    }
    if (/FROM path_model_calibration\b/i.test(sql)) return { rows: snapshot.calibration };
    throw new Error(`Unimplemented snapshot query: ${sql}`);
  };
}

async function replay(file: string, output: string, decoderFile?: string, split = 'dev') {
  if (!['dev', 'test'].includes(split)) throw new Error('split must be dev or test');
  const raw = await readFile(file, 'utf8');
  const snapshot = JSON.parse(raw) as Snapshot;
  const context = makeContext(snapshot);
  const decoder = await import(pathToFileURL(resolve(decoderFile ?? 'src/path-beta/sharedDecoder.ts')).href) as
    typeof import('../path-beta/sharedDecoder.js') & Partial<typeof import('../path-lazy/lazyResolver.js')>;
  const preparation = await import(new URL(decoder.lazyResolvePath ? '../path-beta/resolver.ts' : './resolver.ts',
    pathToFileURL(resolve(decoderFile ?? 'src/path-beta/sharedDecoder.ts'))).href) as typeof import('../path-beta/resolver.js');
  const mode = process.env['PATH_ACCURACY_MODE'] === 'multi' ? 'multi' : 'single';
  const threeByteIndex = new Map<string, MeshNode[]>();
  for (const node of snapshot.nodes) {
    const hash = node.node_id.toUpperCase().slice(0, 6);
    threeByteIndex.set(hash, [...(threeByteIndex.get(hash) ?? []), node]);
  }
  const byPacket = new Map<string, Observation[]>();
  for (const obs of snapshot.observations) {
    if ((obs.split < 85) !== (split === 'dev')) continue;
    byPacket.set(obs.packet_hash, [...(byPacket.get(obs.packet_hash) ?? []), obs]);
  }
  let excluded = 0;
  const cases: { packetHash: string; receiver: string; truth: string[]; prediction: (string | null)[]; ms: number; rawMs: number; rawCorrect: boolean; source: string | null }[] = [];
  for (const [packetHash, observations] of byPacket) {
    const candidates = observations.filter((row) => row.path_hash_size_bytes === 3)
      .sort((a, b) => (b.path_hashes?.length ?? 0) - (a.path_hashes?.length ?? 0) || b.time.localeCompare(a.time));
    let selected: { obs: Observation; hashes: string[]; truth: string[]; rx: MeshNode } | undefined;
    for (const obs of candidates) {
      if (obs.route_type != null && obs.route_type !== 0 && obs.route_type !== 1) continue;
      const rx = context.nodesById.get(obs.rx_node_id ?? '');
      if (!rx || rx.lat == null || rx.lon == null || rx.lat === 0 || rx.lon === 0) continue;
      let hashes = (obs.path_hashes ?? []).map((h) => h.toUpperCase());
      if (hashes.some((h) => !/^[0-9A-F]{6}$/.test(h))) continue;
      if (obs.direction !== 'rx' && hashes.length > 1 && rx.role === 2 && rx.node_id.toUpperCase().startsWith(hashes.at(-1)!)) hashes = hashes.slice(0, -1);
      if (hashes.length < 2 || hashes.length > 25) continue;
      const nodes = hashes.map((h) => threeByteIndex.get(h) ?? []);
      if (nodes.some((matches) => matches.length !== 1 || matches[0]!.lat == null || matches[0]!.lon == null)) continue;
      selected = { obs, rx, hashes, truth: nodes.map((n) => n[0]!.node_id) };
      break;
    }
    if (!selected) { excluded++; continue; }
    const { obs, rx, truth } = selected;
    const decode = async (size: number) => {
      const visible = (mode === 'multi' ? observations.filter((row) => Math.abs(Date.parse(row.time) - Date.parse(obs.time)) <= 60_000) : [obs])
        .map((row) => ({ ...row, path_hashes: row.path_hashes?.map((h) => size === 1 ? h.slice(0, 2) : h) ?? null,
          path_hash_size_bytes: size === 1 ? 1 : row.path_hash_size_bytes }));
      if (decoder.lazyResolvePath) {
        const query = snapshotQuery(snapshot, visible);
        const result = await decoder.lazyResolvePath(packetHash, snapshot.network,
          query as Parameters<typeof decoder.lazyResolvePath>[2], { referenceTime: obs.time });
        const resolved = result?.paths.find((p) => p.observerIds.includes(rx.node_id));
        return new Map(resolved?.canonicalPath.filter((h) => !h.isObserver).map((h) => [h.position, h.nodeId]) ?? []);
      }
      const byObserver = new Map<string, ReturnType<typeof preparation.preparePacketObservation>>();
      for (const row of visible) {
        const observer = context.nodesById.get(row.rx_node_id ?? '');
        if (!observer || observer.lat == null || observer.lon == null) continue;
        const prepared = preparation.preparePacketObservation(row, observer);
        if (prepared.ignoreForPathing || prepared.hops.length === 0) continue;
        const previous = byObserver.get(observer.node_id);
        byObserver.set(observer.node_id, previous ? preparation.mergeObserverEvidence(previous, prepared) : prepared);
      }
      const entries: BetaObserverEntry[] = preparation.prepareObservationEntries
        ? preparation.prepareObservationEntries(visible, context)
        : [...byObserver.values()].map((p) => ({
        observerId: p.rx!.node_id, rx: p.rx!, hashes: p.hashes, hops: p.hops, packet: p.packet,
      }));
      const target = preparation.preparePacketObservation({ ...obs, path_hashes: obs.path_hashes?.map((h) => size === 1 ? h.slice(0, 2) : h) ?? null,
        path_hash_size_bytes: size }, rx);
      const groups = decoder.groupCompatibleObservations(entries);
      const group = groups.find((g) => g.members.some((e) => e.observerId === rx.node_id
        && e.hops.length === target.hops.length && e.hops.every((h, i) => h === target.hops[i])))
        ?? groups.find((g) => g.members.some((e) => e.observerId === rx.node_id));
      if (!group) return new Map<number, string | null>();
      return new Map([...decoder.decodeBetaCanonicalGroup(group, context).hops]
        .map(([position, hop]) => [position, hop.nodeId]));
    };
    const start = performance.now();
    const result = await decode(1);
    const ms = performance.now() - start;
    const rawStart = performance.now();
    const rawResult = await decode(3);
    const rawMs = performance.now() - rawStart;
    cases.push({ packetHash, receiver: rx.node_id, source: obs.src_node_id, truth,
      prediction: truth.map((_, i) => result.get(i) ?? null), ms, rawMs,
      rawCorrect: truth.every((id, i) => rawResult.get(i) === id) });
  }
  const routeCorrect = cases.filter((c) => c.truth.every((id, i) => c.prediction[i] === id)).length;
  const totalHops = cases.reduce((n, c) => n + c.truth.length, 0);
  const correctHops = cases.reduce((n, c) => n + c.truth.filter((id, i) => c.prediction[i] === id).length, 0);
  const times = cases.map((c) => c.ms).sort((a, b) => a - b);
  const summary = { snapshotSha256: createHash('sha256').update(raw).digest('hex'), capturedAt: snapshot.capturedAt,
    decoder: decoderFile ?? 'current', split, mode, routes: cases.length, excluded, routeCorrect, totalHops, correctHops,
    routeAccuracy: routeCorrect / cases.length, hopAccuracy: correctHops / totalHops,
    rawThreeByteCorrect: cases.filter((c) => c.rawCorrect).length,
    uniqueRoutes: new Set(cases.map((c) => c.truth.join('|'))).size,
    medianMs: times[Math.floor(times.length * 0.5)], p95Ms: times[Math.floor(times.length * 0.95)], p99Ms: times[Math.floor(times.length * 0.99)] };
  const sourceRoot = new URL('../', pathToFileURL(resolve(decoderFile ?? 'src/path-beta/sharedDecoder.ts')));
  const sourceFiles = ['path-beta/sharedDecoder.ts', 'path-beta/resolver.ts', 'path-core/decoder.ts', 'path-shared/scoring.ts', 'platform/config/pathing.ts'];
  const sourceHashes = Object.fromEntries(await Promise.all(sourceFiles.map(async (name) => [name,
    createHash('sha256').update(await readFile(new URL(name, sourceRoot))).digest('hex')])));
  await writeFile(output, JSON.stringify({ summary, sourceHashes, cases }, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

type AccuracyCase = { packetHash: string; receiver: string; source: string | null; truth: string[]; prediction: (string | null)[] };
type AccuracyResult = { summary: { snapshotSha256: string; split: string; mode: string; [key: string]: unknown }; cases: AccuracyCase[] };

export function compareAccuracy(baseline: AccuracyResult, current: AccuracyResult) {
  for (const key of ['snapshotSha256', 'split', 'mode'] as const) {
    if (baseline.summary[key] !== current.summary[key]) throw new Error(`comparison mismatch: ${key}`);
  }
  if (baseline.cases.length === 0 || baseline.cases.length !== current.cases.length) throw new Error('comparison mismatch: cases');
  const oldCases = new Map(baseline.cases.map((c) => [`${c.packetHash}|${c.receiver}`, c]));
  let improved = 0;
  let regressed = 0;
  let hopGain = 0;
  let totalHops = 0;
  const routeClusters = new Map<string, number[]>();
  const strata: Record<string, { routes: number; baselineCorrect: number; currentCorrect: number }> = {};
  for (const next of current.cases) {
    const previous = oldCases.get(`${next.packetHash}|${next.receiver}`);
    if (!previous || JSON.stringify(previous.truth) !== JSON.stringify(next.truth)) throw new Error('comparison mismatch: ground truth');
    const before = previous.truth.every((id, i) => previous.prediction[i] === id);
    const after = next.truth.every((id, i) => next.prediction[i] === id);
    if (after && !before) improved++;
    if (before && !after) regressed++;
    totalHops += next.truth.length;
    hopGain += next.truth.filter((id, i) => next.prediction[i] === id).length - previous.truth.filter((id, i) => previous.prediction[i] === id).length;
    const key = next.truth.join('|');
    routeClusters.set(key, [...(routeClusters.get(key) ?? []), Number(after) - Number(before)]);
    for (const label of [next.source ? 'known-source' : 'unknown-source', next.truth.length <= 3 ? 'hops-2-3' : next.truth.length <= 6 ? 'hops-4-6' : next.truth.length <= 12 ? 'hops-7-12' : 'hops-13+']) {
      const bucket = strata[label] ?? { routes: 0, baselineCorrect: 0, currentCorrect: 0 };
      bucket.routes++; bucket.baselineCorrect += Number(before); bucket.currentCorrect += Number(after);
      strata[label] = bucket;
    }
  }
  // Resample unique relay chains, giving recurrent copies of the same route
  // one vote. This is stricter than treating every packet as independent.
  const means = [...routeClusters.values()].map((values) => values.reduce((a, b) => a + b, 0) / values.length);
  let seed = 0x5eeda11;
  const random = () => { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; return (seed >>> 0) / 2 ** 32; };
  const bootstrap: number[] = [];
  for (let iteration = 0; iteration < 2000; iteration++) {
    let sum = 0;
    for (let i = 0; i < means.length; i++) sum += means[Math.floor(random() * means.length)]!;
    bootstrap.push(100 * sum / means.length);
  }
  bootstrap.sort((a, b) => a - b);
  return { baseline: baseline.summary, current: current.summary, improved, regressed,
    routeGainPercentagePoints: 100 * (improved - regressed) / current.cases.length,
    hopGainPercentagePoints: 100 * hopGain / totalHops,
    uniqueRouteGain95IntervalPercentagePoints: [bootstrap[50], bootstrap[1950]], strata };
}

async function main() {
  const [command, file, output, decoder, split] = process.argv.slice(2);
  if (!file) throw new Error('expected capture, baseline, replay or compare; see docs/pathing.md');
  if (command === 'capture') await capture(file);
  else if (command === 'baseline' && output) await archiveBaseline(file, output);
  else if (command === 'replay' && output) await replay(file, output, decoder === 'current' ? undefined : decoder, split);
  else if (command === 'compare' && output && decoder) {
    const result = compareAccuracy(JSON.parse(await readFile(file, 'utf8')), JSON.parse(await readFile(output, 'utf8')));
    await writeFile(decoder, JSON.stringify(result, null, 2));
    console.log(JSON.stringify(result, null, 2));
    if (result.routeGainPercentagePoints <= 0 || result.hopGainPercentagePoints <= 0) process.exitCode = 1;
  } else throw new Error('invalid command');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
