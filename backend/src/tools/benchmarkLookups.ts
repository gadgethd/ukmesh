/** Owned-fixture HTTP/SQL qualification using the actual public route handlers.
 * No fixture writes, .env loading, cache-policy changes or remote requests.
 */
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, writeFile } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import express, { Router } from 'express';
import pg from 'pg';
import { assertLocalDatabaseUrl, option } from './sampleData.js';

const args = process.argv.slice(2);
const endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
const readOnlyUrl = new URL(endpoint);
readOnlyUrl.searchParams.set('options','-c default_transaction_read_only=on');
for (const key of ['DATABASE_URL','MESHCORE_HTTP_DATABASE_URL','MESHCORE_WORKER_DATABASE_URL','MESHCORE_INGEST_DATABASE_URL','OWNER_DATABASE_URL']) {
  process.env[key]=readOnlyUrl.href;
}
delete process.env['DATABASE_SCHEMA'];
const source = path.resolve(option(args, 'source-root') ?? 'src');
const output = path.resolve(option(args, 'output') ?? `../plans/database-lookups/${Date.now()}`);
const samples = Number(option(args, 'samples') ?? 200);
const concurrency = Number(option(args, 'concurrency') ?? 8);
const trials = Number(option(args, 'trials') ?? 3);
const cache = option(args, 'cache') ?? 'miss';
const budgetMs = option(args, 'budget-ms') ? Number(option(args, 'budget-ms')) : undefined;
if (!Number.isSafeInteger(samples) || samples < 3 || samples > 1000
  || ![1, 8].includes(concurrency) || !Number.isSafeInteger(trials) || trials < 1 || trials > 3
  || !['miss', 'warm'].includes(cache)) throw new Error('invalid lookup benchmark options');

const load = (file: string) => import(pathToFileURL(path.join(source, file)).href);
const runtime = await load('db/runtime.ts');
const { createNodeRepository } = await load('repositories/nodes.ts');
const nodeReads = await load('repositories/nodeReads.ts');
const packetReads = await load('repositories/packetReads.ts');
const { registerNodeRoutes } = await load('api/routes/nodes.ts');
const { registerMiscRoutes } = await load('api/routes/misc.ts');
const { wrapAsyncHandlers, apiErrorMiddleware } = await load('api/errors.ts');
const { networkFilters } = await load('platform/query/networkFilters.ts');
const { readPublicResponseGeneration } = await load('privacy/responseGeneration.ts');
const { createGlobalApiLimiter, NODES_LIMITER, PACKET_DETAIL_LIMITER } = await load('api/bootstrap/limiters.ts');
const pool = runtime.pool as pg.Pool;
const query = pool.query.bind(pool);
type CapturedQuery = { workload: string; sql: string; params: unknown[]; calls: number; totalMs: number };
const captured = new Map<string, CapturedQuery>();
let workload = 'fixture-probe';
// Observe checked-out clients as well as pool.query, preserving pg's callback
// and promise overloads. All statements remain byte-for-byte production SQL.
pool.on('connect', client => {
  const execute = client.query.bind(client) as (...values: unknown[]) => unknown;
  client.query = ((...values: unknown[]) => {
    const spec = values[0] as string | pg.QueryConfig;
    const text = typeof spec === 'string' ? spec : spec.text;
    if (!/^\s*(?:\/\*[\s\S]*?\*\/\s*)?(SELECT|WITH|SHOW|EXPLAIN)\b/i.test(text)) {
      throw new Error('lookup benchmark refuses non-read SQL');
    }
    const started = performance.now();
    const record = () => {
      if (/^EXPLAIN\b/i.test(text)) return;
      const key = workload + '\0' + text;
      const previous = captured.get(key);
      if (previous) { previous.calls++; previous.totalMs += performance.now() - started; }
      else captured.set(key, { workload, sql: text,
        params: typeof spec !== 'string' ? spec.values ?? [] : Array.isArray(values[1]) ? values[1] : [],
        calls: 1, totalMs: performance.now() - started });
    };
    const callback = values.at(-1);
    if (typeof callback === 'function') {
      values[values.length - 1] = (...result: unknown[]) => { record(); callback(...result); };
      return execute(...values);
    }
    return (execute(...values) as Promise<unknown>).finally(record);
  }) as typeof client.query;
});

const percentile = (values: number[], quantile: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length * quantile) - 1]!;
};
// Many real loopback clients keep the deployed per-client rate limits intact.
// They share eight active request slots, rather than raising any server limits.
const agents = Array.from({ length: 128 }, (_, index) => new http.Agent({ keepAlive: true,
  maxSockets: 1, localAddress: `127.0.1.${index + 1}` }));
type Case = { name: string; route: string; expected?: number; populated?: boolean };
const report: unknown[] = [];
let requestId = 0;
async function request(port: number, testCase: Case) {
  const started = performance.now();
  return new Promise<{ ms: number; bytes: number; digest: string; dataDigest: string; entries: number | null }>((resolve, reject) => {
    const req = http.get({ hostname: '127.0.0.1', port, path: testCase.route,
      agent: agents[requestId++ % agents.length], headers: { Accept: 'application/json' } }, res => {
      const chunks: Buffer[] = []; let bytes = 0;
      res.on('data', chunk => { bytes += chunk.length; if (bytes > 8 * 1024 * 1024) req.destroy(new Error('response budget exceeded')); else chunks.push(chunk); });
      res.on('error', reject);
      res.on('end', () => {
        try {
          if (res.statusCode !== (testCase.expected ?? 200)) throw new Error(`${testCase.name}: HTTP ${res.statusCode}`);
          const receivedAt = performance.now();
          const body = Buffer.concat(chunks), value = JSON.parse(body.toString());
          const entries = Array.isArray(value) ? value.length : Array.isArray(value.nodes) ? value.nodes.length : null;
          if (testCase.populated && (entries === 0 || value === null)) throw new Error(`${testCase.name}: empty fixture`);
          const data = value?.page?.snapshot ? { ...value, page: { ...value.page, snapshot: '[request snapshot]' } } : value;
          resolve({ ms: receivedAt - started, bytes, entries,
            digest: createHash('sha256').update(body).digest('hex'),
            dataDigest: createHash('sha256').update(JSON.stringify(data)).digest('hex') });
        } catch (error) { reject(error); }
      });
    });
    req.setTimeout(30_000, () => req.destroy(new Error('request timed out')));
    req.on('error', reject);
  });
}

try {
  if ((await query("SELECT current_setting('transaction_read_only') AS value")).rows[0]?.value !== 'on') {
    throw new Error('benchmark connections must enforce read-only transactions');
  }
  const counts = (await query(`SELECT (SELECT count(*)::text FROM core.nodes) nodes,
    (SELECT count(*)::text FROM mesh.packets) observations,
    (SELECT count(DISTINCT packet_hash)::text FROM mesh.packets) packets,
    (SELECT count(*)::text FROM core.node_links) links`)).rows[0];
  if (Number(counts.nodes) < 3 || Number(counts.observations) < 20) throw new Error('populated migrated fixture required');
  const snapshot = option(args, 'snapshot') ?? new Date().toISOString();
  const visible = await createNodeRepository(runtime.query).listPublicMapRows(
    ['node_id'], networkFilters('ukmesh'), snapshot, null, 1000);
  const identity = (await query(`SELECT n.node_id, n.name FROM core.node_identity_nodes n
    WHERE n.name IS NOT NULL AND n.name NOT LIKE '%🚫%' AND n.network <> 'test'
    AND n.role = 2 AND n.lat BETWEEN -90 AND 90 AND n.lon BETWEEN -180 AND 180
    AND EXISTS (SELECT 1 FROM core.node_identity_links l WHERE l.node_a_id=n.node_id OR l.node_b_id=n.node_id)
    AND n.node_id = ANY($1::text[])
    ORDER BY n.advert_count DESC, n.node_id LIMIT 1`, [visible.map((node: {node_id: string}) => node.node_id)])).rows[0];
  if (!identity) throw new Error('public linked identity required');
  const packet = (await query(`SELECT packet_hash FROM mesh.packets WHERE visibility_ok IS TRUE
    AND is_private IS NOT TRUE ORDER BY time DESC, packet_hash LIMIT 1`)).rows[0];
  if (!packet) throw new Error('public packet required');
  const observer = (await query(`SELECT p.rx_node_id FROM mesh.packets p
    WHERE p.visibility_ok IS TRUE AND p.is_private IS NOT TRUE AND p.rx_node_id IS NOT NULL
    AND p.network <> 'test' AND p.time > NOW() - INTERVAL '7 days'
    GROUP BY p.rx_node_id ORDER BY count(*) DESC, p.rx_node_id LIMIT 1`)).rows[0]?.rx_node_id;
  if (!observer) throw new Error('public observer required');
  const map = `/api/nodes/map?network=ukmesh&snapshot=${encodeURIComponent(snapshot)}&limit=1000`;
  const cases: Case[] = [
    { name: 'map-page', route: map, populated: true },
    { name: 'search-name', route: map + '&q=' + encodeURIComponent(identity.name), populated: true },
    { name: 'search-id', route: map + '&q=' + identity.node_id.slice(0, 12), populated: true },
    { name: 'search-short', route: map + '&q=a' },
    { name: 'search-missing', route: map + '&q=zz_lookup_missing_zz' },
    { name: 'map-observer', route: map + '&observer=' + observer },
    { name: 'node-links', route: `/api/nodes/${identity.node_id}/links?network=ukmesh`, populated: true },
    { name: 'node-history', route: `/api/nodes/${observer}/history?network=ukmesh&hours=672`, populated: true },
    { name: 'packet-detail', route: `/api/packets/${packet.packet_hash}?network=ukmesh` },
    { name: 'packet-missing', route: `/api/packets/${'F'.repeat(64)}?network=ukmesh`, expected: 404 },
  ];
  const region = (await query(`SELECT region FROM core.region_scope_members
    GROUP BY region ORDER BY count(*) DESC,region LIMIT 1`)).rows[0]?.region;
  const regionEnabled = ['1','true','yes','on'].includes(String(process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED']??'').toLowerCase());
  if (region && regionEnabled) cases.push({ name: 'map-region', route: map + '&region=' + encodeURIComponent(region), populated: true });
  if (visible.length > 1000) {
    const cursor = Buffer.from(JSON.stringify({ version: 1, nodeId: visible[999].node_id })).toString('base64url');
    cases.push({ name: 'map-next-page', route: map + '&cursor=' + cursor, populated: true });
  }
  const selected = option(args, 'cases')?.split(',');
  if (selected?.some(name => !cases.some(item => item.name === name))) throw new Error('unknown lookup case');
  const cpuBefore = process.cpuUsage(), resourceStarted = performance.now();
  for (let trial = 0; trial < trials; trial++) {
    const router = Router(), nodeLinksCache = new Map(), inferredNodesCache = new Map();
    const closeNodes = registerNodeRoutes(router, {
      ...nodeReads, nodeRepository: createNodeRepository(runtime.query), networkFilters,
      getPublicVisibilityGeneration: () => readPublicResponseGeneration(runtime.query),
      requireLocalOnly: () => false, nodeLinksCache, nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
      inferredNodesCache, inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
      nodesLimiter: NODES_LIMITER,
    });
    registerMiscRoutes(router, { query: runtime.query, ...packetReads,
      getPublicVisibilityGeneration: () => readPublicResponseGeneration(runtime.query), packetDetailLimiter: PACKET_DETAIL_LIMITER });
    wrapAsyncHandlers(router);
    const app = express(); app.use('/api', createGlobalApiLimiter(), router); app.use(apiErrorMiddleware);
    const server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
    const port = (server.address() as AddressInfo).port;
    try {
      for (const definition of cases.filter(item => !selected || selected.includes(item.name))) {
        const testCase = { ...definition };
        // Long baseline runs must not expire a valid pagination snapshot.
        // Record a fresh request snapshot per case; it is fixed within a trial.
        if (!option(args, 'snapshot') && testCase.route.startsWith('/api/nodes/map?')) {
          const url = new URL(testCase.route, 'http://127.0.0.1');
          url.searchParams.set('snapshot', new Date().toISOString());
          testCase.route = url.pathname + url.search;
        }
        workload = testCase.name;
        for (let warmup = 0; warmup < 3; warmup++) await request(port, testCase);
        const results: Awaited<ReturnType<typeof request>>[] = [];
        let next = 0;
        await Promise.all(Array.from({ length: concurrency }, async () => {
          while (next++ < samples) {
            if (cache === 'miss') { nodeLinksCache.clear(); inferredNodesCache.clear(); }
            results.push(await request(port, testCase));
          }
        }));
        const result = { trial: trial + 1, name: testCase.name, samples: results.length,
          p50Ms: percentile(results.map(row => row.ms), .5), p95Ms: percentile(results.map(row => row.ms), .95),
          p99Ms: percentile(results.map(row => row.ms), .99), bytes: results[0]!.bytes,
          entries: results[0]!.entries, digests: [...new Set(results.map(row => row.digest))],
          dataDigests: [...new Set(results.map(row => row.dataDigest))] };
        report.push(result); console.log(JSON.stringify(result));
      }
    } finally {
      closeNodes(); server.closeAllConnections();
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  }
  const resources = { wallMs: performance.now() - resourceStarted, cpuMicros: process.cpuUsage(cpuBefore),
    memory: process.memoryUsage(), note: 'Handler and loopback load generator share this process; SQL plans exclude this overhead.' };
  const plans = [];
  // Explain outside request timing. These are the real SQL strings and binds.
  if (args.includes('--plans')) for (const item of [...captured.values()].sort((a, b) => b.totalMs - a.totalMs)) {
    const explained = await query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${item.sql}`, item.params);
    plans.push({ workload: item.workload, sql: item.sql, calls: item.calls, totalMs: item.totalMs, plan: explained.rows[0]['QUERY PLAN'][0] });
  }
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, 'lookups.json'), JSON.stringify({ version: 1,
    at: new Date().toISOString(), snapshot, source,
    commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    diffSha256: createHash('sha256').update(execFileSync('git', ['diff', 'HEAD'])).digest('hex'),
    hardware: { cpus: os.cpus().length, model: os.cpus()[0]?.model, memory: os.totalmem() },
    database: (await query('SELECT version() version')).rows[0], counts, resources,
    databaseSettings: (await query(`SELECT name,setting,unit FROM pg_settings WHERE name IN
      ('shared_buffers','work_mem','max_connections','max_parallel_workers_per_gather','statement_timeout') ORDER BY name`)).rows,
    settings: { samples, trials, concurrency, cache, clients: agents.length, productionRateLimits: true, regionEnabled,
      scope: 'ukmesh', note: 'Actual public routers, pool size and privacy delivery gates; excludes unrelated background services. Cache-miss bursts may coalesce identical work.' },
    results: report, queries: [...captured.values()].map(({ params: _params, ...rest }) => rest), plans,
  }, null, 2) + '\n');
  if (budgetMs !== undefined && (!Number.isFinite(budgetMs) || budgetMs <= 0
    || report.some(result => (result as {p95Ms: number}).p95Ms > budgetMs))) {
    throw new Error(`lookup p95 budget ${budgetMs}ms exceeded; see saved evidence`);
  }
} finally {
  for (const agent of agents) agent.destroy();
  await runtime.closeDb();
}
