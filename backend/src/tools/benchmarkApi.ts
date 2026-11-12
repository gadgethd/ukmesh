/** Sequential public-route benchmark. Keeps bodies out of the evidence. */
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import pg from 'pg';
import { assertLocalDatabaseUrl, option } from './sampleData.js';
import { refreshSampleRecent } from './refreshSampleRecent.js';

const routes = ['/stats/charts', '/nodes/map', '/regions/summary', '/regions/repeaters', '/topology',
  '/spam/messages/status', '/runtime-config', '/packets/recent', '/observer-activity', '/stats'];
const args = process.argv.slice(2);
const base = new URL(option(args, 'base-url') ?? 'http://127.0.0.1:3100');
if (!['localhost', '127.0.0.1', '[::1]'].includes(base.hostname)) throw new Error('benchmark requires a loopback API');
const runs = Number(option(args, 'runs') ?? 10);
const warmups = Number(option(args, 'warmups') ?? 2);
if (!Number.isSafeInteger(runs) || runs < 3 || runs > 1000 || !Number.isSafeInteger(warmups) || warmups < 0 || warmups > 10) {
  throw new Error('runs must be 3..1000 and warmups 0..10');
}
const directory = path.resolve(option(args, 'output') ?? '../plans/measurements/api-' + Date.now());
const database = process.env['DATABASE_URL'];
if (!database) throw new Error('owned DATABASE_URL required for fixture/query evidence');
assertLocalDatabaseUrl(database);
const pool = new pg.Pool({ connectionString: database, max: 1 });
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

try {
  await mkdir(directory, { recursive: true });
  await refreshSampleRecent(database);
  const commit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  const diff = execFileSync('git', ['diff', 'HEAD'], { maxBuffer: 16 * 1024 * 1024 });
  const counts = (await pool.query(`SELECT 'nodes' AS relation, COUNT(*)::text AS rows FROM core.nodes
    UNION ALL SELECT 'packets', COUNT(*)::text FROM mesh.packets
    UNION ALL SELECT 'paths', COUNT(*)::text FROM mesh.packet_paths
    UNION ALL SELECT 'facts', COUNT(*)::text FROM mesh.multibyte_path_facts
    UNION ALL SELECT 'hourly', COUNT(*)::text FROM mesh.packet_hourly_stats
    UNION ALL SELECT 'region_members', COUNT(*)::text FROM core.region_scope_members`)).rows;
  const bounds = (await pool.query('SELECT MIN(time), MAX(time) FROM mesh.packets')).rows;
  const queryStats = async () => (await pool.query<{ queryid: string; calls: string; total_exec_time: number; rows: string }>(
    `SELECT queryid::text, calls::text, total_exec_time, rows::text FROM pg_stat_statements
     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())`,
  )).rows;
  const before = new Map((await queryStats()).map((row) => [row.queryid, row]));
  const measurements = [];
  for (const route of routes) {
    if (route === '/packets/recent') await refreshSampleRecent(database);
    const samples = [];
    let rateWaits = 0;
    for (let index = -warmups; index < runs; index += 1) {
      const url = new URL('/api' + route, base);
      if (route !== '/runtime-config') url.searchParams.set('network', 'ukmesh');
      const started = performance.now();
      const response = await fetch(url, { signal: AbortSignal.timeout(120_000), headers: { 'accept-encoding': 'gzip' } });
      if (response.status === 429 && rateWaits < 10) {
        await response.body?.cancel();
        rateWaits += 1;
        await wait(Math.min(65_000, Math.max(1_000, Number(response.headers.get('retry-after') ?? 60) * 1000)));
        index -= 1;
        continue;
      }
      const chunks: Uint8Array[] = [];
      let bytes = 0;
      if (!response.body) throw new Error(`missing body: ${route}`);
      for await (const chunk of response.body) {
        bytes += chunk.byteLength;
        if (bytes > 8 * 1024 * 1024) throw new Error(`benchmark payload limit: ${route}`);
        chunks.push(chunk);
      }
      const body = Buffer.concat(chunks);
      if (!response.ok) throw new Error(`${route} returned ${response.status}`);
      const parseStart = performance.now();
      const value = JSON.parse(body.toString()) as unknown;
      if (route === '/packets/recent' && (!Array.isArray(value) || value.length === 0)) {
        throw new Error('recent-packets fixture is empty; no valid benchmark result can be recorded');
      }
      const parseMs = performance.now() - parseStart;
      if (index >= 0) samples.push({ ms: parseStart - started, parseMs, decodedBytes: bytes,
        transferredBytes: Number(response.headers.get('content-length')) || null,
        etag: Boolean(response.headers.get('etag')), cacheControl: response.headers.get('cache-control'),
        entries: Array.isArray(value) ? value.length : null });
    }
    const sorted = samples.map((sample) => sample.ms).sort((a, b) => a - b);
    const result = { route, p50Ms: sorted[Math.ceil(sorted.length * 0.5) - 1],
      p95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1], rateWaits, samples };
    measurements.push(result);
    console.log(`${route}: p95=${result.p95Ms!.toFixed(1)}ms, bytes=${samples[0]!.decodedBytes}`);
  }
  const queries = (await queryStats()).map((row) => {
    const previous = before.get(row.queryid);
    const calls = Number(row.calls) - Number(previous?.calls ?? 0);
    const totalMs = row.total_exec_time - (previous?.total_exec_time ?? 0);
    return { queryid: row.queryid, calls, totalMs, meanMs: calls > 0 ? totalMs / calls : 0 };
  }).filter((row) => row.calls > 0).sort((a, b) => b.totalMs - a.totalMs);
  await writeFile(path.join(directory, 'api.json'), JSON.stringify({
    version: 1, at: new Date().toISOString(), commit, dirty: diff.length > 0,
    diffSha256: createHash('sha256').update(diff).digest('hex'), node: process.version,
    hardware: { cpus: os.cpus().length, model: os.cpus()[0]?.model, memory: os.totalmem(), platform: os.platform() },
    settings: { runs, warmups, concurrency: 1, scope: 'ukmesh', cache: 'warm after explicit warmups' },
    counts, bounds, measurements, queries,
  }, null, 2) + '\n');
  console.log(`Evidence: ${path.join(directory, 'api.json')}`);
} finally { await pool.end(); }
