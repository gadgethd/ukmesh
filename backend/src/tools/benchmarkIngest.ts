/** One controlled/saturation replay on a clean fixture; verifies SQL drain. */
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import os from 'node:os';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import pg from 'pg';
import { assertLocalDatabaseUrl, option, requiredOption } from './sampleData.js';
import { replaySampleMqtt } from './replaySampleMqtt.js';

const args = process.argv.slice(2);
const endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
const directory = requiredOption(args, 'data-dir');
const count = Number(option(args, 'limit') ?? 200);
if (!Number.isSafeInteger(count) || count < 1 || count > 1_000_000) throw new Error('--limit must be 1..1000000');
const output = path.resolve(option(args, 'output') ?? `../plans/measurements/ingest-${Date.now()}`);
const speed = option(args, 'speed') ?? '0';
const concurrency = option(args, 'concurrency') ?? '16';
const metricsUrl = option(args, 'metrics-url') ?? 'http://127.0.0.1:9091/metrics';
const broker = option(args, 'broker') ?? 'ws://127.0.0.1:9002';
for (const endpoint of [metricsUrl, broker]) {
  if (!['127.0.0.1', 'localhost', '[::1]'].includes(new URL(endpoint).hostname)) throw new Error('benchmark requires loopback endpoints');
}
const pool = new pg.Pool({ connectionString: endpoint, max: 1 });
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function metrics(): Promise<Map<string, number>> {
  const response = await fetch(metricsUrl, { signal: AbortSignal.timeout(15_000) });
  if (!response.ok) throw new Error('local metrics endpoint unavailable');
  const text = await response.text();
  if (text.length > 1024 * 1024) throw new Error('metrics body exceeds local benchmark cap');
  return new Map(text.split('\n').flatMap((line) => {
    const match = /^(meshcore_[a-zA-Z0-9_]+(?:\{[^}]*\})?) ([0-9.eE+\-]+)$/.exec(line);
    return match ? [[match[1]!, Number(match[2])]] : [];
  }));
}

try {
  const rowCount = async () => Number((await pool.query('SELECT COUNT(*)::text AS n FROM mesh.packets')).rows[0].n);
  if (await rowCount() !== 0) throw new Error('ingest benchmark requires a fresh fixture with no packets; reset backend/Redis too');
  const queryStats = async () => (await pool.query<{
    queryid: string; query: string; calls: string; total_exec_time: number; rows: string;
  }>(`SELECT queryid::text, query, calls::text, total_exec_time, rows::text FROM pg_stat_statements
       WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())`)).rows;
  const beforeQueries = new Map((await queryStats()).map(row => [row.queryid, row]));
  const walBefore = String((await pool.query('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0].lsn);
  const before = await metrics();
  const samples: { elapsedMs: number; values: Record<string, number> }[] = [];
  // Cover the plan's maximum one-hour soak and bounded drain without growing
  // the collector indefinitely on longer ad-hoc replays.
  const sampleIntervalMs = 250;
  const maximumSamples = 15_000;
  let sampling = false;
  const started = performance.now();
  const timer = setInterval(() => {
    if (sampling || samples.length >= maximumSamples) return;
    sampling = true;
    void metrics().then(values => samples.push({ elapsedMs: performance.now() - started,
      values: Object.fromEntries([...values].filter(([key]) => /resident_memory|heap_size_used|eventloop_lag|queue_depth|mqtt_ingest_active|cache_entries|cache_bytes|mqtt_dedup_entries|mqtt_pre_resolve_history_entries|mqtt_post_persist_inflight/.test(key))),
    })).catch(() => undefined).finally(() => { sampling = false; });
  }, sampleIntervalMs);
  let acknowledgedMs: number;
  let persisted = 0;
  let after = before;
  let drained = false;
  try {
  await replaySampleMqtt(['--data-dir', directory, '--limit', String(count), '--speed', speed, '--qos', '1', '--concurrency', concurrency, '--broker', broker]);
  acknowledgedMs = performance.now() - started;
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    persisted = await rowCount();
    after = await metrics();
    if (persisted === count && (after.get('meshcore_packet_batch_queue_depth') ?? -1) === 0
      && (after.get('meshcore_mqtt_ingest_queue_depth') ?? -1) === 0
      && (after.get('meshcore_mqtt_ingest_active') ?? -1) === 0) { drained = true; break; }
    await wait(250);
  }
  } finally { clearInterval(timer); }
  if (persisted !== count || !drained) throw new Error(`replay did not persist/drain expected unique rows: ${persisted}/${count}`);
  const drainedMs = performance.now() - started;
  const walBytes = Number((await pool.query('SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1)::text AS bytes', [walBefore])).rows[0].bytes);
  const queries = (await queryStats()).map(row => {
    const previous = beforeQueries.get(row.queryid);
    const calls = Number(row.calls) - Number(previous?.calls ?? 0);
    const totalMs = row.total_exec_time - (previous?.total_exec_time ?? 0);
    return { queryid: row.queryid, query: row.query, calls, totalMs, meanMs: calls > 0 ? totalMs / calls : 0 };
  }).filter(row => row.calls > 0).sort((a, b) => b.totalMs - a.totalMs);
  const deltas = [...after].map(([key, value]) => ({ key, value: value - (before.get(key) ?? 0) }))
    .filter(({ key }) => /packet_batch|mqtt_ingest|process_resident|process_heap|eventloop|cache/.test(key));
  const prefix = 'meshcore_packet_batch_queue_wait_seconds';
  const observations = (after.get(prefix + '_count') ?? 0) - (before.get(prefix + '_count') ?? 0);
  const buckets = deltas.flatMap(({ key, value }) => {
    const match = /^meshcore_packet_batch_queue_wait_seconds_bucket\{le="([^"]+)"\}$/.exec(key);
    return match ? [{ upperBound: Number(match[1]), count: value }] : [];
  }).filter((row) => Number.isFinite(row.upperBound)).sort((a, b) => a.upperBound - b.upperBound);
  const p95UpperBound = buckets.find((row) => row.count >= observations * 0.95)?.upperBound;
  if (observations < count || p95UpperBound === undefined) throw new Error('missing enqueue-to-commit histogram evidence');
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, 'ingest.json'), JSON.stringify({
    version: 2, at: new Date().toISOString(), commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    serverSourceRef: option(args, 'server-source-ref') ?? null,
    diffSha256: createHash('sha256').update(execFileSync('git', ['diff', 'HEAD'], { maxBuffer: 16 * 1024 * 1024 })).digest('hex'),
    hardware: { cpus: os.cpus().length, model: os.cpus()[0]?.model, memory: os.totalmem() },
    node: process.version, count, persisted, acknowledgedMs, drainedMs, persistedPerSecond: count / (drainedMs / 1000),
    enqueueToCommitP95UpperBoundSeconds: p95UpperBound, histogramObservations: observations,
    settings: { speed: Number(speed), concurrency: Number(concurrency), qos: 1, order: 'timestamp ascending', mode: 'durable' },
    beforeMetrics: Object.fromEntries(before), afterMetrics: Object.fromEntries(after), deltas,
    // Gauges are time samples, not counter deltas. SQL rankings exclude fixture preparation.
    sampleCollection: { intervalMs: sampleIntervalMs, maximumSamples,
      observedSpanMs: samples.at(-1)?.elapsedMs ?? 0 },
    samples, walBytes, walBytesPerPacket: walBytes / count, queries,
  }, null, 2) + '\n');
  console.log(JSON.stringify({ persisted, persistedPerSecond: count / (drainedMs / 1000), enqueueToCommitP95UpperBoundSeconds: p95UpperBound, output }));
} finally { await pool.end(); }
