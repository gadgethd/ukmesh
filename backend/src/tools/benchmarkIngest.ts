/** One controlled/saturation replay on a clean fixture; verifies SQL drain. */
import { execFileSync } from 'node:child_process';
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
const pool = new pg.Pool({ connectionString: endpoint, max: 1 });
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function metrics(): Promise<Map<string, number>> {
  const response = await fetch('http://127.0.0.1:9091/metrics', { signal: AbortSignal.timeout(15_000) });
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
  const before = await metrics();
  const started = performance.now();
  await replaySampleMqtt(['--data-dir', directory, '--limit', String(count), '--speed', speed, '--qos', '1', '--concurrency', concurrency]);
  const acknowledgedMs = performance.now() - started;
  const deadline = Date.now() + 120_000;
  let persisted = 0;
  let after = before;
  let drained = false;
  while (Date.now() < deadline) {
    persisted = await rowCount();
    after = await metrics();
    if (persisted === count && (after.get('meshcore_packet_batch_queue_depth') ?? -1) === 0
      && (after.get('meshcore_mqtt_ingest_queue_depth') ?? -1) === 0
      && (after.get('meshcore_mqtt_ingest_active') ?? -1) === 0) { drained = true; break; }
    await wait(250);
  }
  if (persisted !== count || !drained) throw new Error(`replay did not persist/drain expected unique rows: ${persisted}/${count}`);
  const drainedMs = performance.now() - started;
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
    version: 1, at: new Date().toISOString(), commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    node: process.version, count, persisted, acknowledgedMs, drainedMs, persistedPerSecond: count / (drainedMs / 1000),
    enqueueToCommitP95UpperBoundSeconds: p95UpperBound, histogramObservations: observations,
    settings: { speed: Number(speed), concurrency: Number(concurrency), qos: 1, order: 'timestamp ascending', mode: 'durable' },
    beforeMetrics: Object.fromEntries(before), afterMetrics: Object.fromEntries(after), deltas,
  }, null, 2) + '\n');
  console.log(JSON.stringify({ persisted, persistedPerSecond: count / (drainedMs / 1000), enqueueToCommitP95UpperBoundSeconds: p95UpperBound, output }));
} finally { await pool.end(); }
