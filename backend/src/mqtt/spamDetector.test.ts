import assert from 'node:assert/strict';
import test from 'node:test';
import type { DatabaseQueryFn } from '../db/index.js';
import { metricsRegistry, spamDetectorRefreshHealthy, spamDetectorSnapshotUpdatedSeconds, spamDetectorRuntimeEntries } from '../metrics.js';
import { evaluateAdvert, refreshSpamDetectorCaches } from './spamDetector.js';

const key = 'A'.repeat(64);
const advert = { name: 'Invented hill', publicKey: key, srcNodeId: key, network: 'ukmesh' };
const emptyQuery: DatabaseQueryFn = async () => ({ rows: [{ row_count: '0', byte_count: '0', snapshot: [] }] as never });
const flood = async () => (await evaluateAdvert(advert)).signals.some(signal => signal.name === 'EXTREME_ADVERT_FLOOD');

test('a flood retains only the latest 31 arrivals and expires against a monotonic clock', async t => {
  let now = 100_000;
  t.mock.method(performance, 'now', () => now);
  await refreshSpamDetectorCaches(emptyQuery);
  for (let i = 0; i < 30; i++) assert.equal(await flood(), false);
  for (let i = 0; i < 50_000; i++) assert.equal(await flood(), true);
  assert.match(await metricsRegistry.metrics(), /meshcore_spam_detector_runtime_entries\{kind="arrivals"\} 31/);
  // Wall clock changes do not extend a runtime window.
  t.mock.method(Date, 'now', () => 0);
  now += 60_000;
  assert.equal(await flood(), false);
  assert.match(await metricsRegistry.metrics(), /meshcore_spam_detector_runtime_entries\{kind="arrivals"\} 1/);
});

test('an unavailable known-ID refresh retains the complete previous location snapshot', async t => {
  t.mock.method(console, 'log', () => undefined);
  t.mock.method(console, 'error', () => undefined);
  const initial: DatabaseQueryFn = async sql => ({ rows: [{
    row_count: '1', byte_count: '100', snapshot: sql.includes('AVG(')
      ? [{ src_node_id: key, avg_lat: 54, avg_lon: -1, avg_hops: 2, advert_count: 20 }]
      : sql.includes('SELECT node_id') ? [{ node_id: key }] : [],
  }] as never });
  await refreshSpamDetectorCaches(initial);
  const lookup = async () => (await evaluateAdvert({ ...advert, lat: 50, lon: -1 })).signals.map(signal => signal.name);
  assert.ok((await lookup()).includes('LOCATION_JUMP_LARGE'));
  const updated = (await spamDetectorSnapshotUpdatedSeconds.get()).values.find(value => value.labels.dataset === 'established')!.value;
  const failed: DatabaseQueryFn = async sql => {
    if (sql.includes('SELECT node_id')) throw new Error('injected known-ID failure');
    return emptyQuery(sql);
  };
  await refreshSpamDetectorCaches(failed);
  assert.ok((await lookup()).includes('LOCATION_JUMP_LARGE'), 'partial refresh must not erase old evidence');
  assert.equal((await spamDetectorRefreshHealthy.get()).values.find(value => value.labels.dataset === 'established')!.value, 0);
  assert.equal((await spamDetectorSnapshotUpdatedSeconds.get()).values.find(value => value.labels.dataset === 'established')!.value, updated);
  await refreshSpamDetectorCaches(emptyQuery);
  assert.equal((await lookup()).includes('LOCATION_JUMP_LARGE'), false, 'a complete empty snapshot replaces old evidence');
});

test('concurrent refresh callers share one bounded refresh', async t => {
  t.mock.method(console, 'log', () => undefined);
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let calls = 0;
  const query: DatabaseQueryFn = async sql => { calls++; await gate; return emptyQuery(sql); };
  const first = refreshSpamDetectorCaches(query);
  const second = refreshSpamDetectorCaches(query);
  assert.equal(first, second);
  release();
  await Promise.all([first, second]);
  assert.equal(calls, 5);
});


test('tracker and name populations remain bounded under distinct-key churn', async t => {
  t.mock.method(performance, 'now', () => 1_000_000);
  for (let i = 0; i < 10_020; i++) {
    const id = i.toString(16).padStart(64, '0').toUpperCase();
    await evaluateAdvert({ ...advert, publicKey: id, srcNodeId: id });
  }
  const count = async (kind: string) => (await spamDetectorRuntimeEntries.get()).values.find(v => v.labels.kind === kind)!.value;
  assert.equal(await count('trackers'), 10_000);
  const id = (10_019).toString(16).padStart(64, '0').toUpperCase();
  for (let i = 0; i < 200; i++) await evaluateAdvert({ ...advert, publicKey: id, srcNodeId: id, name: `Invented ${i}` });
  assert.equal(await count('names'), 10_063);
  assert.equal(await count('arrivals'), 10_030);
});
