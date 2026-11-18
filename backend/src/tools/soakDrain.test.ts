import assert from 'node:assert/strict';
import test from 'node:test';
import { waitForSoakDrain } from './soakDrain.js';

const metrics = () => new Map(['meshcore_packet_batch_queue_depth', 'meshcore_mqtt_ingest_queue_depth',
  'meshcore_mqtt_ingest_active', 'meshcore_mqtt_post_persist_inflight'].map(key => [key, 0]));
const before = { packets: 100, paths: 100, facts: 100, ledger: 100 };
const expectedDeltas = { packets: 304, paths: 300, facts: 300, ledger: 304 };
const committed = { packets: 404, paths: 400, facts: 400, ledger: 404 };

test('the last live frame gates the expensive SQL check', async () => {
  let now = 0, seen = 299, polls = 0;
  const result = await waitForSoakDrain({ before, expectedDeltas, count: 300, metrics: async () => metrics(),
    counts: async () => { polls++; return committed; }, seenCount: () => seen },
  { now: () => now, pause: async milliseconds => { now += milliseconds; seen = 300; } });
  assert.equal(polls, 1); assert.deepEqual(result, { drained: true, drainElapsedMs: 1000 });
});

test('post-persist work cannot be mistaken for complete live delivery', async () => {
  let now = 0;
  const result = await waitForSoakDrain({ before, expectedDeltas, count: 300,
    metrics: async () => { const value = metrics(); value.set('meshcore_mqtt_post_persist_inflight', now ? 0 : 1); return value; },
    counts: async () => committed, seenCount: () => 300 },
  { now: () => now, pause: async milliseconds => { now += milliseconds; } });
  assert.equal(result.drainElapsedMs, 1000); assert.equal(result.drained, true);
});

test('an awaited probe completing after120s fails even if every count is then complete', async () => {
  let now = 0;
  const result = await waitForSoakDrain({ before, expectedDeltas, count: 300,
    metrics: async () => { now = 119_000; return metrics(); },
    counts: async () => { now = 120_001; return committed; }, seenCount: () => 300 },
  { now: () => now, pause: async () => assert.fail('late completion must not open another window') });
  assert.deepEqual(result, { drained: false, drainElapsedMs: 120_001 });
});

test('a missing durable observation cannot pass because the packet count and live feed match', async () => {
  let now = 0;
  const result = await waitForSoakDrain({ before, expectedDeltas, count: 300, metrics: async () => metrics(),
    counts: async () => ({ ...committed, ledger: 403 }), seenCount: () => 300 },
  { now: () => now, pause: async () => { now = 120_000; } });
  assert.deepEqual(result, { drained: false, drainElapsedMs: 120_000 });
});
