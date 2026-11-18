import assert from 'node:assert/strict';
import test from 'node:test';
import { waitForSoakDrain } from './soakDrain.js';

const metrics = () => new Map(['meshcore_packet_batch_queue_depth', 'meshcore_mqtt_ingest_queue_depth',
  'meshcore_mqtt_ingest_active', 'meshcore_mqtt_post_persist_inflight'].map(key => [key, 0]));
test('the last live frame gates the expensive SQL check', async () => {
  let now = 0, seen = 299, polls = 0;
  const result = await waitForSoakDrain({ count: 300, metrics: async () => metrics(),
    verifyCommit: async () => { polls++; return true; }, seenCount: () => seen },
  { now: () => now, pause: async milliseconds => { now += milliseconds; seen = 300; } });
  assert.equal(polls, 1); assert.deepEqual(result, { drained: true, drainElapsedMs: 1000 });
});

test('post-persist work cannot be mistaken for complete live delivery', async () => {
  let now = 0;
  const result = await waitForSoakDrain({ count: 300,
    metrics: async () => { const value = metrics(); value.set('meshcore_mqtt_post_persist_inflight', now ? 0 : 1); return value; },
    verifyCommit: async () => true, seenCount: () => 300 },
  { now: () => now, pause: async milliseconds => { now += milliseconds; } });
  assert.equal(result.drainElapsedMs, 1000); assert.equal(result.drained, true);
});

test('an awaited probe completing after120s fails even if every count is then complete', async () => {
  let now = 0;
  const result = await waitForSoakDrain({ count: 300,
    metrics: async () => { now = 119_000; return metrics(); },
    verifyCommit: async () => { now = 120_001; return true; }, seenCount: () => 300 },
  { now: () => now, pause: async () => assert.fail('late completion must not open another window') });
  assert.deepEqual(result, { drained: false, drainElapsedMs: 120_001 });
});

test('receipt time after ACK completion consumes the same drain deadline', async () => {
  let now = 119_900;
  const result = await waitForSoakDrain({ count: 300, acknowledgedAt: 0,
    metrics: async () => metrics(), seenCount: () => 300,
    verifyCommit: async () => { now = 120_001; return true; } },
    { now: () => now, pause: async () => assert.fail('receipt I/O cannot reopen the deadline') });
  assert.deepEqual(result, { drained: false, drainElapsedMs: 120_001 });
});

test('a missing durable observation cannot pass because the packet count and live feed match', async () => {
  let now = 0;
  const result = await waitForSoakDrain({ count: 300, metrics: async () => metrics(),
    verifyCommit: async () => false, seenCount: () => 300 },
  { now: () => now, pause: async () => { now = 120_000; } });
  assert.deepEqual(result, { drained: false, drainElapsedMs: 120_000 });
});
