import assert from 'node:assert/strict';
import test from 'node:test';
import { createPublicBatch } from './publicBatch.js';
import { PrivatePrefixCache } from './privatePrefixCache.js';

test('F01 public batches query authority once, preserve FIFO and reserve active payload bytes', async () => {
  const policy = new PrivatePrefixCache(); policy.replace(2, []);
  let release!: (value: PrivatePrefixCache) => void;
  let queries = 0;
  const delivered: number[] = [], refused: string[] = [];
  const batch = createPublicBatch<number>({ maxItems: 3, maxBytes: 30, delayMs: 60_000,
    loadPolicy: () => { queries++; return new Promise((resolve) => { release = resolve; }); },
    deliver: (items) => { delivered.push(...items); }, refused: (reason) => { refused.push(reason); } });
  batch.enqueue(1, 10); batch.enqueue(2, 10);
  const active = batch.flush();
  batch.enqueue(3, 10);
  assert.equal(batch.enqueue(4, 10), false);
  assert.deepEqual(batch.size(), { count: 3, bytes: 30 });
  assert.equal(queries, 1);
  release(policy); await active;
  assert.deepEqual(delivered, [1, 2]);
  const next = batch.flush(); release(policy); await next;
  assert.deepEqual(delivered, [1, 2, 3]);
  assert.deepEqual(refused, ['overflow']);
  assert.deepEqual(batch.size(), { count: 0, bytes: 0 });
  await batch.close();
});

test('F01 unavailable authority and shutdown suppress captured public batches', async () => {
  let resolve!: (value: null) => void;
  let deliveries = 0, refusals = 0;
  const batch = createPublicBatch<number>({ maxItems: 2, maxBytes: 20, delayMs: 60_000,
    loadPolicy: () => new Promise((r) => { resolve = r; }),
    deliver: () => { deliveries++; }, refused: () => { refusals++; } });
  batch.enqueue(1, 10); const active = batch.flush(); resolve(null); await active;
  assert.equal(refusals, 1); assert.equal(deliveries, 0);
  batch.enqueue(2, 10); const held = batch.flush(); const closed = batch.close();
  resolve(null); await held; await closed;
  assert.equal(deliveries, 0); assert.equal(refusals, 1);
  assert.deepEqual(batch.size(), { count: 0, bytes: 0 });
});
