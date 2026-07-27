import assert from 'node:assert/strict';
import test from 'node:test';
import { LinkQueueV3Model, linkObservationIdentity } from './linkQueueV3.js';

test('link queue accounting stays exact across admission, retry, and ACK', () => {
  const model = new LinkQueueV3Model(2, 20, 3);
  assert.equal(model.admit('a', 'observe:a', 7), 'accepted');
  assert.equal(model.admit('a', 'observe:a', 7), 'coalesced');
  assert.equal(model.admit('b', 'observe:b', 9), 'accepted');
  assert.equal(model.admit('c', 'observe:c', 1), 'full');
  assert.deepEqual({ count: model.count, bytes: model.bytes }, { count: 2, bytes: 16 });

  assert.equal(model.claim('token-1', 0, 10), 'a');
  assert.equal(model.nack('a', 'wrong-token'), 'invalid');
  assert.equal(model.nack('a', 'token-1'), 'retry');
  assert.equal(model.claim('token-2', 1, 10), 'b');
  assert.equal(model.ack('b', 'token-2'), true);
  assert.deepEqual({ count: model.count, bytes: model.bytes }, { count: 1, bytes: 7 });
});

test('expired leases recover after a quick restart without orphaning capacity', () => {
  const model = new LinkQueueV3Model(1, 100);
  model.admit('a', 'observe:a', 10);
  assert.equal(model.claim('old-worker', 100, 50), 'a');
  assert.equal(model.reap(149), 0);
  assert.equal(model.reap(150), 1);
  assert.equal(model.claim('new-worker', 151, 50), 'a');
  assert.equal(model.ack('a', 'new-worker'), true);
  assert.deepEqual({ count: model.count, bytes: model.bytes }, { count: 0, bytes: 0 });
});

test('crash-safe claim has no state where a job leaves ready without a lease', () => {
  const model = new LinkQueueV3Model(1, 100);
  model.admit('a', 'observe:a', 10);
  const claimed = model.claim('worker', 0, 10);
  assert.equal(claimed, 'a');
  assert.equal(model.jobs.get('a')?.state, 'in_flight');
  assert.equal(model.jobs.get('a')?.leaseUntil, 10);
});

test('rebuild defers live jobs and releases every accepted job after publication', () => {
  const model = new LinkQueueV3Model(3, 100);
  model.rebuildActive = true;
  assert.equal(model.admit('live', 'observe:live', 10), 'accepted');
  assert.equal(model.admit('generation', 'observe:generation', 10, true), 'accepted');
  assert.equal(model.claim('worker', 0, 10), 'generation');
  assert.equal(model.ack('generation', 'worker'), true);
  assert.equal(model.releaseDeferred(), 1);
  assert.equal(model.claim('worker', 1, 10), 'live');
});

test('observation identity is stable for replay but isolated by receiver', () => {
  const first = linkObservationIdentity('ABCD', '11');
  assert.deepEqual(first, linkObservationIdentity('abcd', '11'));
  assert.notEqual(first.jobId, linkObservationIdentity('abcd', '22').jobId);
  assert.notEqual(first.jobId, linkObservationIdentity('abcd', '11', 'link_rebuild_0123456789abcdef').jobId);
});
