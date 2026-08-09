import assert from 'node:assert/strict';
import test from 'node:test';
import {
  LINK_V3_ADMIT_SCRIPT,
  LINK_V3_KEYS,
  LinkQueueV3Model,
  linkObservationIdentity,
} from './linkQueueV3.js';

test('v3 admission emits one bounded blocking-wake token', () => {
  assert.equal(LINK_V3_KEYS.wake, 'meshcore:link:v3:wake');
  assert.match(LINK_V3_ADMIT_SCRIPT, /LPUSH', KEYS\[13\], '1'/);
  assert.match(LINK_V3_ADMIT_SCRIPT, /LTRIM', KEYS\[13\], 0, 0/);
});

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

test('terminal jobs release active capacity and use separate requeueable DLQ accounting', () => {
  const model = new LinkQueueV3Model(1, 10, 1);
  assert.equal(model.admit('a', 'observe:a', 7), 'accepted');
  assert.equal(model.claim('worker', 0, 10), 'a');
  assert.equal(model.nack('a', 'worker'), 'dead');
  assert.deepEqual(
    { count: model.count, bytes: model.bytes, deadCount: model.deadCount, deadBytes: model.deadBytes },
    { count: 0, bytes: 0, deadCount: 1, deadBytes: 7 },
  );
  assert.equal(model.admit('b', 'observe:b', 10), 'accepted');
  assert.equal(model.requeueDead('a'), false);
  assert.equal(model.claim('worker-2', 0, 10), 'b');
  assert.equal(model.ack('b', 'worker-2'), true);
  assert.equal(model.requeueDead('a'), true);
  assert.equal(model.purgeDead('a'), false);
  assert.equal(model.claim('worker-3', 0, 10), 'a');
  assert.equal(model.nack('a', 'worker-3'), 'dead');
  assert.equal(model.purgeDead('a'), true);
  assert.deepEqual({ deadCount: model.deadCount, deadBytes: model.deadBytes }, { deadCount: 0, deadBytes: 0 });
});

test('randomized queue transitions preserve active and DLQ count/byte invariants', () => {
  const model = new LinkQueueV3Model(50, 4_000, 3);
  let seed = 0x20260729;
  let nextJob = 0;
  let now = 0;
  const random = (): number => {
    seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
    return seed / 0x1_0000_0000;
  };

  for (let step = 0; step < 5_000; step += 1) {
    now += 1;
    const action = Math.floor(random() * 8);
    if (action <= 2) {
      nextJob += 1;
      model.admit(`job-${nextJob}`, `dedupe-${nextJob}`, 1 + Math.floor(random() * 100));
    } else if (action <= 4) {
      const token = `token-${step}`;
      const claimed = model.claim(token, now, 10);
      if (claimed) {
        if (action === 3) model.ack(claimed, token);
        else model.nack(claimed, token);
      }
    } else if (action === 5) {
      model.reap(now);
    } else if (action === 6 && model.dead.length > 0) {
      model.requeueDead(model.dead[Math.floor(random() * model.dead.length)]!);
    } else if (model.dead.length > 0) {
      model.purgeDead(model.dead[Math.floor(random() * model.dead.length)]!);
    }

    const jobs = [...model.jobs.values()];
    const active = jobs.filter((job) => job.state === 'queued' || job.state === 'in_flight');
    const dead = jobs.filter((job) => job.state === 'dead');
    assert.deepEqual(
      {
        count: model.count,
        bytes: model.bytes,
        deadCount: model.deadCount,
        deadBytes: model.deadBytes,
      },
      {
        count: active.length,
        bytes: active.reduce((total, job) => total + job.bytes, 0),
        deadCount: dead.length,
        deadBytes: dead.reduce((total, job) => total + job.bytes, 0),
      },
    );
  }
});
