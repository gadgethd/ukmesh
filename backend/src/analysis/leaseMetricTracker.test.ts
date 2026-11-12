import assert from 'node:assert/strict';
import test from 'node:test';
import { LeaseMetricTracker } from './leaseMetricTracker.js';

function fixture(capacity = 4_096) {
  let now = 0;
  const counts = new Map<string, number>();
  const drops = { capacity: 0, expired: 0 };
  const tracker = new LeaseMetricTracker(capacity,
    (label, count) => { counts.set(label, count); },
    (_label, reason) => { drops[reason]++; }, () => now);
  return { tracker, counts, drops, at: (time: number) => { now = time; } };
}

test('renewals retain one observation and expire at the hard deadline', () => {
  const f = fixture();
  f.tracker.track('spam-analysis', 'run', 10, 25);
  f.at(5);
  f.tracker.track('spam-analysis', 'run', 20, 25);
  f.tracker.sweep(10);
  assert.equal(f.tracker.size, 1);
  assert.equal(f.counts.get('spam-analysis'), 1);
  f.at(15);
  f.tracker.track('spam-analysis', 'run', 100, 25);
  f.tracker.sweep(25);
  assert.equal(f.tracker.size, 0);
  assert.equal(f.counts.get('spam-analysis'), 0);
  assert.equal(f.drops.expired, 1);
  f.tracker.remove('run');
  assert.equal(f.counts.get('spam-analysis'), 0);
});

test('capacity eviction preserves other workloads and bounded labels', () => {
  const f = fixture(2);
  f.tracker.track('spam-analysis', 'a', 100, 100);
  f.tracker.track('path-learning', 'b', 100, 100);
  f.tracker.track('unknown-private-scope', 'c', 100, 100);
  assert.equal(f.tracker.size, 2);
  assert.equal(f.counts.get('spam-analysis'), 0);
  assert.equal(f.counts.get('path-learning'), 1);
  assert.equal(f.counts.get('other'), 1);
  assert.equal(f.counts.has('unknown-private-scope'), false);
  assert.equal(f.drops.capacity, 1);
  f.tracker.remove('a');
  assert.equal(f.tracker.size, 2);
});

test('50k abandoned terminal writes stay bounded and expire while idle', () => {
  const f = fixture();
  for (let i = 0; i < 50_000; i++) f.tracker.track('spam-analysis', `run-${i}`, 60_000, 120_000);
  assert.equal(f.tracker.size, 4_096);
  assert.equal(f.counts.get('spam-analysis'), 4_096);
  assert.equal(f.drops.capacity, 50_000 - 4_096);
  f.tracker.sweep(60_000);
  assert.equal(f.tracker.size, 0);
  assert.equal(f.counts.get('spam-analysis'), 0);
  assert.equal(f.drops.expired, 4_096);
});

test('invalid or elapsed observations cannot stay retained', () => {
  const f = fixture();
  for (const expiry of [NaN, Infinity, -1, 0]) f.tracker.track('spam-analysis', 'run', expiry, expiry);
  assert.equal(f.tracker.size, 0);
  assert.throws(() => fixture(Infinity), /INVALID_LEASE_METRIC_CAPACITY/);
});
