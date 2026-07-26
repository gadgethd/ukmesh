import assert from 'node:assert/strict';
import test from 'node:test';
import { HealthSnapshotCache } from './snapshot.js';

test('health snapshot coalesces refreshes and serves only fresh completed data', async () => {
  let now = 10;
  let loads = 0;
  let resolveLoad!: (value: { ok: boolean }) => void;
  const cache = new HealthSnapshotCache(
    () => {
      loads += 1;
      return new Promise<{ ok: boolean }>((resolve) => {
        resolveLoad = resolve;
      });
    },
    100,
    () => now,
  );

  assert.deepEqual(cache.read(), { ready: false, generatedAt: null, lastError: null });
  const first = cache.refresh();
  const second = cache.refresh();
  assert.equal(first, second);
  assert.equal(loads, 1);
  resolveLoad({ ok: true });
  await first;
  assert.deepEqual(cache.read(), { ready: true, generatedAt: 10, data: { ok: true } });
  now = 111;
  assert.deepEqual(cache.read(), { ready: false, generatedAt: 10, lastError: null });
});

test('failed refresh preserves the last successful snapshot until its hard TTL', async () => {
  let now = 0;
  let fail = false;
  const cache = new HealthSnapshotCache(
    async () => {
      if (fail) throw new Error('database unavailable');
      return { ok: true };
    },
    100,
    () => now,
  );

  await cache.refresh();
  fail = true;
  now = 50;
  await cache.refresh();
  assert.deepEqual(cache.read(), { ready: true, generatedAt: 0, data: { ok: true } });
  now = 101;
  assert.deepEqual(cache.read(), {
    ready: false,
    generatedAt: 0,
    lastError: 'database unavailable',
  });
});
