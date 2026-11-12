import assert from 'node:assert/strict';
import test from 'node:test';
import { createActivityTimelineCache } from './activityTimelineCache.js';

const settle = () => new Promise<void>(resolve => setImmediate(resolve));
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

test('timeline fills coalesce and completed entries remain scoped', async t => {
  const cache = createActivityTimelineCache(); t.after(cache.shutdown);
  const gate = deferred<unknown>(); let loads = 0;
  const load = () => { loads++; return gate.promise; };
  const first = cache.get('ukmesh:360:15:v1', 1, async () => 1, load);
  const second = cache.get('ukmesh:360:15:v1', 1, async () => 1, load);
  assert.equal(loads, 1); assert.equal(cache.metrics().inflight, 1);
  gate.resolve({ scope: 'first' });
  assert.deepEqual(await first, { scope: 'first' }); assert.deepEqual(await second, await first);
  assert.deepEqual(await cache.get('ukmesh:360:15:v1', 1, async () => 1, async () => assert.fail('fresh hit')), await first);
  for (const key of ['test:360:15:v1', 'ukmesh:60:15:v1', 'ukmesh:360:5:v1', 'ukmesh:360:15:v2']) {
    const generation = key.endsWith('v2') ? 2 : 1;
    assert.deepEqual(await cache.get(key, generation, async () => generation, async () => ({ scope: key })), { scope: key });
  }
});

test('a stale complete snapshot returns immediately while exactly one refresh runs', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const cache = createActivityTimelineCache(); t.after(cache.shutdown);
  await cache.get('scope:v1', 1, async () => 1, async () => ({ version: 1 }));
  t.mock.timers.setTime(31_000);
  const gate = deferred<unknown>(); let loads = 0;
  const load = () => { loads++; return gate.promise; };
  assert.deepEqual(await cache.get('scope:v1', 1, async () => 1, load), { version: 1 });
  assert.deepEqual(await cache.get('scope:v1', 1, async () => 1, load), { version: 1 });
  assert.equal(loads, 1); assert.equal(cache.metrics().inflight, 1);
  gate.resolve({ version: 2 }); await settle();
  assert.deepEqual(await cache.get('scope:v1', 1, async () => 1, async () => assert.fail('fresh hit')), { version: 2 });
});

test('refresh failure never extends maximum age or turns an error into cached data', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 }); t.mock.method(console, 'error', () => undefined);
  const cache = createActivityTimelineCache(); t.after(cache.shutdown);
  await cache.get('scope:v1', 1, async () => 1, async () => ({ version: 1 }));
  const fail = async () => { throw new Error('invented SQL failure'); };
  for (const time of [31_000, 89_000]) {
    t.mock.timers.setTime(time);
    assert.deepEqual(await cache.get('scope:v1', 1, async () => 1, fail), { version: 1 });
    await settle(); assert.equal(cache.metrics().inflight, 0);
  }
  t.mock.timers.setTime(91_000);
  await assert.rejects(cache.get('scope:v1', 1, async () => 1, fail), /invented SQL failure/);
  assert.equal(cache.metrics().size, 0); assert.equal(cache.metrics().inflight, 0);
});

test('changed or unavailable publication authority cannot publish a background result', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 }); t.mock.method(console, 'error', () => undefined);
  const cache = createActivityTimelineCache(); t.after(cache.shutdown);
  await cache.get('scope:v1', 1, async () => 1, async () => ({ version: 1 }));
  for (const read of [async () => 2, async (): Promise<number> => { throw new Error('authority unavailable'); }]) {
    t.mock.timers.setTime(Date.now() + 31_000);
    assert.deepEqual(await cache.get('scope:v1', 1, read, async () => ({ version: 'unpublished' })), { version: 1 });
    await settle();
    assert.deepEqual(await cache.get('scope:v1', 1, read, async () => ({ version: 'unpublished' })), { version: 1 });
    await settle();
  }
  assert.deepEqual(await cache.get('scope:v2', 2, async () => 2, async () => ({ version: 2 })), { version: 2 });
});

test('retained scopes and detached refreshes stay bounded through a burst', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const cache = createActivityTimelineCache(); t.after(cache.shutdown);
  for (let i = 0; i < 20; i++) await cache.get(`scope${i}:v1`, 1, async () => 1, async () => ({ index: i }));
  assert.equal(cache.metrics().size, 16); assert.equal(cache.metrics().evictions, 4);
  t.mock.timers.setTime(31_000);
  const gates = Array.from({ length: 4 }, () => deferred<unknown>()); let loads = 0;
  for (let i = 4; i < 20; i++) {
    assert.deepEqual(await cache.get(`scope${i}:v1`, 1, async () => 1, () => gates[loads++]!.promise), { index: i });
  }
  assert.equal(loads, 4); assert.equal(cache.metrics().inflight, 4);
  // A cold request at capacity follows its original awaited read; it creates
  // neither another detached refresh nor another retained cache entry.
  assert.deepEqual(await cache.get('uncached:v1', 1, async () => 1, async () => ({ cold: true })), { cold: true });
  assert.equal(cache.metrics().inflight, 4); assert.equal(cache.metrics().size, 16);
  gates.forEach((gate, index) => gate.resolve({ index: index + 4 })); await settle();
  assert.equal(cache.metrics().inflight, 0);
});

test('shutdown releases retention and a late fill cannot repopulate it', async () => {
  const cache = createActivityTimelineCache(); const gate = deferred<unknown>();
  const pending = cache.get('scope:v1', 1, async () => 1, () => gate.promise);
  cache.shutdown(); gate.resolve({ version: 1 }); await pending;
  assert.equal(cache.metrics().size, 0); assert.equal(cache.metrics().inflight, 0);
});
