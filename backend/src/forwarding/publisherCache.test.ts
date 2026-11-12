import assert from 'node:assert/strict';
import test from 'node:test';
import { createPublisherCache } from './publisherCache.js';

test('publisher rotation and invalidation isolate destinations and bound stuck cleanup', async () => {
  const cache = createPublisherCache(5, 'owned cleanup');
  let staleCloses = 0, currentCloses = 0, otherCloses = 0;
  const stale = { publish: async () => {}, close: () => { staleCloses++; return new Promise<void>(() => {}); } };
  const current = { publish: async () => {}, close: async () => { currentCloses++; } };
  const other = { publish: async () => {}, close: async () => { otherCloses++; } };
  assert.equal(await cache.get('one', 'old', () => stale), stale);
  assert.equal(await cache.get('one', 'old', () => { throw new Error('must reuse'); }), stale);
  await cache.get('two', 'fixed', () => other);
  assert.equal(await cache.get('one', 'rotated', () => current), current);
  assert.equal(staleCloses, 1);
  assert.equal(otherCloses, 0);
  await cache.discard('one');
  assert.equal(currentCloses, 1);
  await cache.close();
  assert.equal(otherCloses, 1);
  assert.equal(currentCloses, 1);
});


test('concurrent rotation coalesces one replacement while other destinations remain independent', async () => {
  const cache = createPublisherCache(1_000, 'owned cleanup');
  let release!: () => void, retiring!: () => void, created = 0;
  const retirementStarted = new Promise<void>(resolve => { retiring = resolve; });
  await cache.get('one', 'old', () => ({ publish: async () => {}, close: () => {
    retiring(); return new Promise<void>(resolve => { release = resolve; });
  } }));
  const replacement = { publish: async () => {}, close: async () => {} };
  const create = () => { created++; return replacement; };
  const first = cache.get('one', 'new', create);
  await retirementStarted;
  const second = cache.get('one', 'new', create);
  const other = { publish: async () => {}, close: async () => {} };
  assert.equal(await cache.get('two', 'independent', () => other), other);
  assert.equal(created, 0, 'same-key replacement waits for retirement');
  release();
  assert.deepEqual(await Promise.all([first, second]), [replacement, replacement]);
  assert.equal(created, 1);
  await cache.close();
});

test('close during rotation joins retirement and cannot install a late publisher', async () => {
  const cache = createPublisherCache(1_000, 'owned cleanup');
  let release!: () => void, retiring!: () => void;
  const retirementStarted = new Promise<void>(resolve => { retiring = resolve; });
  await cache.get('one', 'old', () => ({ publish: async () => {}, close: () => {
    retiring(); return new Promise<void>(resolve => { release = resolve; });
  } }));
  let created = 0, stopped = false;
  const rotated = cache.get('one', 'new', () => { created++; return { publish: async () => {}, close: async () => {} }; });
  const rejected = assert.rejects(rotated, /FORWARDING_PUBLISHERS_CLOSED/);
  await retirementStarted;
  const closed = cache.close().then(() => { stopped = true; });
  await Promise.resolve();
  assert.equal(stopped, false, 'close joins the retiring owned publisher');
  release();
  await Promise.all([rejected, closed]);
  assert.equal(created, 0);
  await assert.rejects(cache.get('late', 'new', () => { throw new Error('must not create'); }), /FORWARDING_PUBLISHERS_CLOSED/);
});
