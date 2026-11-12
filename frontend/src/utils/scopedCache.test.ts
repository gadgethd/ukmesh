import assert from 'node:assert/strict';
import test from 'node:test';
import { ScopedCache } from './scopedCache.js';

test('scoped cache enforces TTL, LRU count/byte bounds, and scope invalidation', () => {
  let now = 0;
  const evicted: string[] = [];
  const cache = new ScopedCache<string>({
    name: 'test',
    ttlMs: 100,
    maxEntries: 2,
    maxBytes: 6,
    maxInflight: 2,
    estimateBytes: (value) => value.length,
    onEvict: (value, reason) => evicted.push(`${value}:${reason}`),
    now: () => now,
  });
  cache.set('a', 'one', '111');
  cache.set('a', 'two', '22');
  assert.equal(cache.get('a', 'one'), '111');
  cache.set('b', 'three', '3');
  assert.equal(cache.peek('a', 'two'), undefined, 'least-recently-used entry must be evicted');
  assert.equal(cache.snapshot().entries, 2);
  assert.equal(cache.invalidateScope('a'), 1);
  assert.equal(cache.peek('b', 'three'), '3');
  now = 101;
  assert.equal(cache.get('b', 'three'), undefined);
  assert.ok(evicted.includes('22:capacity'));
  assert.ok(evicted.includes('3:expired'));
});

test('scoped cache single-flights loaders and rejects work above its inflight cap', async () => {
  const cache = new ScopedCache<number>({
    name: 'singleflight',
    ttlMs: 100,
    maxEntries: 4,
    maxBytes: 1_024,
    maxInflight: 1,
  });
  let resolve!: (value: number) => void;
  let loads = 0;
  const loader = () => {
    loads += 1;
    return new Promise<number>((done) => { resolve = done; });
  };
  const first = cache.getOrLoad('scope', 'same', loader);
  const second = cache.getOrLoad('scope', 'same', loader);
  assert.equal(loads, 1);
  await assert.rejects(
    cache.getOrLoad('scope', 'different', async () => 2),
    /inflight limit exceeded/,
  );
  resolve(7);
  assert.deepEqual(await Promise.all([first, second]), [7, 7]);
  assert.equal(cache.get('scope', 'same'), 7);
  assert.equal(cache.snapshot().rejectedLoads, 1);
});

test('detaching one consumer leaves the session-owned request available to another', async () => {
  const cache = new ScopedCache<number>({ name: 'detach', ttlMs: 100, maxEntries: 4, maxBytes: 100, maxInflight: 1 });
  const consumer = new AbortController();
  let resolve!: (value: number) => void;
  let requestSignal!: AbortSignal;
  let calls = 0;
  const loader = (signal: AbortSignal) => {
    calls++;
    requestSignal = signal;
    return new Promise<number>(done => { resolve = done; });
  };
  const first = cache.getOrLoad('scope', 'same', loader, consumer.signal);
  const second = cache.getOrLoad('scope', 'same', loader);
  consumer.abort(new DOMException('Unmounted', 'AbortError'));
  await assert.rejects(first, { name: 'AbortError' });
  assert.equal(requestSignal.aborted, false);
  assert.equal(calls, 1);
  resolve(9);
  assert.equal(await second, 9);
  assert.equal(cache.get('scope', 'same'), 9);
});

test('invalidated abort-ignoring requests remain counted and cannot publish', async () => {
  const cache = new ScopedCache<number>({ name: 'physical-cap', ttlMs: 100, maxEntries: 4, maxBytes: 100, maxInflight: 1 });
  let resolve!: (value: number) => void;
  let requestSignal!: AbortSignal;
  const pending = cache.getOrLoad('old', 'same', signal => {
    requestSignal = signal;
    return new Promise<number>(done => { resolve = done; });
  });
  cache.invalidateScope('old');
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(requestSignal.aborted, true);
  assert.equal(cache.snapshot().inflight, 1);
  await assert.rejects(cache.getOrLoad('new', 'other', async () => 2), /inflight limit/);
  resolve(7);
  await new Promise<void>(done => setImmediate(done));
  assert.equal(cache.snapshot().inflight, 0);
  assert.equal(cache.get('old', 'same'), undefined);
  assert.equal(await cache.getOrLoad('new', 'other', async () => 2), 2);
});

test('cache disposal aborts all requests and prevents late publication', async () => {
  const cache = new ScopedCache<number>({ name: 'dispose', ttlMs: 100, maxEntries: 4, maxBytes: 100 });
  let resolve!: (value: number) => void;
  const pending = cache.getOrLoad('scope', 'key', () => new Promise<number>(done => { resolve = done; }));
  cache.clear();
  await assert.rejects(pending, { name: 'AbortError' });
  resolve(1);
  await new Promise<void>(done => setImmediate(done));
  assert.equal(cache.get('scope', 'key'), undefined);
  assert.equal(cache.snapshot().inflight, 0);
});

test('a session deadline rejects waiters without freeing an unfinished physical request', async () => {
  const cache = new ScopedCache<number>({ name: 'deadline', ttlMs: 100, maxEntries: 4, maxBytes: 100, loadTimeoutMs: 5 });
  let resolve!: (value: number) => void;
  const pending = cache.getOrLoad('scope', 'key', () => new Promise<number>(done => { resolve = done; }));
  const rejection = assert.rejects(pending, { name: 'TimeoutError' });
  await new Promise(done => setTimeout(done, 20));
  await rejection;
  assert.equal(cache.snapshot().inflight, 1);
  resolve(1);
  await new Promise<void>(done => setImmediate(done));
  assert.equal(cache.get('scope', 'key'), undefined);
  assert.equal(cache.snapshot().inflight, 0);
});
