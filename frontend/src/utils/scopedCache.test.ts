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
