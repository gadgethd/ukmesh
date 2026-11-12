import assert from 'node:assert/strict';
import test from 'node:test';
import { BoundedTtlMap } from './boundedTtlMap.js';

test('bounded cache evicts LRU entries and physically sweeps cold expiry', () => {
  let now = 0;
  const cache = new BoundedTtlMap<string, string>({
    maxEntries: 2,
    maxWeight: 10,
    ttlMs: 100,
    weightOf: (_key, value) => value.length,
    now: () => now,
  });
  cache.set('a', 'aa').set('b', 'bb');
  assert.equal(cache.get('a'), 'aa');
  cache.set('c', 'cc');
  assert.equal(cache.has('b'), false);
  now = 101;
  cache.sweep();
  assert.equal(cache.size, 0);
  assert.equal(cache.weight(), 0);
  cache.shutdown();
});

test('bounded cache rejects a single overweight value', () => {
  const cache = new BoundedTtlMap<string, string>({
    maxEntries: 2,
    maxWeight: 2,
    ttlMs: 100,
    weightOf: (_key, value) => value.length,
  });
  cache.set('huge', 'oversized');
  assert.equal(cache.size, 0);
  assert.equal(cache.metrics().rejections, 1);
  cache.shutdown();
});

test('bounded cache overwrite accounts weight once and reports hit/miss/eviction metrics', () => {
  const cache = new BoundedTtlMap<string, string>({
    maxEntries: 2,
    maxWeight: 5,
    ttlMs: 100,
    weightOf: (_key, value) => value.length,
  });
  cache.set('a', 'a');
  cache.set('a', 'aaa');
  assert.equal(cache.weight(), 3);
  assert.equal(cache.get('a'), 'aaa');
  assert.equal(cache.get('missing'), undefined);
  cache.set('b', 'bb');
  cache.set('c', 'cc');
  assert.deepEqual(cache.metrics(), {
    hits: 1,
    misses: 1,
    expiries: 0,
    evictions: 1,
    rejections: 0,
    size: 2,
    weight: 4,
  });
  cache.shutdown();
});

test('sweeps respect replacement deadlines, access order, and backwards clock changes', () => {
  let now = 100;
  const cache = new BoundedTtlMap<string, string>({
    maxEntries: 4, maxWeight: 100, ttlMs: 100, now: () => now,
  });
  try {
    cache.set('replaced', 'old');
    now = 150;
    cache.set('replaced', 'new');
    now = 50;
    cache.set('earlier', 'value');
    cache.get('replaced'); // LRU order must not determine expiry order.
    now = 150;
    cache.sweep();
    assert.equal(cache.has('earlier'), false);
    assert.equal(cache.get('replaced'), 'new');
    now = 200; // The overwritten value's original expiry is harmless.
    cache.sweep();
    assert.equal(cache.size, 1);
    now = 250;
    cache.sweep();
    assert.equal(cache.size, 0);
    assert.equal(cache.weight(), 0);
    assert.equal(cache.metrics().expiries, 2);
    cache.clear();
    now = 0;
    cache.set('after-clear', 'value');
    now = 100;
    cache.sweep();
    assert.equal(cache.size, 0);
  } finally {
    cache.shutdown();
  }
});

test('expiry bound follows overwrites, deletion, clear, and backwards clock changes', () => {
  let now = 100;
  const cache = new BoundedTtlMap<string, string>({
    maxEntries: 5, maxWeight: 10, ttlMs: 100, weightOf: () => 1, now: () => now,
  });
  try {
    cache.set('overwritten', 'old').set('deleted', 'old');
    now = 150;
    cache.set('overwritten', 'new');
    cache.delete('deleted');
    cache.sweep(200);
    assert.equal(cache.size, 1, 'replaced timestamp determines expiry');
    now = 50;
    cache.set('backdated', 'new');
    cache.sweep(150);
    assert.equal(cache.has('backdated'), false, 'earlier timestamps lower the bound');
    assert.equal(cache.get('overwritten'), 'new');
    cache.sweep(250);
    assert.equal(cache.size, 0);
    assert.equal(cache.weight(), 0);
    cache.set('cleared', 'old');
    cache.clear();
    now = 0;
    cache.set('after-clear', 'new');
    cache.sweep(100);
    assert.equal(cache.size, 0);
  } finally { cache.shutdown(); }
});

test('LRU reads do not postpone TTL and expired entries leave room before eviction', () => {
  let now = 0;
  const cache = new BoundedTtlMap<string, number>({
    maxEntries: 2, maxWeight: 2, ttlMs: 100, weightOf: () => 1, now: () => now,
  });
  try {
    cache.set('a', 1);
    now = 50;
    cache.set('b', 2);
    assert.equal(cache.get('a'), 1);
    now = 100;
    cache.set('c', 3);
    assert.deepEqual([...cache.keys()], ['b', 'c']);
    assert.equal(cache.metrics().expiries, 1);
    assert.equal(cache.metrics().evictions, 0);
  } finally { cache.shutdown(); }
});

test('invalid and unmeasurable weights cannot disable byte bounds', () => {
  for (const weight of [NaN, Infinity, -Infinity, -1]) {
    const cache = new BoundedTtlMap<string, string>({ maxEntries: 2, maxWeight: 100, ttlMs: 1000, weightOf: () => weight });
    try { cache.set('invalid', 'value'); assert.equal(cache.size, 0); assert.equal(cache.weight(), 0); assert.equal(cache.metrics().rejections, 1); }
    finally { cache.shutdown(); }
  }
  const cache = new BoundedTtlMap<string, unknown>({ maxEntries: 2, maxWeight: 100, ttlMs: 1000 });
  const cyclic = { self: null as unknown }; cyclic.self = cyclic;
  try {
    cache.set('cyclic', cyclic).set('undefined', undefined).set('bigint', 1n);
    assert.equal(cache.size, 0); assert.equal(cache.weight(), 0); assert.equal(cache.metrics().rejections, 3);
    cache.set('valid', 'value'); assert.equal(cache.get('valid'), 'value');
  } finally { cache.shutdown(); }
});


test('a rejected custom estimate leaves existing accounting usable', () => {
  const cache = new BoundedTtlMap<string, string>({
    maxEntries: 10, maxWeight: 100, ttlMs: 1000,
    weightOf: key => { if (key === 'throw') throw new Error('injected estimator failure'); return key === 'nan' ? NaN : 50; },
  });
  try {
    cache.set('first', 'one').set('throw', 'bad').set('nan', 'bad').set('second', 'two').set('third', 'three');
    assert.equal(cache.weight(), 100);
    assert.deepEqual([...cache.keys()], ['second', 'third']);
    assert.equal(cache.metrics().rejections, 2);
  } finally { cache.shutdown(); }
});
