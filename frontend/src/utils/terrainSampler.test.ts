import assert from 'node:assert/strict';
import test from 'node:test';
import { TerrainTileCache, type TerrainTile } from './terrainSampler.js';

function fakeTile(id: number, closeLog: number[]): TerrainTile {
  return {
    data: { width: 1, height: 1, data: new Uint8ClampedArray(4) } as ImageData,
    bitmap: {
      width: 1,
      height: 1,
      close: () => closeLog.push(id),
    } as ImageBitmap,
  };
}

test('terrain cache single-flights concurrent tile loads and is true LRU', async () => {
  const closes: number[] = [];
  let loads = 0;
  let resolveFirst!: (tile: TerrainTile) => void;
  const first = new Promise<TerrainTile>((resolve) => {
    resolveFirst = resolve;
  });
  const cache = new TerrainTileCache(async (key) => {
    loads += 1;
    if (key === 'a') return first;
    return fakeTile(key === 'b' ? 2 : 3, closes);
  }, { maxEntries: 2, maxBytes: 16, maxInflight: 2 });

  const a1 = cache.get('a');
  const a2 = cache.get('a');
  resolveFirst(fakeTile(1, closes));
  assert.equal(await a1, await a2);
  assert.equal(loads, 1);

  await cache.get('b');
  assert.ok(cache.peek('a')); // a becomes most recently used
  await cache.get('c');
  assert.deepEqual(closes, [2]);
  assert.ok(cache.peek('a'));
  assert.ok(cache.peek('c'));
});

test('terrain cache keeps a shared load alive until every consumer aborts', async () => {
  const closes: number[] = [];
  let loaderSignal: AbortSignal | undefined;
  const cache = new TerrainTileCache((_key, signal) => {
    loaderSignal = signal;
    return new Promise((_resolve, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    });
  }, { maxEntries: 1, maxBytes: 8, maxInflight: 1 });
  const firstController = new AbortController();
  const secondController = new AbortController();
  const first = cache.get('same', firstController.signal);
  const second = cache.get('same', secondController.signal);

  firstController.abort(new DOMException('first cancelled', 'AbortError'));
  await assert.rejects(first, /first cancelled/);
  assert.equal(loaderSignal?.aborted, false);
  secondController.abort(new DOMException('second cancelled', 'AbortError'));
  await assert.rejects(second, /second cancelled/);
  assert.equal(loaderSignal?.aborted, true);
  assert.deepEqual(closes, []);
});

test('terrain cache closes every retained bitmap on clear', async () => {
  const closes: number[] = [];
  const cache = new TerrainTileCache(
    async (key) => fakeTile(Number(key), closes),
    { maxEntries: 4, maxBytes: 32, maxInflight: 2 },
  );
  await cache.get('1');
  await cache.get('2');
  cache.clear();
  assert.deepEqual(closes.sort(), [1, 2]);
  assert.equal(cache.snapshot().entries, 0);
});
