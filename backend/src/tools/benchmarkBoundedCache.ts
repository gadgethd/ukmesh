/** Run with tsx. Benchmark in an isolated checkout; no services are contacted. */
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

function measure(entries: number) {
  const cache = new BoundedTtlMap<number, number>({
    maxEntries: entries, maxWeight: 16 * 1024 * 1024,
    ttlMs: 3_600_000, weightOf: () => 1, now: () => 0,
  });
  try {
    const start = performance.now();
    for (let key = 0; key < entries; key += 1) cache.set(key, key);
    const fillMs = performance.now() - start;
    const updateStart = performance.now();
    for (let key = 0; key < 2_000; key += 1) cache.set(key, key + 1);
    return {
      entries,
      fillMs: Number(fillMs.toFixed(2)),
      update2000Ms: Number((performance.now() - updateStart).toFixed(2)),
      retainedEntries: cache.size,
    };
  } finally {
    cache.shutdown();
  }
}

measure(2_000);
for (const entries of [4_096, 20_000, 50_000]) {
  console.log(JSON.stringify(measure(entries)));
}
