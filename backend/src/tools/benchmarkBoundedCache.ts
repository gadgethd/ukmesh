import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

// Component benchmark: fixed clock excludes expiry work; metrics stay enabled.
const capacity = Number(process.env['BENCH_CACHE_CAPACITY'] ?? 10_000);
const writes = Number(process.env['BENCH_CACHE_WRITES'] ?? 10_000);
const repeats = Number(process.env['BENCH_CACHE_REPEATS'] ?? 5);
assert.ok([capacity, writes, repeats].every((n) => Number.isSafeInteger(n) && n > 0));
const results: Array<{ elapsedMs: number; cpuMs: number; writesPerSecond: number }> = [];
for (let repetition = 0; repetition < repeats; repetition += 1) {
  const cache = new BoundedTtlMap<number, number>({
    maxEntries: capacity, maxWeight: capacity * 8, ttlMs: 60_000,
    weightOf: () => 8, now: () => 0,
  });
  try {
    for (let i = 0; i < capacity; i += 1) cache.set(i, i);
    const cpu = process.cpuUsage();
    const started = performance.now();
    for (let i = 0; i < writes; i += 1) cache.set(i % capacity, i);
    const elapsedMs = performance.now() - started;
    const used = process.cpuUsage(cpu);
    assert.equal(cache.size, capacity);
    assert.equal(cache.weight(), capacity * 8);
    cache.sweep(60_000);
    assert.equal(cache.size, 0, 'physical expiry remains exact');
    results.push({ elapsedMs, cpuMs: (used.user + used.system) / 1_000, writesPerSecond: writes * 1_000 / elapsedMs });
  } finally { cache.shutdown(); }
}
const median = (key: keyof typeof results[number]) => results.map((run) => run[key]).sort((a, b) => a - b)[Math.floor(results.length / 2)];
console.log(JSON.stringify({ node: process.version, capacity, writes, repeats, results,
  medianElapsedMs: median('elapsedMs'), medianCpuMs: median('cpuMs'), medianWritesPerSecond: median('writesPerSecond'),
  peakRssMiB: process.resourceUsage().maxRSS / 1024 }, null, 2));
