/** Component experiment only: compare bounded minute sweeps with metrics-enabled TTL storage. */
import { performance } from 'node:perf_hooks';
import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

const capacity = 50_000;
const ttlMs = 120_000;
const measuredAt = 240_000;
const runs = 20;
const kinds = ['manual', 'bounded'] as const;
const states = ['fresh', 'half-expired', 'expired'] as const;
const rows: Array<{
  kind: typeof kinds[number]; state: typeof states[number];
  wallMs: number; cpuMs: number; remaining: number;
}> = [];

for (const kind of kinds) for (const state of states) {
  for (let run = 0; run <= runs; run++) {
    let clock = 0;
    const map = kind === 'manual' ? new Map<string, number>() : new BoundedTtlMap<string, number>({
      maxEntries: capacity, maxWeight: capacity * 256, ttlMs,
      weightOf: () => 256, now: () => clock,
    });
    for (let i = 0; i < capacity; i++) {
      // Both cohorts coexist before expiry: advancing the setup clock by a
      // full TTL would let BoundedTtlMap prune entries during insertion.
      const fresh = state === 'fresh' || (state === 'half-expired' && i >= capacity / 2);
      clock = fresh ? measuredAt - 60_000 : state === 'half-expired' ? 90_000 : 0;
      map.set(`invented-observation-${i.toString().padStart(8, '0')}-${'A'.repeat(128)}`, clock);
    }
    assert.equal(map.size, capacity, 'equal populated entries before sweep');
    const cpu = process.cpuUsage();
    const started = performance.now();
    if (map instanceof BoundedTtlMap) map.sweep(measuredAt);
    else for (const [key, timestamp] of map) if (timestamp < measuredAt - ttlMs) map.delete(key);
    const wallMs = performance.now() - started;
    const used = process.cpuUsage(cpu);
    const remaining = map.size;
    assert.equal(remaining, state === 'fresh' ? capacity : state === 'half-expired' ? capacity / 2 : 0);
    if (run > 0) rows.push({ kind, state, wallMs, cpuMs: (used.user + used.system) / 1_000, remaining });
    if (map instanceof BoundedTtlMap) map.shutdown();
    map.clear();
  }
}

const median = (values: number[]) => (values[runs / 2 - 1]! + values[runs / 2]!) / 2;
const summary = [];
for (const kind of kinds) for (const state of states) {
  const group = rows.filter(row => row.kind === kind && row.state === state);
  const wall = group.map(row => row.wallMs).sort((a, b) => a - b);
  const cpu = group.map(row => row.cpuMs).sort((a, b) => a - b);
  summary.push({ kind, state, medianMs: median(wall), p95Ms: wall[Math.ceil(.95 * runs) - 1],
    medianCpuMs: median(cpu), amortizedCpuMsPerSecond: median(cpu) / 60 });
}

const outputIndex = process.argv.indexOf('--output');
if (outputIndex >= 0) {
  const value = process.argv[outputIndex + 1];
  if (!value) throw new Error('--output needs a file path');
  const output = path.resolve(value);
  mkdirSync(path.dirname(output), { recursive: true });
  writeFileSync(output, JSON.stringify({ node: process.version, capacity, runs, warmup: 1,
    percentile: 'nearest rank', summary, rows }, null, 2) + '\n');
}
console.log(JSON.stringify(summary));
