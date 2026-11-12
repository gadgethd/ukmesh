import assert from 'node:assert/strict';
import test from 'node:test';
import type { Redis } from 'ioredis';
import { createHealthRuntime } from './runtime.js';
import { metricsRegistry } from '../metrics/registry.js';

test('health runtime starts probes explicitly and detaches only its own collectors', async t => {
  const fixture = () => {
    let reads = 0, connections = 0, disconnects = 0;
    const runtime = createHealthRuntime({
      execute: async () => {
        reads++;
        return { rows: [{ desiredGeneration: '7', materializedGeneration: '7', appliedGeneration: '7', queueActive: false }] } as never;
      },
      createRedis: () => {
        connections++;
        return { on() {}, get: async () => String(Date.now() / 1_000),
          disconnect() { disconnects++; } } as unknown as Redis;
      },
    });
    return { runtime, counts: () => ({ reads, connections, disconnects }) };
  };
  t.mock.method(console, 'log', () => undefined);
  const first = fixture(), second = fixture();
  t.after(first.runtime.close); t.after(second.runtime.close);
  assert.deepEqual(first.counts(), { reads: 0, connections: 0, disconnects: 0 });
  first.runtime.startTelemetry(); first.runtime.startTelemetry();
  assert.equal(first.counts().reads, 0);
  await metricsRegistry.metrics();
  assert.deepEqual(first.counts(), { reads: 1, connections: 1, disconnects: 0 });
  second.runtime.startTelemetry();
  first.runtime.close();
  assert.match(await metricsRegistry.metrics(), /\nmeshcore_privacy_generation_healthy 1\n/);
  assert.equal(first.counts().reads, 1);
  assert.equal(first.counts().disconnects, 1);
  assert.equal(second.counts().reads, 1);
  second.runtime.close(); second.runtime.startTelemetry();
  assert.match(await metricsRegistry.metrics(), /\nmeshcore_privacy_generation_healthy 0\n/);
  assert.equal(second.counts().reads, 1);
  assert.equal(second.counts().disconnects, 1);
});
