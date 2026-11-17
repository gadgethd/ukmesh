import assert from 'node:assert/strict';
import test from 'node:test';
import type { Redis } from 'ioredis';
import { createHealthRuntime } from './runtime.js';
import { metricsRegistry } from '../metrics/registry.js';
import type { OperationalCheckRow } from './types.js';

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

test('operational checks observe detached failures and preserve blocking errors and retry', { timeout: 2_000 }, async t => {
  let now = Date.now();
  t.mock.method(Date, 'now', () => now);
  t.mock.method(console, 'log', () => undefined);
  const errors: unknown[][] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { errors.push(args); });
  const unhandled: unknown[] = [];
  const onUnhandled = (error: unknown) => { unhandled.push(error); };
  process.on('unhandledRejection', onUnhandled);
  t.after(() => process.off('unhandledRejection', onUnhandled));
  const original: OperationalCheckRow[] = [{
    check_name: 'http', status: 'ok', latency_ms: 1, detail: null, ts: new Date(now).toISOString(),
  }];
  let loads = 0;
  let load = async (): Promise<{ rows: OperationalCheckRow[] }> => ({ rows: original });
  const runtime = createHealthRuntime({
    execute: async text => {
      if (text.includes('FROM ops.operational_check_results')) {
        loads++;
        return await load() as never;
      }
      return { rows: [] } as never;
    },
    createRedis: () => ({
      on() {}, disconnect() {}, get: async () => null,
      llen: async () => 0, hmget: async () => [], zcard: async () => 0,
      zrange: async () => [], hvals: async () => [],
      config: async (_command: string, setting: string) =>
        [setting, setting === 'appendonly' ? 'yes' : 'noeviction'],
    }) as unknown as Redis,
  });
  t.after(runtime.close);

  await t.test('fresh rows are cached and stale rows return before a failed reload settles', async () => {
    assert.deepEqual((await runtime.getWorkerHealthOverview()).operational_checks, original);
    assert.deepEqual((await runtime.getWorkerHealthOverview()).operational_checks, original);
    assert.equal(loads, 1);
    now += 30_000;
    let reject!: (reason: unknown) => void;
    const pending = new Promise<{ rows: OperationalCheckRow[] }>((_resolve, no) => { reject = no; });
    load = () => pending;
    assert.deepEqual((await runtime.getWorkerHealthOverview()).operational_checks, original);
    assert.equal(loads, 2);
    assert.deepEqual(errors, []);
    reject(new Error('owned background checkout failure'));
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.deepEqual(unhandled, []);
    assert.deepEqual(errors, [['[health] operational checks refresh failed:', 'owned background checkout failure']]);
  });

  await t.test('expired callers still receive the original refresh failure', async () => {
    now += 5 * 60_000;
    const failure = new Error('owned blocking checkout failure');
    load = async () => { throw failure; };
    await assert.rejects(runtime.getWorkerHealthOverview(), error => error === failure);
    assert.equal(loads, 3);
    assert.deepEqual(errors[1], ['[health] operational checks refresh failed:', failure.message]);
  });

  await t.test('a failed refresh releases singleflight so the next call can retry', async () => {
    const replacement = [{ ...original[0]!, latency_ms: 2, ts: new Date(now).toISOString() }];
    load = async () => ({ rows: replacement });
    assert.deepEqual((await runtime.getWorkerHealthOverview()).operational_checks, replacement);
    assert.deepEqual((await runtime.getWorkerHealthOverview()).operational_checks, replacement);
    assert.equal(loads, 4);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.deepEqual(unhandled, []);
    assert.equal(errors.length, 2);
  });
});
