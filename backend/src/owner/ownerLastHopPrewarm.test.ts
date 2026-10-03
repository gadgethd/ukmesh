import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createOwnerLastHopPrewarm,
  ownerLastHopPrewarmConcurrency,
} from './ownerLastHopPrewarm.js';

function capturedLog() {
  const entries: Array<{ level: string; message: string; detail: unknown }> = [];
  return {
    entries,
    log: {
      log(message: string, detail: unknown) { entries.push({ level: 'log', message, detail }); },
      warn(message: string, detail: unknown) { entries.push({ level: 'warn', message, detail }); },
    },
  };
}

test('prewarm concurrency defaults to two and cannot exceed the DB load cap', () => {
  for (const value of [undefined, null, '', 'invalid', NaN, Infinity]) {
    assert.equal(ownerLastHopPrewarmConcurrency(value), 2);
  }
  for (const value of [0, -1, 1, '1', 1.9]) assert.equal(ownerLastHopPrewarmConcurrency(value), 1);
  for (const value of [2, '2', 12]) assert.equal(ownerLastHopPrewarmConcurrency(value), 2);
});

test('a pass counts completed owners and nodes, logs every ten, and continues after failures', async () => {
  const captured = capturedLog();
  let active = 0;
  let peak = 0;
  const refreshed: string[] = [];
  const prewarm = createOwnerLastHopPrewarm({
    concurrency: 99,
    log: captured.log,
    loadOwners: async () => [
      { mqttUsername: 'a', nodeIds: Array.from({ length: 8 }, (_, i) => String(i)) },
      { mqttUsername: 'b', nodeIds: Array.from({ length: 8 }, (_, i) => String(i + 8)) },
      { mqttUsername: 'c', nodeIds: Array.from({ length: 7 }, (_, i) => String(i + 16)) },
      { mqttUsername: 'empty', nodeIds: [] },
    ],
    refresh: async (_owner, nodeId) => {
      active++;
      peak = Math.max(peak, active);
      await new Promise<void>((resolve) => setImmediate(resolve));
      active--;
      refreshed.push(nodeId);
      if (nodeId === '4' || nodeId === '18') throw new Error('test refresh failed');
    },
  });
  assert.deepEqual(await prewarm.run(), {
    owners: 3, ownersTotal: 3, nodes: 23, nodesTotal: 23, refreshed: 21, failed: 2,
  });
  assert.equal(peak, 2);
  assert.equal(new Set(refreshed).size, 23);
  assert.deepEqual(captured.entries.filter((entry) => entry.message.endsWith('progress')).map((entry) => entry.detail), [
    { owners: 1, ownersTotal: 3, nodes: 10, nodesTotal: 23, refreshed: 9, failed: 1 },
    { owners: 2, ownersTotal: 3, nodes: 20, nodesTotal: 23, refreshed: 18, failed: 2 },
  ]);
});

test('overlapping callers share one pass and duplicate nodes are refreshed once per owner', async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let loads = 0;
  let calls = 0;
  const prewarm = createOwnerLastHopPrewarm({
    log: capturedLog().log,
    loadOwners: async () => { loads++; return [{ mqttUsername: 'a', nodeIds: ['one', 'one'] }]; },
    refresh: async () => { calls++; await gate; },
  });
  const first = prewarm.run();
  assert.equal(prewarm.run(), first);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(loads, 1);
  assert.equal(calls, 1);
  release();
  assert.equal((await first).refreshed, 1);
});

test('slow refresh warns while the refresh is still running, including one that fails', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const captured = capturedLog();
  let clock = 0;
  let rejectRefresh!: (error: Error) => void;
  const gate = new Promise<void>((_resolve, reject) => { rejectRefresh = reject; });
  const prewarm = createOwnerLastHopPrewarm({
    now: () => clock,
    log: captured.log,
    loadOwners: async () => [{ mqttUsername: 'a', nodeIds: ['slow'] }],
    refresh: () => gate,
  });
  const result = prewarm.run();
  await Promise.resolve();
  clock = 20_001;
  t.mock.timers.tick(20_001);
  assert.equal(captured.entries.filter((entry) => entry.message.includes('exceeds 20s')).length, 1);
  rejectRefresh(new Error('late failure'));
  assert.equal((await result).failed, 1);
  assert.equal(captured.entries.filter((entry) => entry.message.includes('exceeds 20s')).length, 1);
});

test('stopping drains current refreshes and prevents further admissions', async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let calls = 0;
  const prewarm = createOwnerLastHopPrewarm({
    concurrency: 1,
    log: capturedLog().log,
    loadOwners: async () => [{ mqttUsername: 'a', nodeIds: ['one', 'two'] }],
    refresh: async () => { calls++; await gate; },
  });
  const result = prewarm.run();
  await Promise.resolve();
  const stopping = prewarm.stop();
  release();
  await stopping;
  assert.equal(calls, 1);
  assert.deepEqual(await result, {
    owners: 0, ownersTotal: 1, nodes: 1, nodesTotal: 2, refreshed: 1, failed: 0,
  });
  await assert.rejects(prewarm.run(), /PREWARM_STOPPED/);
});

test('scheduled passes wait for completion before starting their interval and start is idempotent', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let loads = 0;
  let refreshes = 0;
  const prewarm = createOwnerLastHopPrewarm({
    log: capturedLog().log,
    loadOwners: async () => { loads++; return [{ mqttUsername: 'a', nodeIds: ['one'] }]; },
    refresh: async () => { refreshes++; await gate; },
  });
  t.after(() => prewarm.stop());
  prewarm.start(100);
  prewarm.start(100);
  await new Promise<void>((resolve) => setImmediate(resolve));
  t.mock.timers.tick(1_000);
  assert.equal(loads, 1, 'a long pass cannot overlap or accumulate interval ticks');
  assert.equal(refreshes, 1);
  release();
  await new Promise<void>((resolve) => setImmediate(resolve));
  t.mock.timers.tick(99);
  assert.equal(loads, 1, 'the interval begins when the pass completes');
  t.mock.timers.tick(1);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(loads, 2);
  assert.equal(refreshes, 2);
  await prewarm.stop();
  t.mock.timers.tick(1_000);
  assert.equal(loads, 2, 'shutdown cancels the next scheduled pass');
});

test('a failed owner enumeration logs the failure and the next scheduled pass recovers', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const captured = capturedLog();
  let loads = 0;
  let refreshes = 0;
  const prewarm = createOwnerLastHopPrewarm({
    log: captured.log,
    loadOwners: async () => {
      if (++loads === 1) throw new Error('owner inventory unavailable');
      return [{ mqttUsername: 'a', nodeIds: ['one'] }];
    },
    refresh: async () => { refreshes++; },
  });
  t.after(() => prewarm.stop());
  prewarm.start(100);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(refreshes, 0);
  assert.deepEqual(captured.entries, [{
    level: 'warn', message: '[owner-last-hop-prewarm] pass failed', detail: 'owner inventory unavailable',
  }]);
  t.mock.timers.tick(100);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(loads, 2);
  assert.equal(refreshes, 1);
  assert.deepEqual(captured.entries.at(-1)?.detail, {
    owners: 1, ownersTotal: 1, nodes: 1, nodesTotal: 1, refreshed: 1, failed: 0,
  });
});

test('shutdown during owner enumeration drains the pass without admitting a refresh', async () => {
  let release!: (owners: Array<{ mqttUsername: string; nodeIds: string[] }>) => void;
  const inventory = new Promise<Array<{ mqttUsername: string; nodeIds: string[] }>>((resolve) => { release = resolve; });
  let refreshes = 0;
  const prewarm = createOwnerLastHopPrewarm({
    log: capturedLog().log,
    loadOwners: () => inventory,
    refresh: async () => { refreshes++; },
  });
  const pass = prewarm.run();
  const stopping = prewarm.stop();
  release([{ mqttUsername: 'a', nodeIds: ['one', 'two'] }]);
  await stopping;
  assert.equal(refreshes, 0);
  assert.deepEqual(await pass, {
    owners: 0, ownersTotal: 1, nodes: 0, nodesTotal: 2, refreshed: 0, failed: 0,
  });
  await assert.rejects(prewarm.run(), /PREWARM_STOPPED/);
});
