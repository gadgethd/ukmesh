import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { createOwnerLastHopPrewarm, type OwnerLastHopPrewarmDeps } from './ownerLastHopPrewarm.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function setup(t: TestContext, overrides: Partial<OwnerLastHopPrewarmDeps> = {}) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout', 'setInterval'], now: 0 });
  const logs: Array<{ message: string; data: Record<string, unknown> }> = [];
  const warnings: typeof logs = [];
  t.mock.method(console, 'log', (message: string, data: Record<string, unknown>) => { logs.push({ message, data }); });
  t.mock.method(console, 'warn', (message: string, data: Record<string, unknown>) => { warnings.push({ message, data }); });
  const stop = createOwnerLastHopPrewarm({
    listOwnerNodes: async () => [{ mqttUsername: 'owner', isActive: true, nodeIds: ['a', 'b', 'c'] }],
    refreshLastHop: async () => true,
    intervalMs: 120_000,
    nodeDelayMs: 0,
    ...overrides,
  });
  t.after(stop);
  const advance = async (ms: number) => { t.mock.timers.tick(ms); await setImmediate(); };
  const entry = (suffix: string) => {
    const found = logs.findLast((log) => log.message === `[owner-last-hop-prewarm] ${suffix}`);
    assert.ok(found, `missing ${suffix} log`);
    return found.data;
  };
  return { stop, advance, logs, warnings, entry };
}

test('default serial order, both windows, owner scopes, per-node delay and outcome accounting are preserved', async (t) => {
  const calls: unknown[][] = [];
  const aNodes = ['a', 'b'];
  const bNodes = ['a'];
  const h = setup(t, {
    nodeDelayMs: 150,
    listOwnerNodes: async () => [
      { mqttUsername: 'one', isActive: true, nodeIds: aNodes },
      { mqttUsername: 'inactive', isActive: false, nodeIds: ['ignored'] },
      { mqttUsername: 'empty', isActive: true, nodeIds: [] },
      { mqttUsername: 'two', isActive: true, nodeIds: bNodes },
    ],
    refreshLastHop: async (owned, node, window, minAge) => {
      calls.push([owned, node, window, minAge]);
      if (node === 'b' && window === 7) throw new Error('fixture failure');
      return window === 7 || node === 'b';
    },
  });
  await h.advance(19_999);
  assert.equal(calls.length, 0);
  await h.advance(1);
  assert.deepEqual(calls, [[aNodes, 'a', 7, 60_000], [aNodes, 'a', 30, 60_000]]);
  assert.equal(h.entry('enabled').concurrency, 1);
  assert.equal(h.entry('pass started').totalRefreshes, 6);
  await h.advance(149);
  assert.equal(calls.length, 2);
  await h.advance(1);
  assert.deepEqual(calls.slice(2), [[aNodes, 'b', 7, 60_000], [aNodes, 'b', 30, 60_000]]);
  await h.advance(150);
  assert.deepEqual(calls.slice(4), [[bNodes, 'a', 7, 60_000], [bNodes, 'a', 30, 60_000]]);
  assert.equal(h.logs.some((log) => log.message.endsWith('pass complete')), false);
  await h.advance(150);
  assert.deepEqual(h.entry('pass complete'), {
    owners: 2, ownersCompleted: 2, nodes: 3, nodesCompleted: 3, totalNodes: 3,
    totalRefreshes: 6, completedRefreshes: 6, remainingRefreshes: 0,
    refreshed: 3, skipped: 2, failed: 1, durationMs: 450,
    ratePerSecond: 6_000 / 450, etaSeconds: 0, concurrency: 1,
  });
  assert.equal(h.warnings.length, 1);
  assert.equal(h.warnings[0]!.data.error, 'fixture failure');
});

test('timed progress reports completed work, rate and ETA while a refresh is still in flight', async (t) => {
  const slow = deferred<boolean>();
  const h = setup(t, {
    windows: [30],
    listOwnerNodes: async () => [{ mqttUsername: 'owner', isActive: true, nodeIds: ['a', 'b'] }],
    refreshLastHop: async (_owned, node) => node === 'a' ? true : slow.promise,
  });
  await h.advance(20_000);
  assert.equal(h.logs.filter((log) => log.message.endsWith(' progress')).length, 0);
  await h.advance(29_999);
  assert.equal(h.logs.filter((log) => log.message.endsWith(' progress')).length, 0);
  await h.advance(1);
  assert.deepEqual(h.entry('progress'), {
    owners: 1, ownersCompleted: 0, nodes: 2, nodesCompleted: 1, totalNodes: 2,
    totalRefreshes: 2, completedRefreshes: 1, remainingRefreshes: 1,
    refreshed: 1, skipped: 0, failed: 0, durationMs: 30_000,
    ratePerSecond: 1 / 30, etaSeconds: 30, concurrency: 1,
  });
  slow.resolve(false);
  await setImmediate();
  assert.equal(h.entry('pass complete').skipped, 1);
  await h.advance(30_000);
  assert.equal(h.logs.filter((log) => log.message.endsWith(' progress')).length, 1);
});

test('stalled work logs zero throughput and unknown ETA; stop admits no new nodes and cleans up progress', async (t) => {
  const slow = deferred<boolean>();
  const calls: number[] = [];
  const h = setup(t, {
    refreshLastHop: async (_owned, _node, window) => {
      calls.push(window);
      return window === 7 ? slow.promise : true;
    },
  });
  await h.advance(20_000);
  await h.advance(30_000);
  assert.equal(h.entry('progress').ratePerSecond, 0);
  assert.equal(h.entry('progress').etaSeconds, null);
  h.stop();
  slow.resolve(true);
  await setImmediate();
  assert.deepEqual(calls, [7, 30]);
  const stopped = h.entry('pass stopped');
  assert.equal(stopped.completedRefreshes, 2);
  assert.equal(stopped.remainingRefreshes, 4);
  assert.equal(stopped.ownersCompleted, 0);
  await h.advance(300_000);
  assert.equal(calls.length, 2);
  assert.equal(h.logs.filter((log) => log.message.endsWith(' progress')).length, 1);
});

test('parallel workers cap active refreshes at two, keep node windows serial and account for out-of-order owners', async (t) => {
  const gates = new Map<string, ReturnType<typeof deferred<boolean>>>();
  const calls: string[] = [];
  let active = 0;
  let peak = 0;
  const h = setup(t, {
    concurrency: 1_000,
    listOwnerNodes: async () => [
      { mqttUsername: 'one', isActive: true, nodeIds: ['a', 'b'] },
      { mqttUsername: 'two', isActive: true, nodeIds: ['c'] },
    ],
    refreshLastHop: async (_owned, node, window) => {
      const key = `${node}/${window}`;
      calls.push(key);
      const gate = deferred<boolean>();
      gates.set(key, gate);
      active += 1;
      peak = Math.max(peak, active);
      try { return await gate.promise; } finally { active -= 1; }
    },
  });
  await h.advance(20_000);
  assert.deepEqual(calls, ['a/7', 'b/7']);
  assert.equal(h.entry('enabled').concurrency, 2);
  gates.get('b/7')!.resolve(true);
  await setImmediate();
  assert.deepEqual(calls, ['a/7', 'b/7', 'b/30']);
  gates.get('b/30')!.reject(new Error('fixture'));
  await setImmediate();
  gates.get('c/7')!.resolve(false);
  await setImmediate();
  gates.get('c/30')!.resolve(true);
  await setImmediate();
  await h.advance(30_000);
  assert.equal(h.entry('progress').ownersCompleted, 1);
  assert.equal(h.entry('progress').nodesCompleted, 2);
  assert.equal(h.entry('progress').completedRefreshes, 4);
  assert.equal(h.entry('progress').failed, 1);
  gates.get('a/7')!.resolve(true);
  await setImmediate();
  gates.get('a/30')!.resolve(true);
  await setImmediate();
  assert.equal(peak, 2);
  assert.equal(h.entry('pass complete').ownersCompleted, 2);
  assert.equal(h.entry('pass complete').completedRefreshes, 6);
  assert.equal(h.entry('pass complete').refreshed, 4);
  assert.equal(h.entry('pass complete').skipped, 1);
});

for (const concurrency of [0, -1, NaN, Infinity, 1.9]) {
  test(`invalid or fractional concurrency ${concurrency} falls back to serial work`, async (t) => {
    const slow = deferred<boolean>();
    let calls = 0;
    const h = setup(t, { concurrency, refreshLastHop: async () => { calls += 1; return slow.promise; } });
    await h.advance(20_000);
    assert.equal(calls, 1);
    assert.equal(h.entry('enabled').concurrency, 1);
    h.stop();
    slow.resolve(true);
    await setImmediate();
  });
}

test('long passes cannot overlap; the existing fixed-interval schedule resumes after completion', async (t) => {
  const slow = deferred<boolean>();
  let enumerations = 0;
  const h = setup(t, {
    intervalMs: 60_000,
    listOwnerNodes: async () => {
      enumerations += 1;
      return [{ mqttUsername: 'owner', isActive: true, nodeIds: ['a'] }];
    },
    refreshLastHop: async () => slow.promise,
  });
  await h.advance(20_000);
  await h.advance(60_000);
  assert.equal(enumerations, 1);
  slow.resolve(true);
  await setImmediate();
  await h.advance(40_000);
  assert.equal(enumerations, 2);
});

test('enumeration failure recovers on the next pass and empty passes have finite zero counts', async (t) => {
  let enumerations = 0;
  const h = setup(t, {
    listOwnerNodes: async () => {
      if (++enumerations === 1) throw new Error('snapshot unavailable');
      return [];
    },
  });
  await h.advance(20_000);
  assert.equal(h.warnings[0]!.data.error, 'snapshot unavailable');
  await h.advance(100_000);
  assert.equal(enumerations, 2);
  assert.equal(h.entry('pass complete').totalNodes, 0);
  assert.equal(h.entry('pass complete').ratePerSecond, 0);
  assert.equal(h.entry('pass complete').etaSeconds, 0);
});

test('stop during enumeration prevents any refresh admission', async (t) => {
  const owners = deferred<Array<{ mqttUsername: string; isActive: boolean; nodeIds: string[] }>>();
  let calls = 0;
  const h = setup(t, {
    listOwnerNodes: () => owners.promise,
    refreshLastHop: async () => { calls += 1; return true; },
  });
  await h.advance(20_000);
  h.stop();
  owners.resolve([{ mqttUsername: 'owner', isActive: true, nodeIds: ['a'] }]);
  await setImmediate();
  assert.equal(calls, 0);
  assert.equal(h.entry('pass stopped').completedRefreshes, 0);
  await h.advance(300_000);
  assert.equal(h.logs.filter((log) => log.message.endsWith(' progress')).length, 0);
});

test('invalid intervals disable all scheduled work', async (t) => {
  let calls = 0;
  const h = setup(t, {
    intervalMs: 0,
    listOwnerNodes: async () => { calls += 1; return []; },
  });
  await h.advance(300_000);
  assert.equal(calls, 0);
  assert.equal(h.warnings[0]!.message, '[owner-last-hop-prewarm] disabled (invalid interval)');
});
