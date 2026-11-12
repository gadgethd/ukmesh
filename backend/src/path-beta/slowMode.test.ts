import assert from 'node:assert/strict';
import test, { beforeEach } from 'node:test';
import { setImmediate as nextTurn } from 'node:timers/promises';

// Env is read at CALL time (window/enabled/pending-max), so tests can flip
// it per test. Mock the clock: a loaded CI host can exhaust a real 50ms
// window before the assertions run, even though the scheduler is correct.
process.env['PATH_SLOW_MODE_ENABLED'] = 'true';
process.env['PATH_SLOW_MODE_WINDOW_MS'] = '40';

import { createSlowResolutionScheduler, type SlowResolutionScheduler } from './slowMode.js';

let scheduler: SlowResolutionScheduler;
const scheduleSlowResolution: SlowResolutionScheduler['scheduleSlowResolution'] = (...args) => scheduler.scheduleSlowResolution(...args);
const slowModeRemainingMs: SlowResolutionScheduler['slowModeRemainingMs'] = (...args) => scheduler.slowModeRemainingMs(...args);
const slowModePendingCount = () => scheduler.slowModePendingCount();
const slowModeStatus = () => scheduler.slowModeStatus();

type FakePool = {
  calls: Array<{ type: string; packetHash: string; network: string }>;
  runBackground<T>(job: unknown): Promise<T | null>;
};
function makeFakePool(): FakePool {
  const pool: FakePool = {
    calls: [],
    async runBackground<T>(job: unknown): Promise<T | null> {
      this.calls.push(job as { type: string; packetHash: string; network: string });
      return { observers: ['obs-a', 'obs-b'], canonicalPath: [] } as T;
    },
  };
  return pool;
}

let fakePool: FakePool;
const recorder = {
  calls: [] as Array<{ packetHash: string; network: string }>,
  async fn(packetHash: string, network: string): Promise<void> {
    this.calls.push({ packetHash, network });
  },
};

beforeEach((t) => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1_700_000_000_000 });
  process.env['PATH_SLOW_MODE_ENABLED'] = 'true';
  process.env['PATH_SLOW_MODE_WINDOW_MS'] = '40';
  delete process.env['PATH_SLOW_MODE_PENDING_MAX'];
  fakePool = makeFakePool();
  scheduler = createSlowResolutionScheduler({ pool: fakePool, record: recorder.fn.bind(recorder) });
  t.after(scheduler.close);
  recorder.calls.length = 0;
});

test('schedule is idempotent per (hash, network) and runs ONE final resolve', async (t) => {
  scheduleSlowResolution('AA11', 'ukmesh');
  scheduleSlowResolution('AA11', 'ukmesh');
  scheduleSlowResolution('AA11', 'othernet');
  assert.equal(slowModePendingCount(), 2);
  assert.ok(slowModeRemainingMs('AA11', 'ukmesh') > 0);

  t.mock.timers.tick(120);
  await nextTurn(); // finish the resolver's promise chain

  assert.equal(slowModePendingCount(), 0);
  assert.equal(slowModeRemainingMs('AA11', 'ukmesh'), 0);
  assert.equal(fakePool.calls.length, 2);
  assert.deepEqual(fakePool.calls[0], { type: 'resolveMulti', packetHash: 'AA11', network: 'ukmesh' });
  // observability recorded for both finals
  assert.equal(recorder.calls.length, 2);
});

test('disabled mode schedules nothing', async () => {
  process.env['PATH_SLOW_MODE_ENABLED'] = 'false';
  scheduleSlowResolution('BB22', 'ukmesh');
  assert.equal(slowModePendingCount(), 0);
  assert.equal(slowModeRemainingMs('BB22', 'ukmesh'), 0);
  assert.equal(slowModeStatus().enabled, false);
});

test('pending set is bounded and evicts oldest on overflow', () => {
  process.env['PATH_SLOW_MODE_PENDING_MAX'] = '2';
  scheduleSlowResolution('C1', 'ukmesh');
  scheduleSlowResolution('C2', 'ukmesh');
  scheduleSlowResolution('C3', 'ukmesh'); // evicts C1
  assert.equal(slowModePendingCount(), 2);
  assert.equal(slowModeRemainingMs('C1', 'ukmesh'), 0);
  assert.ok(slowModeRemainingMs('C2', 'ukmesh') > 0);
  assert.ok(slowModeRemainingMs('C3', 'ukmesh') > 0);
});

test('remaining ms tracks the window and reaches 0 after close', async (t) => {
  process.env['PATH_SLOW_MODE_WINDOW_MS'] = '60';
  scheduleSlowResolution('DD33', 'ukmesh');
  const t0 = Date.now();
  const remaining = slowModeRemainingMs('DD33', 'ukmesh');
  assert.ok(remaining > 0 && remaining <= 60);
  t.mock.timers.tick(150);
  await nextTurn();
  assert.equal(slowModeRemainingMs('DD33', 'ukmesh'), 0);
  assert.ok(Date.now() - t0 >= 50);
});

test('closing one scheduler cancels only its own pending work', async t => {
  const firstPool = makeFakePool(), secondPool = makeFakePool();
  const first = createSlowResolutionScheduler({ pool: firstPool, record: async () => {} });
  const second = createSlowResolutionScheduler({ pool: secondPool, record: async () => {} });
  t.after(first.close); t.after(second.close);
  first.scheduleSlowResolution('owned-first', 'ukmesh');
  second.scheduleSlowResolution('owned-second', 'ukmesh');
  await first.close();
  t.mock.timers.tick(120); await nextTurn();
  assert.equal(firstPool.calls.length, 0);
  assert.equal(secondPool.calls.length, 1);
});

test('scheduler close joins active work and withholds late publication', async t => {
  let finish!: (value: unknown) => void, recorded = 0;
  const gate = new Promise<unknown>(resolve => { finish = resolve; });
  const runtime = createSlowResolutionScheduler({
    pool: { runBackground: async () => await gate as never },
    record: async () => { recorded++; },
  });
  t.after(async () => { finish(null); await runtime.close(); });
  runtime.scheduleSlowResolution('owned-active', 'ukmesh');
  t.mock.timers.tick(120); await nextTurn();
  const closing = runtime.close();
  finish({ observers: [], canonicalPath: [] }); await closing;
  assert.equal(recorded, 0);
  runtime.scheduleSlowResolution('closed', 'ukmesh');
  assert.equal(runtime.slowModePendingCount(), 0);
});
