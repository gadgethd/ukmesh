import assert from 'node:assert/strict';
import test, { beforeEach } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';

// Env is read at CALL time (window/enabled/pending-max), so tests can flip
// it per test. Use a tiny real window — Node 18 has no mock timers here.
process.env['PATH_SLOW_MODE_ENABLED'] = 'true';
process.env['PATH_SLOW_MODE_WINDOW_MS'] = '40';

import {
  scheduleSlowResolution,
  slowModeRemainingMs,
  slowModePendingCount,
  slowModeStatus,
  __setSlowModePoolForTests,
  __setSlowModeRecorderForTests,
  __resetSlowModeForTests,
} from './slowMode.js';

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

beforeEach(() => {
  process.env['PATH_SLOW_MODE_ENABLED'] = 'true';
  process.env['PATH_SLOW_MODE_WINDOW_MS'] = '40';
  delete process.env['PATH_SLOW_MODE_PENDING_MAX'];
  __resetSlowModeForTests();
  fakePool = makeFakePool();
  __setSlowModePoolForTests(fakePool);
  __setSlowModeRecorderForTests(recorder.fn.bind(recorder));
  recorder.calls.length = 0;
});

test('schedule is idempotent per (hash, network) and runs ONE final resolve', async () => {
  scheduleSlowResolution('AA11', 'ukmesh');
  scheduleSlowResolution('AA11', 'ukmesh');
  scheduleSlowResolution('AA11', 'othernet');
  assert.equal(slowModePendingCount(), 2);
  assert.ok(slowModeRemainingMs('AA11', 'ukmesh') > 0);

  await sleep(120); // window is 40ms — let it fire

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

test('remaining ms tracks the window and reaches 0 after close', async () => {
  process.env['PATH_SLOW_MODE_WINDOW_MS'] = '60';
  scheduleSlowResolution('DD33', 'ukmesh');
  const t0 = Date.now();
  const remaining = slowModeRemainingMs('DD33', 'ukmesh');
  assert.ok(remaining > 0 && remaining <= 60);
  await sleep(150);
  assert.equal(slowModeRemainingMs('DD33', 'ukmesh'), 0);
  assert.ok(Date.now() - t0 >= 50);
});
