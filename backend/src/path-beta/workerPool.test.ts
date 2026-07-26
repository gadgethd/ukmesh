import assert from 'node:assert/strict';
import test from 'node:test';
import {
  WorkerPool,
  WorkerPoolOverloadedError,
  WorkerPoolTimeoutError,
} from './workerPool.js';

const fixtureUrl = new URL('./workerPoolFixture.mjs', import.meta.url);

test('worker pool bounds queued interactive work without losing accepted jobs', async () => {
  const pool = new WorkerPool(fixtureUrl, 1, 1, 1, 2_000);
  try {
    const first = pool.run<string>({ value: 'first', delayMs: 50 });
    const second = pool.run<string>({ value: 'second', delayMs: 1 });
    await assert.rejects(
      pool.run({ value: 'rejected', delayMs: 1 }),
      WorkerPoolOverloadedError,
    );
    assert.deepEqual(await Promise.all([first, second]), ['first', 'second']);
    assert.deepEqual(pool.snapshot(), {
      active: 0,
      interactiveQueued: 0,
      backgroundQueued: 0,
    });
  } finally {
    await pool.close();
  }
});

test('worker pool timeout includes queue and execution time and replaces the worker', async () => {
  const pool = new WorkerPool(fixtureUrl, 1, 1, 1, 150);
  try {
    await assert.rejects(
      pool.run({ value: 'late', delayMs: 300 }),
      WorkerPoolTimeoutError,
    );
    assert.equal(await pool.run<string>({ value: 'recovered', delayMs: 1 }), 'recovered');
  } finally {
    await pool.close();
  }
});
