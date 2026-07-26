import assert from 'node:assert/strict';
import test from 'node:test';
import { runBoundedItems } from './boundedRun.js';

for (const failedIndex of [0, 1, 2]) {
  test(`bounded run isolates an item failure at index ${failedIndex}`, async () => {
    const result = await runBoundedItems([1, 2, 3], async (value, index) => {
      if (index === failedIndex) throw new Error('sentinel');
      return value * 2;
    }, {
      windowStart: new Date(0),
      windowEnd: new Date(1),
      deadlineMs: 1_000,
    });
    assert.equal(result.status, 'partial');
    assert.equal(result.checkpoint, 3);
    assert.equal(result.results.length, 2);
    assert.deepEqual(result.errors, [{ index: failedIndex, message: 'sentinel' }]);
  });
}

test('bounded run reports timeout without claiming the unprocessed suffix', async () => {
  let clock = 0;
  const result = await runBoundedItems([1, 2, 3], async (value) => {
    clock += 6;
    return value;
  }, {
    windowStart: new Date(0),
    windowEnd: new Date(1),
    deadlineMs: 10,
    now: () => clock,
  });
  assert.equal(result.status, 'timed_out');
  assert.equal(result.checkpoint, 2);
  assert.deepEqual(result.results, [1, 2]);
});

test('bounded run caps concurrency and preserves input order in results', async () => {
  let active = 0;
  let maxActive = 0;
  const result = await runBoundedItems([1, 2, 3, 4, 5, 6], async (value) => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    await new Promise((resolve) => setImmediate(resolve));
    active -= 1;
    return value * 2;
  }, {
    windowStart: new Date(0),
    windowEnd: new Date(1),
    deadlineMs: 1_000,
    concurrency: 3,
  });

  assert.equal(result.status, 'complete');
  assert.equal(result.checkpoint, 6);
  assert.equal(maxActive, 3);
  assert.deepEqual(result.results, [2, 4, 6, 8, 10, 12]);
});

test('bounded run can discard successful values without changing completion accounting', async () => {
  const result = await runBoundedItems([1, 2, 3], async (value) => {
    if (value === 2) throw new Error('sentinel');
    return { large: 'x'.repeat(1000) };
  }, {
    windowStart: new Date(0),
    windowEnd: new Date(1),
    deadlineMs: 1_000,
    collectResults: false,
  });

  assert.equal(result.status, 'partial');
  assert.equal(result.checkpoint, 3);
  assert.deepEqual(result.results, []);
  assert.deepEqual(result.errors, [{ index: 1, message: 'sentinel' }]);
});
