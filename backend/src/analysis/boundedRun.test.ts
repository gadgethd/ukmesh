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
