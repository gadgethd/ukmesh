import assert from 'node:assert/strict';
import test from 'node:test';
import { measuredRowRange, rowAtOffset, rowOffsets } from './measuredVirtualRows.js';

test('measured row heights determine prefix offsets and both spacers', () => {
  const offsets = rowOffsets(
    ['short', 'wrapped', 'tagged', 'unmeasured'],
    new Map([['short', 58], ['wrapped', 92], ['tagged', 78]]),
  );

  assert.deepEqual(offsets, [0, 58, 150, 228, 304]);
  assert.deepEqual(measuredRowRange(offsets, 140, 80, 0), {
    start: 1,
    end: 3,
    top: 58,
    bottom: 76,
  });
});

test('offset lookup uses measured boundaries and clamps empty or past-end lists', () => {
  const offsets = rowOffsets(['a', 'b', 'c'], new Map([['a', 40], ['b', 120], ['c', 60]]));

  assert.equal(rowAtOffset(offsets, 0), 0);
  assert.equal(rowAtOffset(offsets, 40), 1);
  assert.equal(rowAtOffset(offsets, 159), 1);
  assert.equal(rowAtOffset(offsets, 160), 2);
  assert.equal(rowAtOffset(offsets, 1_000), 2);
  assert.equal(rowAtOffset([0], 1_000), 0);
  assert.deepEqual(measuredRowRange([0], 1_000, 300), { start: 0, end: 0, top: 0, bottom: 0 });
});

test('measurements follow stable packet keys across reorder and filters', () => {
  const heights = new Map([['a', 110], ['b', 30], ['c', 150]]);

  assert.deepEqual(rowOffsets(['new', 'c', 'a'], heights), [0, 76, 226, 336]);
});
