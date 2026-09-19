import assert from 'node:assert/strict';
import test from 'node:test';
import { measuredRowRange, rowAtOffset, rowOffsets } from './measuredVirtualRows.js';

test('measured border boxes, including wrapped/tagged rows, determine spacers', () => {
  const offsets = rowOffsets(['a', 'b', 'c', 'd'], new Map([['a', 42.5], ['b', 181], ['c', 60]]));
  assert.deepEqual(offsets, [0, 42.5, 223.5, 283.5, 359.5]);
  assert.deepEqual(measuredRowRange(offsets, 70, 180, 0), { start: 1, end: 3, top: 42.5, bottom: 76 });
  assert.equal(rowAtOffset(offsets, 42.5), 1);
  assert.equal(rowAtOffset(offsets, 1e6), 3);
});

test('reordering and filtering reuse measurements by packet identity', () => {
  const heights = new Map([['a', 110], ['b', 30], ['c', 150]]);
  assert.deepEqual(rowOffsets(['new', 'c', 'a'], heights), [0, 76, 226, 336]);
  assert.deepEqual(measuredRowRange([0], 900, 400), { start: 0, end: 0, top: 0, bottom: 0 });
});
