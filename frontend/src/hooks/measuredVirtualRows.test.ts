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

test('fractional ranges cover every visible row and preserve total height across overscan boundaries', () => {
  const heights = Array.from({ length: 50 }, (_, index) => [58.375, 92.125, 211.25][index % 3]!);
  const keys = heights.map((_, index) => String(index));
  const offsets = rowOffsets(keys, new Map(keys.map((key, index) => [key, heights[index]!])));
  const total = heights.reduce((sum, height) => sum + height, 0);
  const positions = [-20, 0, total, total + 100, ...offsets.flatMap((offset) => [offset - 0.125, offset, offset + 0.125])];
  for (const position of positions) {
    for (const viewportHeight of [1, 84.125, 667]) {
      for (const overscan of [0, 5, 60]) {
        const range = measuredRowRange(offsets, position, viewportHeight, overscan);
        assert.ok(range.start >= 0 && range.start <= range.end && range.end <= heights.length);
        assert.ok(range.top >= 0 && range.bottom >= 0);
        const renderedHeight = heights.slice(range.start, range.end).reduce((sum, height) => sum + height, 0);
        assert.equal(range.top + renderedHeight + range.bottom, total);
        let rowTop = 0;
        for (const [index, height] of heights.entries()) {
          if (rowTop + height > position && rowTop < position + viewportHeight) {
            assert.ok(index >= range.start && index < range.end, `visible row ${index} missing at ${position}px`);
          }
          rowTop += height;
        }
      }
    }
  }
});
