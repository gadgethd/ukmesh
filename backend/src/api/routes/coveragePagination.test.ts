import assert from 'node:assert/strict';
import test from 'node:test';
import {
  COVERAGE_DEFAULT_LIMIT,
  COVERAGE_MAX_LIMIT,
  boundCoveragePage,
  parseCoverageBounds,
  parseCoverageCursor,
  parseCoverageLimit,
} from './coveragePagination.js';

test('parseCoverageBounds accepts an ordered, bounded viewport', () => {
  assert.deepEqual(parseCoverageBounds('-2.5,50.1,1.2,55.7'), {
    minLon: -2.5,
    minLat: 50.1,
    maxLon: 1.2,
    maxLat: 55.7,
  });
});

test('parseCoverageBounds rejects invalid and excessively large viewports', () => {
  assert.equal(parseCoverageBounds('nope'), null);
  assert.equal(parseCoverageBounds('2,50,1,51'), null);
  assert.equal(parseCoverageBounds('-30,40,30,60'), null);
  assert.equal(parseCoverageBounds('-181,40,1,50'), null);
});

test('coverage limits are defaulted and clamped', () => {
  assert.equal(parseCoverageLimit(undefined), COVERAGE_DEFAULT_LIMIT);
  assert.equal(parseCoverageLimit('abc'), COVERAGE_DEFAULT_LIMIT);
  assert.equal(parseCoverageLimit('0'), 1);
  assert.equal(parseCoverageLimit('999'), COVERAGE_MAX_LIMIT);
});

test('coverage cursors accept only canonical node identifiers', () => {
  assert.equal(parseCoverageCursor(undefined), null);
  assert.equal(parseCoverageCursor('a'.repeat(64)), 'A'.repeat(64));
  assert.equal(parseCoverageCursor('not-a-node'), undefined);
});

test('boundCoveragePage caps row count and reports a continuation cursor', () => {
  const rows = [
    { node_id: 'A', geom: 'one' },
    { node_id: 'B', geom: 'two' },
    { node_id: 'C', geom: 'three' },
  ];
  assert.deepEqual(boundCoveragePage(rows, 2, 1_000), {
    items: rows.slice(0, 2),
    hasMore: true,
    nextCursor: 'B',
  });
});

test('boundCoveragePage enforces a byte budget without producing an empty page', () => {
  const rows = [
    { node_id: 'A', geom: 'x'.repeat(100) },
    { node_id: 'B', geom: 'x'.repeat(100) },
  ];
  assert.deepEqual(boundCoveragePage(rows, 2, 50), {
    items: [{ node_id: 'A', truncated: true }],
    hasMore: true,
    nextCursor: 'A',
  });
});
