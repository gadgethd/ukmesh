import assert from 'node:assert/strict';
import test from 'node:test';
import {
  availableRfCoverageTiers,
  isValidRfCoverageTile,
  rfNodeCoverageState,
  type RfCoverageMeta,
} from './useRfCoverage.js';

const validTile = {
  image: 'tiles/standard/0-0.png',
  bounds: { South: 49, North: 55, West: -6, East: 2 },
};

test('coverage availability accepts progressive standard then precision tiles', () => {
  const meta = {
    generated_at: '2026-08-02T10:00:00Z', source: 'test', version: 'v0.1.32', complete: false,
    coverage: { standard: { tiles: [validTile] } },
  } as unknown as RfCoverageMeta;
  assert.deepEqual(availableRfCoverageTiers(meta), ['standard']);
  meta.coverage!.precision = { tiles: [{ ...validTile, image: 'tiles/precision/0-0.png' }] } as never;
  assert.deepEqual(availableRfCoverageTiers(meta), ['standard', 'precision']);
});

test('node coverage exposes pending, available, stale, and error states', () => {
  const key = 'a'.repeat(64);
  const base = {
    generated_at: '2026-08-03T10:00:00Z', source: 'test', version: 'dev', complete: true,
    node_coverage: {
      [key]: {
        dataset_id: `n${'1'.repeat(24)}`,
        state: 'available', position_status: 'active', lat: 55, lon: -3,
        requested_at: '2026-08-03T10:00:00Z', updated_at: '2026-08-03T10:00:00Z',
        fresh_until: '2026-08-03T16:00:00Z',
        standard: { tiles: [{ ...validTile, image: `tiles/nodes/n${'1'.repeat(24)}/0-0.png` }] },
      },
    },
  } as unknown as RfCoverageMeta;
  assert.equal(rfNodeCoverageState(null, key), 'pending');
  assert.equal(rfNodeCoverageState(base, key, Date.parse('2026-08-03T12:00:00Z')), 'available');
  assert.equal(rfNodeCoverageState(base, key, Date.parse('2026-08-03T17:00:00Z')), 'stale');
  base.node_coverage![key]!.state = 'failed';
  assert.equal(rfNodeCoverageState(base, key), 'error');
});

test('coverage tile paths and geographic bounds are validated before map insertion', () => {
  assert.equal(isValidRfCoverageTile(validTile), true);
  assert.equal(isValidRfCoverageTile({ ...validTile, image: '../secret.png' }), false);
  assert.equal(isValidRfCoverageTile({ ...validTile, bounds: { ...validTile.bounds, North: 48 } }), false);
});
