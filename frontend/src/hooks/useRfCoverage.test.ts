import assert from 'node:assert/strict';
import test from 'node:test';
import {
  availableRfCoverageTiers,
  isValidRfCoverageTile,
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

test('coverage tile paths and geographic bounds are validated before map insertion', () => {
  assert.equal(isValidRfCoverageTile(validTile), true);
  assert.equal(isValidRfCoverageTile({ ...validTile, image: '../secret.png' }), false);
  assert.equal(isValidRfCoverageTile({ ...validTile, bounds: { ...validTile.bounds, North: 48 } }), false);
});
