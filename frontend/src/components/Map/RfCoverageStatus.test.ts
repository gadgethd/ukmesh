import assert from 'node:assert/strict';
import test from 'node:test';
import { formatRfEta } from './RfCoverageStatus.js';
import { rfCoverageTileUrl } from './RfCoverageOverlay.js';

test('coverage tile refresh URLs are revisioned without changing the path contract', () => {
  assert.equal(
    rfCoverageTileUrl('tiles/standard/0-1.png', 'run id:3'),
    '/rf-coverage/tiles/standard/0-1.png?revision=run%20id%3A3',
  );
});

test('coverage ETA is readable on compact mobile controls', () => {
  assert.equal(formatRfEta(20), '<1 min');
  assert.equal(formatRfEta(5_400), '1h 30m');
  assert.equal(formatRfEta(undefined), null);
});
