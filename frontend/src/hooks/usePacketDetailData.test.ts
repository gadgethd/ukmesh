import assert from 'node:assert/strict';
import test from 'node:test';
import { LAZY_PATH_SETTLE_MS, lazyPathSettleRemainingMs } from './usePacketDetailData.js';

test('old packets resolve immediately instead of restarting the settle delay', () => {
  const now = Date.parse('2026-08-01T12:00:30.000Z');
  assert.equal(lazyPathSettleRemainingMs('2026-08-01T12:00:00.000Z', now), 0);
});

test('new packets wait only for the remainder of the propagation window', () => {
  const now = Date.parse('2026-08-01T12:00:04.000Z');
  assert.equal(
    lazyPathSettleRemainingMs('2026-08-01T12:00:00.000Z', now),
    LAZY_PATH_SETTLE_MS - 4_000,
  );
});

test('invalid timestamps do not leave lazy resolution settling forever', () => {
  assert.equal(lazyPathSettleRemainingMs('not-a-date'), 0);
});
