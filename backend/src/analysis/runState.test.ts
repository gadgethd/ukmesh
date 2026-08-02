import assert from 'node:assert/strict';
import test from 'node:test';
import { analysisRunHeartbeatIsStale } from './runState.js';

test('analysis run heartbeat remains active inside its lease window', () => {
  const now = Date.parse('2026-08-01T12:05:00.000Z');
  assert.equal(
    analysisRunHeartbeatIsStale('2026-08-01T12:04:01.000Z', now, 60_000),
    false,
  );
});

test('analysis run heartbeat expires after its lease window', () => {
  const now = Date.parse('2026-08-01T12:05:01.001Z');
  assert.equal(
    analysisRunHeartbeatIsStale(new Date('2026-08-01T12:04:00.000Z'), now, 60_000),
    true,
  );
});

test('missing and invalid heartbeats are recoverable as stale', () => {
  assert.equal(analysisRunHeartbeatIsStale(null), true);
  assert.equal(analysisRunHeartbeatIsStale('invalid'), true);
});
