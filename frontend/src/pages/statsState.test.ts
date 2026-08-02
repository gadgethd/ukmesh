import assert from 'node:assert/strict';
import test from 'node:test';
import { isStatsPayload, isStatsPayloadEmpty } from './statsState.js';

test('stats response validation rejects error bodies and recognizes an empty dataset', () => {
  assert.equal(isStatsPayload({ error: 'failed' }), false);
  assert.equal(isStatsPayload({ summary: { totalPackets24h: '0' } }), false);
  const empty = {
    summary: {
      totalPackets24h: 0,
      totalPackets7d: 0,
      uniqueRadios24h: 0,
    },
    packetsPerHour: [],
    packetsPerDay: [],
    observerRegions: [],
  };
  assert.equal(isStatsPayload(empty), true);
  assert.equal(isStatsPayloadEmpty(empty), true);
  assert.equal(isStatsPayloadEmpty({
    ...empty,
    summary: { ...empty.summary, totalPackets24h: 1 },
  }), false);
});
