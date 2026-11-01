import assert from 'node:assert/strict';
import test from 'node:test';
import { isStatsPayload, isStatsPayloadEmpty, statsGeneratedAt, isLiveObserverSnapshot } from './statsState.js';

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

test('fetching an old chart does not label it newly generated', () => {
  const summary = { totalPackets24h: 1, totalPackets7d: 1, uniqueRadios24h: 1 };
  assert.equal(statsGeneratedAt({ summary, snapshot: { generatedAt: '2026-11-01T16:24:21Z' } }),
    Date.parse('2026-11-01T16:24:21Z'));
  assert.equal(statsGeneratedAt({ summary }), null);
  const live = { generatedAt: '2026-11-01T20:00:00Z', windowMinutes: 15,
    regions: [{ iata: 'EDI', activeObservers: 1, lastPacketAt: '2026-11-01T19:59:59Z' }] };
  assert.equal(isLiveObserverSnapshot(live), true);
  assert.equal(isLiveObserverSnapshot({ ...live, regions: [{ iata: 'EDI', activeObservers: -1 }] }), false);
});
