import assert from 'node:assert/strict';
import test from 'node:test';
import { isStatsPayload, isStatsPayloadEmpty, statsGeneratedAt, isLiveObserverSnapshot, mergeObserverRegions } from './state.js';

test('live observer overlays retain historical totals and identify newly active regions without invented history', () => {
  const historical = ['A', 'B'].map(iata => ({ iata, activeObservers: 9, observers: 12,
    packets24h: 30, packets7d: 100, lastPacketAt: '2026-11-01T00:00:00Z',
    health: { score: 90, status: 'healthy' as const, factors: {} }, series: [{ day: '2026-11-01', count: 10 }] }));
  const live = { generatedAt: '2026-11-12T12:00:00Z', windowMinutes: 15,
    regions: [{ iata: 'A', activeObservers: 2, lastPacketAt: '2026-11-12T11:59:00Z' },
      { iata: 'C', activeObservers: 1, lastPacketAt: '2026-11-12T11:58:00Z' }] };
  const result = mergeObserverRegions(historical, live);
  assert.equal(result[0]?.activeObservers, 2);
  assert.equal(result[0]?.packets7d, 100);
  assert.equal(result[0]?.lastPacketAt, '2026-11-12T11:59:00Z');
  assert.equal(result[1]?.activeObservers, 0);
  assert.equal(result[1]?.packets24h, 30);
  assert.equal(result[2]?.historyAvailable, false);
  assert.deepEqual(result[2]?.series, []);
  assert.equal(historical[0]?.activeObservers, 9);
  assert.equal(mergeObserverRegions(historical, null)[0]?.activeObservers, 9);
});

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
