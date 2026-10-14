import assert from 'node:assert/strict';
import test from 'node:test';
import { buildOwnerHeardNeighbors } from './ownerService.js';

const time = '2026-09-08T01:00:00Z';
const direct = [{ node_id: 'direct', last_seen: time, avg_rssi: -103.5, avg_snr: 7.25 }];

test('absent neighbour publisher uses real direct packet signal with explicit provenance', () => {
  assert.deepEqual(buildOwnerHeardNeighbors(undefined, direct), {
    heardNeighbors: [{ id: 'direct', last_seen_at: '2026-09-08T01:00:00.000Z', rssi: -103.5, snr: 7.25, sampled_at: null }],
    heardNeighborsSource: 'direct_packets', heardNeighborsSampledAt: null,
  });
  assert.equal(buildOwnerHeardNeighbors(undefined, []).heardNeighborsSource, 'none');
  assert.equal(buildOwnerHeardNeighbors({ time, neighbors: null }, direct).heardNeighborsSource, 'direct_packets');
});

test('published lists, including empty lists, take precedence over direct packet fallback', () => {
  const empty = buildOwnerHeardNeighbors({ time, neighbors: [] }, direct);
  assert.equal(empty.heardNeighborsSource, 'reported');
  assert.equal(empty.heardNeighborsSampledAt, '2026-09-08T01:00:00.000Z');
  assert.deepEqual(empty.heardNeighbors, []);
  const result = buildOwnerHeardNeighbors({ time, neighbors: [{ pubkey: 'reported', RSSI: -95, SNR: 0 }] }, direct);
  assert.equal(result.heardNeighborsSource, 'reported');
  assert.deepEqual(result.heardNeighbors.map(n => n.id), ['reported']);
  assert.equal(result.heardNeighbors[0]!.snr, 0);
});

test('neighbour timestamps normalize ISO, epoch seconds, milliseconds and relative ages before sorting', () => {
  const epoch = Date.parse(time) / 1_000;
  const result = buildOwnerHeardNeighbors({ time, neighbors: [
    { id: 'iso', last_seen: '2026-09-08T00:00:00-01:00', rssi: 'not a number' },
    { node_id: 'seconds', lastSeen: epoch - 1, snr_db: 1 },
    { public_key: 'milliseconds', last_seen: (epoch - 2) * 1_000 },
    { id: 'relative', heard_secs_ago: 3 },
    { id: 'unknown', last_seen: 'invalid', rssi: Infinity, snr: null },
    { id: 'fallback-timestamp', last_seen: 'invalid', lastSeen: String(epoch - 4) },
    { id: 'negative-age', heard_secs_ago: -10 },
    null, [], 'invalid', {}, { id: '' },
  ] }, direct);
  assert.deepEqual(result.heardNeighbors.map(n => n.id), ['iso', 'seconds', 'milliseconds', 'relative', 'fallback-timestamp', 'unknown', 'negative-age']);
  assert.equal(result.heardNeighbors[0]!.rssi, null);
  assert.equal(result.heardNeighbors[3]!.last_seen_at, '2026-09-08T00:59:57.000Z');
  assert.equal(result.heardNeighbors[5]!.last_seen_at, null);
  assert.equal(result.heardNeighbors[5]!.rssi, null);
});

test('neighbour cap keeps the latest 32 rather than the first 32 reported entries', () => {
  const result = buildOwnerHeardNeighbors({ time, neighbors: Array.from({ length: 40 }, (_, i) => ({ id: String(i), heard_secs_ago: 39 - i })) }, []);
  assert.equal(result.heardNeighbors.length, 32);
  assert.equal(result.heardNeighbors[0]!.id, '39');
  assert.equal(result.heardNeighbors[31]!.id, '8');
});
