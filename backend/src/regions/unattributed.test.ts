import assert from 'node:assert/strict';
import test from 'node:test';
import {
  clusterUnattributedPackets,
  DEFAULT_CLUSTER_MIN_PACKETS,
  haversineKm,
  type LocatedNode,
  type UnattributedPacket,
} from './unattributed.js';

const BRISTOL: LocatedNode = { nodeId: 'A1'.repeat(32), lat: 51.4545, lon: -2.5879, name: 'Bristol relay' };
const BATH: LocatedNode = { nodeId: 'A2'.repeat(32), lat: 51.3811, lon: -2.3590, name: 'Bath relay' };
const NORWICH: LocatedNode = { nodeId: 'B1'.repeat(32), lat: 52.6309, lon: 1.2974, name: 'Norwich relay' };
const LEEDS: LocatedNode = { nodeId: 'C1'.repeat(32), lat: 53.8008, lon: -1.5491, name: 'Leeds relay' };

function packetsAt(node: LocatedNode, count: number, offset = 0): UnattributedPacket[] {
  return Array.from({ length: count }, (_, index) => ({
    hash: `${node.nodeId.slice(0, 4)}-${offset + index}`,
    nodes: [node],
    firstSeen: '2026-10-01T00:00:00.000Z',
    lastSeen: '2026-10-02T00:00:00.000Z',
  }));
}

test('haversineKm returns a plausible UK distance', () => {
  const londonToBristol = haversineKm(51.5074, -0.1278, 51.4545, -2.5879);
  assert.ok(londonToBristol > 150 && londonToBristol < 180, `got ${londonToBristol}`);
  assert.equal(haversineKm(51.5, -2.5, 51.5, -2.5), 0);
});

test('separates well-separated groups and orders ids by packet count', () => {
  const result = clusterUnattributedPackets([
    ...packetsAt(BRISTOL, 20),
    ...packetsAt(NORWICH, 40),
  ], { radiusKm: 60, minPackets: 10 });
  assert.equal(result.clusters.length, 2);
  assert.equal(result.clusters[0]!.id, 'u1');
  assert.equal(result.clusters[0]!.packets, 40);
  assert.equal(result.clusters[0]!.lat, 52.6309);
  assert.equal(result.clusters[1]!.id, 'u2');
  assert.equal(result.clusters[1]!.packets, 20);
  assert.equal(result.clusteredPackets, 60);
});

test('merges nodes within the radius into one footprint', () => {
  const result = clusterUnattributedPackets([
    ...packetsAt(BRISTOL, 8),
    ...packetsAt(BATH, 8),
  ], { radiusKm: 60, minPackets: 10 });
  assert.equal(result.clusters.length, 1);
  const cluster = result.clusters[0]!;
  assert.equal(cluster.packets, 16);
  assert.equal(cluster.nodeCount, 2);
  // Centroid sits between the two relays.
  assert.ok(cluster.lat > 51.38 && cluster.lat < 51.46);
  assert.ok(cluster.lon > -2.59 && cluster.lon < -2.36);
});

test('discards clusters below the packet floor and reports why', () => {
  const result = clusterUnattributedPackets([
    ...packetsAt(BRISTOL, DEFAULT_CLUSTER_MIN_PACKETS),
    ...packetsAt(LEEDS, 3),
  ], { radiusKm: 60, minPackets: DEFAULT_CLUSTER_MIN_PACKETS });
  assert.equal(result.clusters.length, 1);
  assert.equal(result.clusters[0]!.packets, DEFAULT_CLUSTER_MIN_PACKETS);
  assert.equal(result.discardedPackets, 3);
});

test('packets with no locatable node are counted, never clustered', () => {
  const result = clusterUnattributedPackets([
    ...packetsAt(BRISTOL, 12),
    { hash: 'no-coords', nodes: [{ nodeId: 'D1'.repeat(32), lat: Number.NaN, lon: 0 }] },
    { hash: 'null-island', nodes: [{ nodeId: 'D2'.repeat(32), lat: 0, lon: 0 }] },
    { hash: 'no-nodes', nodes: [] },
  ], { radiusKm: 60, minPackets: 10 });
  assert.equal(result.clusters.length, 1);
  assert.equal(result.unlocatedPackets, 3);
});

test('top nodes are ordered by appearances and capped', () => {
  const packets: UnattributedPacket[] = [];
  for (let index = 0; index < 6; index += 1) {
    packets.push({ hash: `b-${index}`, nodes: [BRISTOL] });
  }
  for (let index = 0; index < 3; index += 1) {
    packets.push({ hash: `ba-${index}`, nodes: [BRISTOL, BATH] });
  }
  for (let index = 0; index < 4; index += 1) {
    packets.push({ hash: `x-${index}`, nodes: [BRISTOL, BATH] });
  }
  const result = clusterUnattributedPackets(packets, { radiusKm: 60, minPackets: 10, maxTopNodes: 1 });
  const cluster = result.clusters[0]!;
  assert.equal(cluster.topNodes.length, 1);
  assert.equal(cluster.topNodes[0]!.nodeId, BRISTOL.nodeId);
  assert.equal(cluster.topNodes[0]!.packets, 13);
});

test('is deterministic for identical input', () => {
  const input = [...packetsAt(BRISTOL, 15), ...packetsAt(NORWICH, 25)];
  const first = clusterUnattributedPackets(input, { radiusKm: 60, minPackets: 10 });
  const second = clusterUnattributedPackets(input, { radiusKm: 60, minPackets: 10 });
  assert.deepEqual(first, second);
});

test('tracks the observation window of each cluster', () => {
  const result = clusterUnattributedPackets([
    { hash: 'a', nodes: [BRISTOL], firstSeen: '2026-09-01T00:00:00.000Z', lastSeen: '2026-09-02T00:00:00.000Z' },
    ...packetsAt(BRISTOL, 11, 100),
    { hash: 'b', nodes: [BRISTOL], firstSeen: '2026-10-05T00:00:00.000Z', lastSeen: '2026-10-06T00:00:00.000Z' },
  ], { radiusKm: 60, minPackets: 10 });
  const cluster = result.clusters[0]!;
  assert.equal(cluster.firstSeen, '2026-09-01T00:00:00.000Z');
  assert.equal(cluster.lastSeen, '2026-10-06T00:00:00.000Z');
});
