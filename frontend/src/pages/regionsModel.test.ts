import assert from 'node:assert/strict';
import test from 'node:test';
import {
  confidenceFor,
  convexHull,
  isEstablished,
  memberIdSet,
  regionColor,
  regionRing,
  regionsForDisplay,
  type RegionMember,
  type RegionSummary,
} from './regionsModel.js';

function member(
  nodeId: string, role: number | null, lat: number | null, lon: number | null,
  packets = 5, isBridge = false,
): RegionMember {
  return { nodeId, name: nodeId, role, lat, lon, packets, isBridge };
}

function region(members: RegionMember[], overrides: Partial<RegionSummary> = {}): RegionSummary {
  return {
    name: 'yorkshire',
    packetsTotal: 100,
    memberCount: members.length,
    repeaterMemberCount: members.filter((entry) => entry.role === 2).length,
    confidence: 'confirmed',
    firstSeen: null,
    lastSeen: null,
    members,
    ...overrides,
  };
}

test('convexHull returns the square hull with interior points excluded', () => {
  const hull = convexHull([[0, 0], [2, 0], [2, 2], [0, 2], [1, 1], [0.5, 1.5]]);
  assert.equal(hull.length, 4);
  const corners = new Set(hull.map(([x, y]) => `${x},${y}`));
  for (const corner of ['0,0', '2,0', '2,2', '0,2']) assert.ok(corners.has(corner), `missing ${corner}`);
});

test('convexHull collapses collinear and duplicate points', () => {
  assert.deepEqual(convexHull([[0, 0], [1, 1], [2, 2], [1, 1]]), [[0, 0], [2, 2]]);
  assert.deepEqual(convexHull([[5, 5]]), [[5, 5]]);
  assert.deepEqual(convexHull([]), []);
});

test('regionRing builds a closed ring preferring repeater members', () => {
  const ring = regionRing(region([
    member('A', 2, 54.0, -1.0),
    member('B', 2, 54.2, -0.8),
    member('C', 2, 53.8, -1.4),
    member('D', 1, 60.0, 5.0),
  ]));
  assert.ok(ring);
  assert.equal(ring.length, 4);
  assert.deepEqual(ring[0], ring[ring.length - 1]);
  for (const [, lat] of ring) assert.ok(lat < 55, 'far companion must not shape the repeater hull');
});

test('regionRing falls back to all located members when repeaters are scarce', () => {
  const ring = regionRing(region([
    member('A', 2, 54.0, -1.0),
    member('B', 1, 54.2, -0.8),
    member('C', 1, 53.8, -1.4),
  ]));
  assert.ok(ring);
  assert.equal(ring.length, 4);
});

test('regionRing returns null for degenerate inputs', () => {
  assert.equal(regionRing(region([member('A', 2, 54, -1), member('B', 2, 54.1, -0.9)])), null);
  assert.equal(regionRing(region([
    member('A', 2, 54, -1),
    member('B', 2, null, null),
    member('C', 2, 54.02, -1.02),
  ])), null);
  assert.equal(regionRing(region([member('A', 2, 54, -1), member('B', 2, 54.1, -0.9), member('C', 2, 54.2, -0.8)])), null);
});

test('regionRing ignores null-island placeholder members', () => {
  const ring = regionRing(region([
    member('A', 2, 54.0, -1.0),
    member('B', 2, 54.2, -0.8),
    member('C', 2, 53.8, -1.4),
    member('JUNK', 2, 0, 0),
  ]));
  assert.ok(ring);
  for (const [lon, lat] of ring) {
    assert.ok(lat > 50 && lon < 0, 'placeholder coordinates must not shape the hull');
  }
});

test('regionColor is stable and well-formed', () => {
  assert.equal(regionColor('yorkshire'), regionColor('yorkshire'));
  assert.match(regionColor('eng-ne'), /^#[0-9a-f]{6}$/);
});

test('confidenceFor mirrors the backend evidence tiers', () => {
  assert.equal(confidenceFor(9), 'confirmed');
  assert.equal(confidenceFor(4), 'confirmed');
  assert.equal(confidenceFor(3), 'probable');
  assert.equal(confidenceFor(2), 'probable');
  assert.equal(confidenceFor(1), 'single');
});

test('memberIdSet collects every member id', () => {
  const ids = memberIdSet(region([member('A', 2, 54, -1), member('B', 1, 53, -2)]));
  assert.deepEqual([...ids].sort(), ['A', 'B']);
});

test('isEstablished and regionsForDisplay gate low-confidence regions', () => {
  assert.equal(isEstablished({ packetsTotal: 3 }), true);
  assert.equal(isEstablished({ packetsTotal: 2 }), false);
  const strong = region([member('A', 2, 54, -1)], { name: 'yorkshire', packetsTotal: 100 });
  const weak = region([member('B', 2, 53, -2)], { name: 'micro', packetsTotal: 1 });
  assert.deepEqual(regionsForDisplay([strong, weak], false).map((entry) => entry.name), ['yorkshire']);
  assert.deepEqual(regionsForDisplay([strong, weak], true).map((entry) => entry.name), ['yorkshire', 'micro']);
});

test('regionRing excludes universal bridge members from the hull', () => {
  const ring = regionRing(region([
    member('A', 2, 54.0, -1.0),
    member('B', 2, 54.2, -0.8),
    member('C', 2, 53.8, -1.4),
    member('HUB', 2, 60.0, 5.0, 9000, true),
  ]));
  assert.ok(ring);
  for (const [, lat] of ring) assert.ok(lat < 55, 'bridge members must not shape the hull');

  const allBridges = regionRing(region([
    member('A', 2, 54.0, -1.0, 5, true),
    member('B', 2, 54.2, -0.8, 5, true),
    member('C', 2, 53.8, -1.4, 5, true),
  ]));
  assert.equal(allBridges, null);
});
