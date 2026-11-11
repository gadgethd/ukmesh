import assert from 'node:assert/strict';
import test from 'node:test';
import { buildRegionAreasGeoJSON, buildRegionRepeatersGeoJSON, buildUnattributedGeoJSON, type RegionRepeaterFilters } from './regionOverlay.js';
import type { RegionRepeater, RegionSummary } from '../../pages/regionsModel.js';

function repeater(overrides: Partial<RegionRepeater> = {}): RegionRepeater {
  return {
    nodeId: 'A'.repeat(64),
    name: 'Repeater',
    role: 2,
    lat: 54,
    lon: -1,
    status: 'green',
    evidence: 'scoped',
    scopedPackets: 10,
    unscopedPackets7d: 0,
    regions: { yorkshire: 10 },
    ...overrides,
  };
}

const baseFilters: RegionRepeaterFilters = {
  roleFilter: 'all',
  statusFilter: 'all',
  regionFilter: null,
  memberIds: null,
  selectedNodeId: null,
};

test('buildRegionRepeatersGeoJSON filters by role, status, region and coordinates', () => {
  const repeaters = [
    repeater(),
    repeater({ nodeId: 'B'.repeat(64), role: 1, regions: { yorkshire: 5 } }),
    repeater({ nodeId: 'C'.repeat(64), status: 'red', scopedPackets: 0, regions: {} }),
    repeater({ nodeId: 'D'.repeat(64), lat: null, lon: null }),
  ];
  const all = buildRegionRepeatersGeoJSON(repeaters, baseFilters);
  assert.equal(all.features.length, 3);
  assert.equal(all.features.some((feature) => feature.properties?.['node_id'] === 'D'.repeat(64)), false);

  const repeatersOnly = buildRegionRepeatersGeoJSON(repeaters, { ...baseFilters, roleFilter: 'repeaters' });
  assert.deepEqual(repeatersOnly.features.map((feature) => feature.properties?.['status']), ['green', 'red']);

  const greenOnly = buildRegionRepeatersGeoJSON(repeaters, { ...baseFilters, statusFilter: 'green' });
  assert.equal(greenOnly.features.length, 2);

  const byMembers = buildRegionRepeatersGeoJSON(repeaters, {
    ...baseFilters, regionFilter: 'yorkshire', memberIds: new Set(['A'.repeat(64)]),
  });
  assert.equal(byMembers.features.length, 1);
  assert.equal(byMembers.features[0]?.properties?.['node_id'], 'A'.repeat(64));

  const byRegionMap = buildRegionRepeatersGeoJSON(repeaters, { ...baseFilters, regionFilter: 'yorkshire' });
  assert.equal(byRegionMap.features.length, 2);
});

test('buildRegionRepeatersGeoJSON carries selection and tier properties', () => {
  const collection = buildRegionRepeatersGeoJSON(
    [repeater(), repeater({ nodeId: 'E'.repeat(64), scopedPackets: 1 })],
    { ...baseFilters, selectedNodeId: 'A'.repeat(64) },
  );
  const selected = collection.features.find((feature) => feature.properties?.['node_id'] === 'A'.repeat(64));
  const single = collection.features.find((feature) => feature.properties?.['node_id'] === 'E'.repeat(64));
  assert.equal(selected?.properties?.['selected'], true);
  assert.equal(selected?.properties?.['tier'], 'confirmed');
  assert.equal(single?.properties?.['tier'], 'single');
});

test('buildRegionAreasGeoJSON skips regions without a ring and marks selection', () => {
  const shapeable: RegionSummary = {
    name: 'yorkshire', packetsTotal: 100, memberCount: 3, repeaterMemberCount: 3,
    confidence: 'confirmed', firstSeen: null, lastSeen: null,
    members: [
      { nodeId: 'A', name: 'A', role: 2, lat: 54, lon: -1, packets: 9 },
      { nodeId: 'B', name: 'B', role: 2, lat: 54.2, lon: -0.8, packets: 8 },
      { nodeId: 'C', name: 'C', role: 2, lat: 53.8, lon: -1.4, packets: 7 },
    ],
  };
  const tooSmall: RegionSummary = { ...shapeable, name: 'micro', members: [shapeable.members[0]!] };
  const collection = buildRegionAreasGeoJSON([shapeable, tooSmall], 'yorkshire');
  assert.equal(collection.features.length, 1);
  assert.equal(collection.features[0]?.properties?.['region'], 'yorkshire');
  assert.equal(collection.features[0]?.properties?.['selected'], true);
  assert.match(String(collection.features[0]?.properties?.['color']), /^#[0-9a-f]{6}$/);
  const ring = collection.features[0]?.geometry.coordinates[0]!;
  assert.deepEqual(ring[0], ring[ring.length - 1]);
});

test('buildRegionAreasGeoJSON hides other regions when one is selected', () => {
  const base: RegionSummary = {
    name: 'yorkshire', packetsTotal: 100, memberCount: 3, repeaterMemberCount: 3,
    confidence: 'confirmed', firstSeen: null, lastSeen: null,
    members: [
      { nodeId: 'A', name: 'A', role: 2, lat: 54, lon: -1, packets: 9 },
      { nodeId: 'B', name: 'B', role: 2, lat: 54.2, lon: -0.8, packets: 8 },
      { nodeId: 'C', name: 'C', role: 2, lat: 53.8, lon: -1.4, packets: 7 },
    ],
  };
  const northwest: RegionSummary = {
    ...base,
    name: 'northwest',
    members: base.members.map((member) => ({ ...member, lat: member.lat! + 0.5, lon: member.lon! - 1 })),
  };
  assert.equal(buildRegionAreasGeoJSON([base, northwest], null).features.length, 2);
  const selectedOnly = buildRegionAreasGeoJSON([base, northwest], 'northwest');
  assert.equal(selectedOnly.features.length, 1);
  assert.equal(selectedOnly.features[0]?.properties?.['region'], 'northwest');
  assert.equal(selectedOnly.features[0]?.properties?.['selected'], true);
});

test('buildUnattributedGeoJSON writes a closed footprint ring per cluster', () => {
  const collection = buildUnattributedGeoJSON([
    { id: 'u1', lat: 51.45, lon: -2.59, packets: 1455, nodeCount: 12, firstSeen: null, lastSeen: null, topNodes: [] },
    { id: 'u2', lat: 52.63, lon: 1.29, packets: 205, nodeCount: 4, firstSeen: null, lastSeen: null, topNodes: [] },
  ]);
  assert.equal(collection.features.length, 2);
  const [first, second] = collection.features;
  const ring = first!.geometry.coordinates[0]!;
  assert.ok(ring.length > 8);
  assert.deepEqual(ring[0], ring[ring.length - 1]);
  assert.equal(first!.properties['cluster_id'], 'u1');
  assert.equal(first!.properties['label'], 'Unnamed · 1,455 pkts');
  assert.equal(second!.properties['cluster_id'], 'u2');
  // A larger cluster is drawn with a wider footprint than a small one.
  const span = (feature: typeof first): number => {
    const lats = feature!.geometry.coordinates[0]!.map(([, lat]) => lat);
    return Math.max(...lats) - Math.min(...lats);
  };
  assert.ok(span(first) > span(second));
});
