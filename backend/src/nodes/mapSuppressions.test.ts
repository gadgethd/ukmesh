import assert from 'node:assert/strict';
import test from 'node:test';
import {
  computeNodeMapSuppressions,
  mapSuppressionConfig,
  type MapSuppressionNode,
} from './mapSuppressions.js';

const NOW = Date.parse('2026-10-27T00:00:00Z');
const DAY = 86_400_000;
function node(node_id: string, age: number, overrides: Partial<MapSuppressionNode> = {}) {
  return {
    node_id, name: 'Same repeater', role: 2, network: 'ukmesh', lat: 54, lon: -1,
    last_seen: new Date(NOW - age * DAY), ...overrides,
  };
}

test('same-name same-coordinate stale node is suppressed with evidence', () => {
  assert.deepEqual(computeNodeMapSuppressions([node('S', 20), node('F', 1)], NOW), [{
    node_id: 'S', suppressed_by_node_id: 'F', reason: 'stale-colocated',
    distance_m: 0, stale_age_days: 20, freshest_age_days: 1,
  }]);
});

test('nodes 49 metres apart are not suppressed', () => {
  const offset = 49 / 6_371_000 * 180 / Math.PI;
  assert.deepEqual(computeNodeMapSuppressions([
    node('S', 20, { lat: 0, lon: 10 }), node('F', 1, { lat: offset, lon: 10 }),
  ], NOW), []);
});

test('two fresh co-located nodes remain visible', () => {
  assert.deepEqual(computeNodeMapSuppressions([node('A', 1), node('B', 6)], NOW), []);
});

for (const difference of [3, 4]) {
  test(`two stale nodes ${difference} days apart remain visible`, () => {
    assert.deepEqual(computeNodeMapSuppressions([node('A', 20), node('B', 20 - difference)], NOW), []);
  });
}

test('co-located nodes in different networks do not suppress each other', () => {
  assert.deepEqual(computeNodeMapSuppressions([
    node('S', 20), node('F', 1, { network: 'teesside' }),
  ], NOW), []);
});

test('30.0 metre radius boundary is inclusive and points just beyond it remain visible', () => {
  const atDistance = (distance: number) => node('F', 7, {
    lat: distance / 6_371_000 * 180 / Math.PI, lon: 10,
  });
  const stale = node('S', 14, { lat: 0, lon: 10 });
  const result = computeNodeMapSuppressions([stale, atDistance(30)], NOW);
  assert.equal(result.length, 1);
  assert.ok(Math.abs(result[0]!.distance_m - 30) < 1e-9);
  assert.deepEqual(computeNodeMapSuppressions([stale, atDistance(30.001)], NOW), []);
});

test('14-day stale and 7-day active age boundaries are inclusive', () => {
  assert.equal(computeNodeMapSuppressions([node('S', 14), node('F', 7)], NOW).length, 1);
  assert.deepEqual(computeNodeMapSuppressions([node('S', 14 - 1 / DAY), node('F', 7)], NOW), []);
  assert.deepEqual(computeNodeMapSuppressions([node('S', 14), node('F', 7 + 1 / DAY)], NOW), []);
});

test('radius boundary remains inclusive at UK latitudes', () => {
  const offset = 30 / 6_371_000 * 180 / Math.PI;
  assert.equal(computeNodeMapSuppressions([
    node('S', 20), node('F', 1, { lat: 54 + offset }),
  ], NOW).length, 1);
});

test('names and map-visible roles do not restrict co-location suppression', () => {
  assert.equal(computeNodeMapSuppressions([
    node('S', 20), { ...node('F', 1), name: 'Different name', role: 0 },
  ], NOW).length, 1);
});

test('freshest neighbour wins without transitive clustering or input mutation', () => {
  const metre = 180 / Math.PI / 6_371_000;
  const nodes = [node('S', 20, { lat: 0, lon: 10 }),
    node('near', 3, { lat: 1 * metre, lon: 10 }),
    node('freshest', 1, { lat: 29 * metre, lon: 10 }),
    node('outside', 0, { lat: 40 * metre, lon: 10 }),
    node('no-transitive', 20, { lat: -29 * metre, lon: 10 })];
  const before = structuredClone(nodes);
  const result = computeNodeMapSuppressions(nodes, NOW);
  assert.equal(result.find((row) => row.node_id === 'S')?.suppressed_by_node_id, 'freshest');
  assert.equal(result.find((row) => row.node_id === 'no-transitive')?.suppressed_by_node_id, 'near');
  assert.deepEqual(nodes, before);
  assert.deepEqual(computeNodeMapSuppressions([...nodes].reverse(), NOW).sort((a, b) => a.node_id.localeCompare(b.node_id)),
    result.sort((a, b) => a.node_id.localeCompare(b.node_id)));
});

test('a stale neighbour cannot carry suppression beyond the fresh node radius', () => {
  const metre = 180 / Math.PI / 6_371_000;
  const result = computeNodeMapSuppressions([
    node('F', 1, { lat: 0, lon: 10 }),
    node('S', 20, { lat: 20 * metre, lon: 10 }),
    node('T', 20, { lat: 40 * metre, lon: 10 }),
  ], NOW);
  assert.deepEqual(result.map((row) => row.node_id), ['S']);
});

test('invalid positions, missing timestamps, future activity, and unknown networks are ignored', () => {
  for (const invalid of [{ lat: null }, { lat: NaN }, { lon: 181 }, { lat: 0, lon: 0 },
    { last_seen: null }, { last_seen: 'invalid' }, { network: null }]) {
    assert.deepEqual(computeNodeMapSuppressions([node('S', 20), node('F', 1, invalid)], NOW), []);
  }
  assert.deepEqual(computeNodeMapSuppressions([node('S', 20), node('F', -1)], NOW), []);
});

test('thresholds are tunable and invalid configuration is rejected', () => {
  assert.deepEqual(mapSuppressionConfig({}), { radiusM: 30, staleAfterDays: 14, activeWithinDays: 7 });
  const config = mapSuppressionConfig({ MAP_STALE_DUPLICATE_RADIUS_M: '10',
    MAP_STALE_DUPLICATE_STALE_AFTER_DAYS: '20', MAP_STALE_DUPLICATE_ACTIVE_WITHIN_DAYS: '3' });
  assert.equal(computeNodeMapSuppressions([node('S', 20), node('F', 3)], NOW, config).length, 1);
  assert.deepEqual(computeNodeMapSuppressions([node('S', 19), node('F', 3)], NOW, config), []);
  assert.throws(() => mapSuppressionConfig({ MAP_STALE_DUPLICATE_RADIUS_M: 'NaN' }), /Invalid/);
  assert.throws(() => mapSuppressionConfig({ MAP_STALE_DUPLICATE_STALE_AFTER_DAYS: '7' }), /Invalid/);
});
