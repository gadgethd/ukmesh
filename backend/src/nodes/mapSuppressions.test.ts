import assert from 'node:assert/strict';
import test from 'node:test';
import { haversineKm } from '../platform/geo.js';
import {
  computeNodeMapSuppressions,
  mapSuppressionConfig,
  type MapSuppressionNode,
  type MapSuppressionConfig,
  type NodeMapSuppression,
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

// Independent pre-index loop, retained as the semantic oracle rather than a
// second latitude implementation. Its default policy is the historical value.
function originalLoopOracle(
  nodes: readonly MapSuppressionNode[],
  referenceMs: number,
  config: Readonly<MapSuppressionConfig> = { radiusM: 30, staleAfterDays: 14, activeWithinDays: 7 },
): NodeMapSuppression[] {
  if (Object.values(config).some(value => !Number.isFinite(value) || value < 0)
    || config.staleAfterDays <= config.activeWithinDays) throw new Error('Invalid oracle configuration');
  if (!Number.isFinite(referenceMs)) throw new Error('Invalid map suppression reference time');
  const positioned = nodes.flatMap((node) => {
    const seen = node.last_seen instanceof Date
      ? node.last_seen.getTime()
      : node.last_seen == null ? NaN : Date.parse(node.last_seen);
    if (!node.network || node.lat == null || node.lon == null
        || !Number.isFinite(node.lat) || !Number.isFinite(node.lon)
        || Math.abs(node.lat) > 90 || Math.abs(node.lon) > 180
        || (Math.abs(node.lat) < 5 && Math.abs(node.lon) < 5)
        || !Number.isFinite(seen) || seen > referenceMs) return [];
    return [{ node, age: (referenceMs - seen) / DAY }];
  });
  const fresh = positioned.filter(({ age }) => age <= config.activeWithinDays)
    .sort((a, b) => a.age - b.age || a.node.node_id.localeCompare(b.node.node_id));
  const suppressions: NodeMapSuppression[] = [];
  for (const stale of positioned) {
    if (stale.age < config.staleAfterDays) continue;
    for (const active of fresh) {
      if (stale.node.node_id === active.node.node_id
          || stale.node.network !== active.node.network) continue;
      const distance = haversineKm(stale.node.lat!, stale.node.lon!, active.node.lat!, active.node.lon!) * 1_000;
      // Coordinate subtraction can put an exact 30 m boundary a few
      // nanometres above it. Allow only floating-point noise at that edge.
      if (distance - config.radiusM > 1e-8) continue;
      suppressions.push({
        node_id: stale.node.node_id,
        suppressed_by_node_id: active.node.node_id,
        reason: 'stale-colocated',
        distance_m: distance,
        stale_age_days: stale.age,
        freshest_age_days: active.age,
      });
      break;
    }
  }
  return suppressions;
}

function assertOriginalParity(rows: MapSuppressionNode[], config: MapSuppressionConfig = {
  radiusM: 30, staleAfterDays: 14, activeWithinDays: 7,
}) {
  const before = structuredClone(rows);
  const expected = originalLoopOracle(rows, NOW, config);
  const frozen = Object.freeze(rows.map(row => Object.freeze({ ...row })));
  assert.deepEqual(computeNodeMapSuppressions(frozen, NOW, config), expected);
  assert.deepEqual(rows, before);
}

test('latitude lookup keeps freshest rank, stable duplicate-ID ties and stale input order', () => {
  const metre = 180 / Math.PI / 6_371_000;
  const rows = [
    node('stale-second', 25, { lat: 0, lon: 10 }),
    node('F', 1, { lat: 20 * metre, lon: 10 }),
    node('F', 1, { lat: metre, lon: 10 }),
    node('Z-nearest', 1, { lat: 0, lon: 10 }),
    node('A-freshest-tie', 1, { lat: 29 * metre, lon: 10 }),
    node('stale-first', 20, { lat: 0, lon: 10 }),
    node('stale-first', 0, { lat: 0, lon: 10 }),
  ];
  for (const input of [rows, [...rows].reverse(), [...rows.slice(3), ...rows.slice(0, 3)]]) {
    assertOriginalParity(input);
  }
  const duplicates = [node('S', 20, { lat: 0, lon: 10 }),
    node('F', 1, { lat: 20 * metre, lon: 10 }), node('F', 1, { lat: metre, lon: 10 })];
  const output = computeNodeMapSuppressions(duplicates, NOW);
  assert.ok(output[0]!.distance_m > 19, 'stable original rank wins even when a duplicate ID is nearer');
  assertOriginalParity(duplicates);
});

test('latitude pruning retains exact acceptance at meridian, polar and antimeridian boundaries', () => {
  const anchors = [[0, 10], [54, -1], [89.999, 179.999], [-89.999, -179.999], [90, 0], [-90, 180]];
  for (const [lat, lon] of anchors) {
    for (const delta of [-0.001, -1e-8, 0, 1e-8, 0.001]) {
      const otherLat = Math.max(-90, Math.min(90, lat! + delta));
      for (const otherLon of [lon!, -lon!, lon! < 0 ? 180 : -180]) {
        const distance = haversineKm(lat!, lon!, otherLat, otherLon) * 1_000;
        for (const adjustment of [-2e-8, -1e-8, 0, 1e-8, 2e-8]) {
          assertOriginalParity([node('S', 14, { lat: lat!, lon: lon! }),
            node('F', 7, { lat: otherLat, lon: otherLon })], {
            radiusM: Math.max(0, distance + adjustment), staleAfterDays: 14, activeWithinDays: 7,
          });
        }
      }
    }
  }
});

test('custom radii from zero through whole-globe preserve the complete original predicate', () => {
  const rows = [
    node('S', 30, { lat: 45, lon: 180 }), node('F1', 0, { lat: 45, lon: -180 }),
    node('F2', 1, { lat: -45, lon: 0 }), node('S2', 40, { lat: -89, lon: -170 }),
    node('F3', 2, { lat: 89, lon: 170 }), node('F-other-network', 0, { network: 'custom', lat: 45, lon: 180 }),
    node('S-custom', 30, { network: 'custom', lat: 45, lon: 180 }),
  ];
  for (const radiusM of [0, 1e-12, 1, 30, 10_000, 1_000_000, 20_015_100, Number.MAX_VALUE]) {
    for (const thresholds of [{ staleAfterDays: 14, activeWithinDays: 7 },
      { staleAfterDays: 0.5, activeWithinDays: 0 }, { staleAfterDays: 40, activeWithinDays: 2 }]) {
      assertOriginalParity(rows, { radiusM, ...thresholds });
    }
  }
});

test('independent oracle agrees across seeded clusters, invalid inputs, networks and permutations', () => {
  let seed = 0x930ef1;
  const random = () => { seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0; return seed / 0x1_0000_0000; };
  for (let sample = 0; sample < 40; sample++) {
    const anchorLat = random() * 160 - 80, anchorLon = random() * 360 - 180;
    const rows: MapSuppressionNode[] = [];
    for (let index = 0; index < 100; index++) {
      const clustered = index % 3 !== 0;
      const lat = clustered ? anchorLat + (random() - 0.5) * 0.002 : random() * 180 - 90;
      const lon = clustered ? Math.max(-180, Math.min(180, anchorLon + (random() - 0.5) * 0.002)) : random() * 360 - 180;
      rows.push(node(index % 19 === 0 ? 'duplicate' : 'N' + index, index % 5 === 0 ? 20 : random() * 40, {
        network: index % 7 === 0 ? 'other' : 'ukmesh', lat, lon,
      }));
    }
    rows.push(...[{ lat: null }, { lat: NaN }, { lon: Infinity }, { lat: 91 }, { lon: -181 },
      { lat: 0, lon: 0 }, { last_seen: null }, { last_seen: 'invalid' }, { network: null },
      { network: '' }].map((override, index) => node('invalid' + index, 20, override)));
    rows.push(node('future', -1));
    for (const radiusM of [0, 30, 300, 10_000_000]) {
      const config = { radiusM, staleAfterDays: 14, activeWithinDays: 7 };
      assertOriginalParity(rows, config);
      assertOriginalParity([...rows].reverse(), config);
    }
  }
});

test('latitude index preserves reference-time and configuration validation', () => {
  for (const reference of [NaN, Infinity, -Infinity]) {
    assert.throws(() => computeNodeMapSuppressions([], reference), /Invalid map suppression reference time/);
  }
  for (const config of [
    { radiusM: -1, staleAfterDays: 14, activeWithinDays: 7 },
    { radiusM: Infinity, staleAfterDays: 14, activeWithinDays: 7 },
    { radiusM: 30, staleAfterDays: 7, activeWithinDays: 7 },
    { radiusM: 30, staleAfterDays: 14, activeWithinDays: -1 },
  ]) assert.throws(() => computeNodeMapSuppressions([], NOW, config), /Invalid map suppression thresholds/);
});
