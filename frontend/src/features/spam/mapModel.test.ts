import assert from 'node:assert/strict';
import test from 'node:test';
import { buildZonesGeoJSON, groupSpamOrigins, type SpamMapIncident } from './mapModel.js';

function incident(id: string, overrides: Partial<SpamMapIncident> = {}): SpamMapIncident {
  return {
    id, network: 'ukmesh', status: 'closed',
    origin: { confidence: 0.8, zone: { lat: 51.5, lon: -2.5, radiusKm: 15 } },
    ...overrides,
  };
}

test('47 incidents at the same estimated source produce one circle and count label', () => {
  const incidents = Array.from({ length: 47 }, (_, index) => incident(String(index)));
  const groups = groupSpamOrigins(incidents);
  assert.equal(groups.length, 1);
  assert.equal(groups[0].incidentIds.length, 47);
  const features = buildZonesGeoJSON(groups).features;
  assert.equal(features.filter((feature) => feature.geometry.type === 'Polygon').length, 1);
  assert.equal(features.filter((feature) => feature.geometry.type === 'Point').length, 1);
  assert.equal(features[1].properties?.incidentCount, 47);
});

test('neighbouring coarse cells group while preserving both full uncertainty areas', () => {
  const incidents = [incident('a'), incident('b', {
    origin: { confidence: 0.6, zone: { lat: 51.6, lon: -2.5, radiusKm: 30 } },
  })];
  const before = JSON.stringify(incidents);
  const [group] = groupSpamOrigins(incidents);
  assert.equal(group.incidentIds.length, 2);
  assert.equal(group.zone.lat, 51.5);
  assert.equal(group.zone.lon, -2.5);
  assert.equal(group.zone.radiusKm, 42);
  assert.equal(JSON.stringify(incidents), before, 'incident detail estimates stay intact');
});

test('a broad uncertainty circle cannot merge distinct source centres', () => {
  const groups = groupSpamOrigins([
    incident('a', { origin: { confidence: 0.5, zone: { lat: 51.5, lon: -2.5, radiusKm: 125 } } }),
    incident('b', { origin: { confidence: 0.5, zone: { lat: 51.8, lon: -2.5, radiusKm: 125 } } }),
  ]);
  assert.equal(groups.length, 2);
});

test('a chain of nearby incidents does not bridge separate source areas', () => {
  const groups = groupSpamOrigins([51.5, 51.6, 51.7].map((lat, index) => incident(String(index), {
    origin: { confidence: 0.8, zone: { lat, lon: -2.5, radiusKm: 15 } },
  })));
  assert.equal(groups.length, 2);
  assert.deepEqual(groups.map((group) => group.incidentIds.length), [2, 1]);
});

test('nearby estimates with small disjoint uncertainty areas remain separate', () => {
  const groups = groupSpamOrigins([
    incident('a', { origin: { confidence: 0.8, zone: { lat: 51.5, lon: -2.5, radiusKm: 2 } } }),
    incident('b', { origin: { confidence: 0.8, zone: { lat: 51.6, lon: -2.5, radiusKm: 2 } } }),
  ]);
  assert.equal(groups.length, 2);
});

test('incidents from different networks never share a source group', () => {
  assert.equal(groupSpamOrigins([incident('a'), incident('b', { network: 'test' })]).length, 2);
});

test('a group stays active when any member is ongoing, including a weaker estimate', () => {
  const groups = groupSpamOrigins([incident('a'), incident('b', {
    status: 'active', origin: { confidence: 0.5, zone: { lat: 51.5, lon: -2.5, radiusKm: 20 } },
  })]);
  assert.equal(groups[0].active, true);
  assert.equal(buildZonesGeoJSON(groups).features[0].properties?.active, true);
  assert.equal(groupSpamOrigins([incident('a')])[0].active, false);
});

test('groups and their anchor are stable across input order and status changes', () => {
  const incidents = [incident('a'), incident('b'), incident('c', {
    origin: { confidence: 0.9, zone: { lat: 51.6, lon: -2.5, radiusKm: 20 } },
  })];
  const expected = groupSpamOrigins(incidents);
  assert.deepEqual(groupSpamOrigins([...incidents].reverse()), expected);
  assert.equal(expected[0].id, 'c');
  incidents[0].status = 'active';
  assert.deepEqual(groupSpamOrigins(incidents)[0].zone, expected[0].zone);
});

test('missing or invalid origin estimates do not create map geometry', () => {
  const groups = groupSpamOrigins([
    incident('none', { origin: { confidence: 0, zone: null } }),
    ...[NaN, Infinity, 91].map((lat, index) => incident(String(index), {
      origin: { confidence: 0.8, zone: { lat, lon: -2.5, radiusKm: 15 } },
    })),
    incident('negative', { origin: { confidence: 0.8, zone: { lat: 51.5, lon: -2.5, radiusKm: -1 } } }),
  ]);
  assert.deepEqual(groups, []);
  assert.deepEqual(buildZonesGeoJSON(groups).features, []);
});
