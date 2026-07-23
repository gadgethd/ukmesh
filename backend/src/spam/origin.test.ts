import { test } from 'node:test';
import assert from 'node:assert/strict';
import { estimateOrigin } from './origin.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';
import type { ObserverObservation } from './types.js';

function obs(
  id: string,
  lat: number,
  lon: number,
  hopCount: number,
  snr = 5,
  rssi = -80,
): ObserverObservation {
  return { observerId: id, lat, lon, hopCount, snr, rssi };
}

test('no observers yields insufficient data', () => {
  const e = estimateOrigin([], CFG);
  assert.equal(e.level, 'insufficient');
  assert.equal(e.lat, null);
  assert.equal(e.radiusKm, null);
});

test('a single observer is not enough to triangulate', () => {
  const e = estimateOrigin([obs('a', 53.8, -1.55, 0)], CFG);
  assert.equal(e.level, 'insufficient');
});

test('several observers produce a coarse estimate snapped to a region', () => {
  const e = estimateOrigin(
    [
      obs('a', 53.80, -1.55, 1),
      obs('b', 53.83, -1.50, 2),
      obs('c', 53.78, -1.60, 1),
    ],
    CFG,
  );
  assert.notEqual(e.level, 'insufficient');
  assert.ok(e.lat != null && Math.abs(e.lat - 53.8) < 0.3);
  assert.ok(e.radiusKm != null && e.radiusKm >= CFG.originMinRadiusKm);
  assert.equal(e.radiusKm! % 5, 0, 'radius is coarsened to 5km buckets');
  assert.equal(e.region, 'West Yorkshire');
});

test('strong low-hop observer pulls the centroid toward itself', () => {
  // A: strong, 0 hops at lat 54.0; B: weak, far hops at lat 53.0.
  const e = estimateOrigin(
    [
      obs('a', 54.0, -1.5, 0, 11, -50),
      obs('b', 53.0, -1.5, 9, -8, -115),
    ],
    CFG,
  );
  assert.ok(e.lat != null && e.lat > 53.5, `expected pull toward strong observer, got ${e.lat}`);
});

test('more diverse observers raise confidence', () => {
  const few = estimateOrigin([obs('a', 53.8, -1.55, 3), obs('b', 53.9, -1.5, 4)], CFG);
  const many = estimateOrigin(
    [
      obs('a', 53.80, -1.55, 1),
      obs('b', 53.82, -1.52, 1),
      obs('c', 53.78, -1.58, 2),
      obs('d', 53.81, -1.5, 1),
      obs('e', 53.79, -1.6, 2),
      obs('f', 53.83, -1.53, 1),
    ],
    CFG,
  );
  assert.ok(many.confidence > few.confidence);
  assert.ok(many.observerCount === 6);
});

test('a low-hop receiver anchors the origin away from distant relays', () => {
  // Mirrors the real "!test" flood: one observer heard it at 2 hops on the south
  // coast; everything else only relayed it many hops away up north. The estimate
  // must sit near the near-source observer (south), not the relay-cloud middle.
  const e = estimateOrigin(
    [
      obs('south', 50.76, -1.55, 2), // near source
      obs('bristol', 51.5, -2.55, 4),
      obs('yorks', 53.55, -1.44, 9),
      obs('tees1', 54.54, -1.42, 11),
      obs('tees2', 54.59, -1.39, 11),
      obs('tyne', 54.95, -1.64, 14),
    ],
    CFG,
  );
  assert.ok(e.lat != null && Math.abs(e.lat - 50.76) < 0.5, `expected southern anchor, got ${e.lat}`);
  assert.notEqual(e.region, 'South Yorkshire');
  assert.equal(e.level, 'high', `low-hop evidence should read high, got ${e.confidence}`);
  assert.ok(e.reasons.some((r) => /within \d+ hops of the source/.test(r)));
});

test('a flood heard only via distant relays stays humble (not high confidence)', () => {
  const e = estimateOrigin(
    [
      obs('a', 53.8, -1.55, 6),
      obs('b', 53.9, -1.5, 7),
      obs('c', 53.7, -1.6, 8),
      obs('d', 53.85, -1.52, 9),
    ],
    CFG,
  );
  assert.notEqual(e.level, 'high');
  assert.ok(e.reasons.some((r) => /broad area|several hops/.test(r)));
});

// Generality across the UK: a low-hop receiver near the source should anchor the
// estimate to that area and snap to the right region — north, south, Wales or
// Scotland alike (i.e. not overfit to the one south-coast flood we tuned on).
for (const place of [
  { name: 'Greater Glasgow', lat: 55.86, lon: -4.25 },
  { name: 'South Wales', lat: 51.55, lon: -3.3 },
  { name: 'Greater Manchester', lat: 53.48, lon: -2.24 },
  { name: 'Greater London', lat: 51.5, lon: -0.12 },
]) {
  test(`a near-source receiver in ${place.name} anchors and snaps there`, () => {
    const e = estimateOrigin(
      [
        obs('near', place.lat, place.lon, 1), // direct-ish reception at the source
        obs('relay1', 52.9, -1.2, 9), // distant relays elsewhere in the UK
        obs('relay2', 53.8, -1.5, 11),
        obs('relay3', 54.9, -1.6, 13),
      ],
      CFG,
    );
    assert.ok(e.lat != null && Math.abs(e.lat - place.lat) < 0.6, `lat anchored near source, got ${e.lat}`);
    assert.equal(e.region, place.name);
    assert.equal(e.level, 'high', `expected high confidence, got ${e.confidence}`);
  });
}

test('sparse coverage: even a several-hops-out closest receiver anchors (not the cloud middle)', () => {
  // Source up in the Highlands; nearest reception is 4 hops; relays are far south.
  const e = estimateOrigin(
    [
      obs('highland', 57.3, -4.5, 4),
      obs('central', 55.0, -3.6, 9),
      obs('england', 53.5, -1.4, 12),
    ],
    CFG,
  );
  // Cloud middle would be ~55.3; anchoring to the closest receiver keeps it north.
  assert.ok(e.lat != null && e.lat > 56.5, `expected northern anchor, got ${e.lat}`);
  assert.match(e.region, /Highland|Scotland/);
  assert.notEqual(e.level, 'insufficient');
  assert.ok(e.radiusKm != null && e.radiusKm >= 50, 'sparse evidence -> honestly broad radius');
});

test('duplicate observer ids collapse to their best reception', () => {
  const e = estimateOrigin(
    [obs('a', 53.8, -1.55, 5, -5, -110), obs('a', 53.8, -1.55, 1, 10, -55), obs('b', 53.9, -1.5, 2)],
    CFG,
  );
  assert.equal(e.observerCount, 2);
});

test('null-island coordinates are ignored', () => {
  const e = estimateOrigin([obs('a', 0, 0, 1), obs('b', 0, 0, 1)], CFG);
  assert.equal(e.level, 'insufficient');
});
