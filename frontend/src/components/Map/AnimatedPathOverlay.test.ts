import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildAerialPathSegments,
  cachedTerrainElevation,
  hopPeakElevation,
  interpolateBallistic,
  registerAerialPaths,
  terrainAwarePosition,
  type AerialPath,
  type PathRegistryEntry,
} from './AnimatedPathOverlay.js';

function path(id: string, offset = 0): AerialPath {
  return {
    id,
    confidence: 1,
    nodes: [
      { position: [-1 + offset, 51] },
      { position: [0 + offset, 52] },
    ],
  };
}

test('new packet paths coexist without resetting earlier path lifetimes', () => {
  const registry = new Map<string, PathRegistryEntry>();
  const packetA = path('packet-a');
  const packetB = path('packet-b', 1);
  const packetAKey = buildAerialPathSegments([packetA])[0]!.id;
  const packetBKey = buildAerialPathSegments([packetB])[0]!.id;
  registerAerialPaths(registry, [packetA], 100);
  registerAerialPaths(registry, [packetB], 500);

  assert.equal(registry.size, 2);
  assert.equal(registry.get(packetAKey)?.startedAt, 100);
  assert.equal(registry.get(packetBKey)?.startedAt, 500);

  registerAerialPaths(registry, [packetB], 900);
  assert.equal(registry.get(packetAKey)?.startedAt, 100);
  assert.equal(registry.get(packetBKey)?.startedAt, 500, 'unchanged paths do not restart their TTL');
});

test('a later observer reuses the common trunk and registers only its new branch', () => {
  const registry = new Map<string, PathRegistryEntry>();
  const firstObserver: AerialPath = {
    id: 'packet-a',
    confidence: 0.6,
    nodes: [
      { position: [-2, 51] },
      { position: [-1, 52] },
      { position: [0, 53] },
    ],
  };
  const secondObserver: AerialPath = {
    id: 'packet-a',
    confidence: 0.9,
    nodes: [
      { position: [-2, 51] },
      { position: [-1, 52] },
      { position: [1, 53] },
    ],
  };
  const [trunkKey, firstBranchKey] = buildAerialPathSegments([firstObserver]).map((segment) => segment.id);
  const secondBranchKey = buildAerialPathSegments([secondObserver])[1]!.id;

  registerAerialPaths(registry, [firstObserver], 100);
  const firstBranchStartedAt = registry.get(firstBranchKey!)?.startedAt;
  registerAerialPaths(registry, [secondObserver], 500);

  assert.equal(registry.size, 3);
  assert.equal(registry.get(trunkKey!)?.startedAt, 100, 'shared trunk keeps its original lifetime');
  assert.equal(firstBranchStartedAt, 500, 'the original path still animates hop by hop');
  assert.equal(registry.get(firstBranchKey!)?.startedAt, firstBranchStartedAt);
  assert.equal(registry.get(secondBranchKey)?.startedAt, 500, 'only the new branch starts later');
  assert.equal(registry.get(trunkKey!)?.segments[0]?.confidence, 0.9, 'shared edges retain strongest evidence');
});

test('terrain elevation queries are cached per coordinate and preserve null fallbacks', () => {
  const cache = new Map<string, number | null>();
  let queryCount = 0;
  const map = {
    queryTerrainElevation: (position: [number, number]) => {
      queryCount += 1;
      return position[0] === 1 ? 120 : null;
    },
  } as Parameters<typeof cachedTerrainElevation>[0];

  assert.equal(cachedTerrainElevation(map, [1, 2], cache, true), 120);
  assert.equal(cachedTerrainElevation(map, [1.0000004, 2], cache, true), 120);
  assert.equal(cachedTerrainElevation(map, [3, 4], cache, true), null);
  assert.equal(cachedTerrainElevation(map, [3, 4], cache, true), null);
  assert.equal(queryCount, 2);
  assert.equal(cachedTerrainElevation(map, [1, 2], cache, false), null);
  assert.equal(queryCount, 2, 'disabled terrain does not query or use stale elevations');
});

test('terrain-aware endpoint positions apply clearance and fall back to ground level', () => {
  assert.deepEqual(terrainAwarePosition([1, 2], 120, true, 2), [1, 2, 304]);
  assert.deepEqual(terrainAwarePosition([1, 2], null, true, 2), [1, 2, 0]);
  assert.deepEqual(terrainAwarePosition([1, 2], 120, false, 2), [1, 2, 0]);
});

test('hop interpolation is horizontal-linear but vertically ballistic', () => {
  const source: [number, number, number] = [-1, 51, 100];
  const target: [number, number, number] = [0, 52, 200];
  const peak = hopPeakElevation(source, target, 1);
  const midpoint = interpolateBallistic(source, target, 0.5, peak);

  assert.deepEqual(interpolateBallistic(source, target, 0, peak), source);
  assert.deepEqual(interpolateBallistic(source, target, 1, peak), target);
  assert.deepEqual(midpoint.slice(0, 2), [-0.5, 51.5]);
  assert.equal(midpoint[2], peak, 'mid-hop reaches the planned peak elevation');
  assert(midpoint[2] > target[2], 'the active marker clears both endpoints');
});

test('long hops scale their airborne peak above the minimum', () => {
  const peak = hopPeakElevation([0, 0, 0], [1, 0, 0]);
  assert(peak > 300);
});
