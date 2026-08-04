import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildAerialPathSegments,
  cachedTerrainElevation,
  easeArcProgress,
  interpolateArcPosition,
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
  assert.equal(registry.get(trunkKey!)?.segment.confidence, 0.9, 'shared edges retain strongest evidence');
});

test('a branch arriving mid-trunk waits at the split without replaying stable segments', () => {
  const registry = new Map<string, PathRegistryEntry>();
  const initial: AerialPath = {
    id: 'packet-a',
    confidence: 0.6,
    nodes: [
      { position: [-2, 51] },
      { position: [-1, 52] },
      { position: [0, 53] },
    ],
  };
  const diverted: AerialPath = {
    id: 'packet-a',
    confidence: 0.9,
    nodes: [
      { position: [-2, 51] },
      { position: [-1, 52] },
      { position: [1, 53] },
    ],
  };
  const [trunkKey, originalBranchKey] = buildAerialPathSegments([initial]).map((segment) => segment.id);
  const divertedBranchKey = buildAerialPathSegments([diverted])[1]!.id;

  registerAerialPaths(registry, [initial], 100);
  registerAerialPaths(registry, [initial, diverted], 300);

  assert.equal(registry.get(trunkKey!)?.startedAt, 100, 'active trunk progress is retained');
  assert.equal(registry.get(originalBranchKey!)?.startedAt, 500, 'existing stream timing is retained');
  assert.equal(registry.get(divertedBranchKey)?.startedAt, 500, 'new stream starts when the split is reached');
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

test('hop interpolation samples the same eased ArcLayer paraboloid', () => {
  const source: [number, number, number] = [-1, 51, 100];
  const target: [number, number, number] = [0, 52, 200];
  const midpoint = interpolateArcPosition(source, target, easeArcProgress(0.5));

  assert.deepEqual(interpolateArcPosition(source, target, 0), source);
  assert.deepEqual(interpolateArcPosition(source, target, 1), target);
  assert(Math.abs(midpoint[0] - -0.5) < 1e-12);
  assert(midpoint[1] > 51.5, 'the marker follows Web Mercator arc projection, not linear latitude');
  assert(midpoint[2] > target[2], 'the ArcLayer height profile clears both endpoints');
});

test('arc progress eases smoothly while preserving segment endpoints', () => {
  assert.equal(easeArcProgress(-1), 0);
  assert.equal(easeArcProgress(0), 0);
  assert.equal(easeArcProgress(0.25), 0.15625);
  assert.equal(easeArcProgress(0.5), 0.5);
  assert.equal(easeArcProgress(0.75), 0.84375);
  assert.equal(easeArcProgress(1), 1);
  assert.equal(easeArcProgress(2), 1);
});
