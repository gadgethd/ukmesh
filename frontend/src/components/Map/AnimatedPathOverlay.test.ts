import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildAerialPathSegments,
  arcGroundPosition,
  arcHeightMultiplier,
  cachedTerrainElevation,
  easeArcProgress,
  interpolateArcPosition,
  registerAerialPaths,
  renderedPosition,
  terrainAwarePosition,
  type AerialPath,
  type PathRegistryEntry,
  type TerrainElevationMap,
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
    isSourceLoaded: () => true,
    transform: { elevation: 300 },
    queryTerrainElevation: (position: [number, number]) => {
      queryCount += 1;
      return position[0] === 1 ? 120 : null;
    },
  } as Parameters<typeof cachedTerrainElevation>[0];

  assert.equal(cachedTerrainElevation(map, [1, 2], cache, true), 420);
  assert.equal(cachedTerrainElevation(map, [1.0000004, 2], cache, true), 420);
  assert.equal(cachedTerrainElevation(map, [3, 4], cache, true), null);
  assert.equal(cachedTerrainElevation(map, [3, 4], cache, true), null);
  assert.equal(queryCount, 2);
  assert.equal(cachedTerrainElevation(map, [1, 2], cache, false), null);
  assert.equal(queryCount, 2, 'disabled terrain does not query or use stale elevations');
});

test('terrain queries stay unavailable until the DEM source is loaded', () => {
  let queryCount = 0;
  const map = {
    isSourceLoaded: () => false,
    transform: { elevation: 300 },
    queryTerrainElevation: () => {
      queryCount += 1;
      return 0;
    },
  } as Parameters<typeof cachedTerrainElevation>[0];
  const cache = new Map<string, number | null>();

  assert.equal(cachedTerrainElevation(map, [1, 2], cache, true), null);
  assert.equal(queryCount, 0, 'a loading DEM is not converted into a z=0 anchor');
});

test('terrain-aware endpoint positions use absolute exaggerated elevation and clearance', () => {
  assert.deepEqual(terrainAwarePosition([1, 2], 420, true, 2), [1, 2, 484]);
  assert.equal(terrainAwarePosition([1, 2], null, true, 2), null);
  assert.deepEqual(terrainAwarePosition([1, 2], 120, false, 2), [1, 2, 0]);
});

test('arc endpoints and node dots reuse one terrain anchor', () => {
  let queryCount = 0;
  const map = {
    isSourceLoaded: () => true,
    transform: { elevation: 300 },
    queryTerrainElevation: () => {
      queryCount += 1;
      return 120;
    },
  } as TerrainElevationMap;
  const elevations = new Map<string, number | null>();
  const anchors = new Map<string, ReturnType<typeof renderedPosition>>();
  const lineAnchor = renderedPosition(
    { position: [1, 2] },
    map,
    true,
    elevations,
    anchors,
  );
  const dotAnchor = renderedPosition(
    { position: [1.0000004, 2] },
    map,
    true,
    elevations,
    anchors,
  );

  assert.strictEqual(lineAnchor, dotAnchor, 'both layers share the exact coordinate anchor');
  assert.deepEqual(lineAnchor, [1, 2, 516]);
  assert.equal(queryCount, 1, 'the shared anchor uses one terrain query');
});

test('hop interpolation samples the same eased ArcLayer paraboloid', () => {
  const source: [number, number, number] = [-1, 51, 100];
  const target: [number, number, number] = [0, 52, 200];
  const midpoint = interpolateArcPosition(source, target, easeArcProgress(0.5));

  assert.deepEqual(interpolateArcPosition(source, target, 0), source);
  assert.deepEqual(interpolateArcPosition(source, target, 1), target);
  assert(Math.abs(midpoint[0] - -0.5) < 1e-12);
  assert(midpoint[1] > 51.5, 'the marker follows Web Mercator arc projection, not linear latitude');
  assert(midpoint[2] > source[2], 'the ArcLayer height profile clears the lower endpoint');
});

test('terrain crests raise the ArcLayer curve without moving its endpoint anchors', () => {
  const source: [number, number, number] = [-1, 51, 100];
  const target: [number, number, number] = [0, 52, 100];
  const baseHeight = arcHeightMultiplier(source, target);
  const crestPosition = arcGroundPosition(
    [source[0], source[1]],
    [target[0], target[1]],
    0.5,
  );
  const terrainHeight = arcHeightMultiplier(source, target, [{
    progress: 0.5,
    position: [...crestPosition, 1_000],
  }]);
  const midpoint = interpolateArcPosition(source, target, 0.5, terrainHeight);

  assert(terrainHeight > baseHeight, 'a high DEM sample increases only the arc lift');
  assert(midpoint[2] >= 1_000 - 1e-9, 'the curve stays above the sampled terrain crest');
  assert.deepEqual(interpolateArcPosition(source, target, 0, terrainHeight), source);
  assert.deepEqual(interpolateArcPosition(source, target, 1, terrainHeight), target);
});

test('the baseline ArcLayer lift is a consistent physical height across hop lengths', () => {
  const shortSource: [number, number, number] = [0, 51, 100];
  const shortTarget: [number, number, number] = [0.01, 51, 100];
  const longSource: [number, number, number] = [0, 51, 100];
  const longTarget: [number, number, number] = [0.1, 51, 100];
  const shortMidpoint = interpolateArcPosition(
    shortSource,
    shortTarget,
    0.5,
    arcHeightMultiplier(shortSource, shortTarget),
  );
  const longMidpoint = interpolateArcPosition(
    longSource,
    longTarget,
    0.5,
    arcHeightMultiplier(longSource, longTarget),
  );

  assert(Math.abs(shortMidpoint[2] - longMidpoint[2]) < 1, 'hop length does not change the physical lift');
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
