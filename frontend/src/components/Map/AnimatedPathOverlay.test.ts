import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildAerialPathSegments,
  registerAerialPaths,
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
