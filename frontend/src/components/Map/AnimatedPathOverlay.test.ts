import assert from 'node:assert/strict';
import test from 'node:test';
import {
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
  registerAerialPaths(registry, [path('packet-a')], 100);
  registerAerialPaths(registry, [path('packet-b', 1)], 500);

  assert.equal(registry.size, 2);
  assert.equal(registry.get('packet-a')?.startedAt, 100);
  assert.equal(registry.get('packet-b')?.startedAt, 500);

  registerAerialPaths(registry, [path('packet-b', 1)], 900);
  assert.equal(registry.get('packet-a')?.startedAt, 100);
  assert.equal(registry.get('packet-b')?.startedAt, 500, 'unchanged paths do not restart their TTL');
});
