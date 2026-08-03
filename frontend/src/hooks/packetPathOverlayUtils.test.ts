import assert from 'node:assert/strict';
import test from 'node:test';
import {
  aggregateCanonicalPath,
  canonicalPathCoordinates,
  canonicalPathRuns,
  type CanonicalPathNode,
  type MultiObserverBetaResponse,
} from './packetPathOverlayUtils.js';

function node(position: number, lat: number | null, lon: number | null): CanonicalPathNode {
  return {
    position,
    hash: `hash-${position}`,
    nodeId: lat == null || lon == null ? null : `node-${position}`,
    name: null,
    lat,
    lon,
    ambiguous: false,
    confidence: lat == null || lon == null ? null : 0.8,
  };
}

test('canonical path coordinates do not bridge unresolved hops', () => {
  const canonicalPath = [node(0, 51, -1), node(1, null, null), node(2, 52, -2)];

  assert.deepEqual(canonicalPathRuns(canonicalPath), []);
  assert.deepEqual(canonicalPathCoordinates(canonicalPath), []);
});

test('canonical path aggregation renders one route and exposes observer markers', () => {
  const response: MultiObserverBetaResponse = {
    packetHash: 'ABC',
    network: 'uk',
    canonicalPath: [node(0, 51, -1), node(1, 52, -2)],
    observers: [{ observerId: 'rx-a' }, { observerId: 'rx-b' }, { observerId: 'rx-a' }],
    confidence: 0.9,
  };

  assert.deepEqual(aggregateCanonicalPath(response), {
    canonicalPath: response.canonicalPath,
    observerIds: ['rx-a', 'rx-b'],
    confidence: 0.9,
  });
});
