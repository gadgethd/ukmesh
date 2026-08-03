import assert from 'node:assert/strict';
import test from 'node:test';
import {
  aggregateCanonicalPath,
  canonicalPathCoordinates,
  canonicalPathRuns,
  multiObserverPathRoutes,
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
    routes: [{
      confidence: 0.9,
      nodes: [
        { lat: 51, lon: -1, nodeId: 'node-0', name: null, confidence: 0.8 },
        { lat: 52, lon: -2, nodeId: 'node-1', name: null, confidence: 0.8 },
      ],
    }],
    observerIds: ['rx-a', 'rx-b'],
    confidence: 0.9,
  });
});

test('multi-observer projections retain every branch and de-duplicate their common route', () => {
  const canonicalPath = [node(0, 51, -1), node(1, 52, -2)];
  const response: MultiObserverBetaResponse = {
    packetHash: 'ABC',
    network: 'uk',
    canonicalPath,
    observers: [{ observerId: 'rx-a' }, { observerId: 'rx-b' }],
    confidence: 0.8,
    results: [
      {
        ok: true,
        packetHash: 'ABC',
        network: 'uk',
        mode: 'resolved',
        canonicalPath,
        observers: [{ observerId: 'rx-a' }],
        confidence: 0.8,
        purplePath: [[51, -1], [52, -2], [53, -3]],
        extraPurplePaths: [],
      },
      {
        ok: true,
        packetHash: 'ABC',
        network: 'uk',
        mode: 'resolved',
        canonicalPath,
        observers: [{ observerId: 'rx-b' }],
        confidence: 0.6,
        purplePath: [[51, -1], [52, -2], [53, -4]],
        extraPurplePaths: [[[51, -1], [52, -2], [53, -3]]],
      },
    ],
  };

  const routes = multiObserverPathRoutes(response);
  assert.equal(routes.length, 2, 'duplicate observer projections are rendered once');
  assert.deepEqual(routes.map((route) => route.nodes.map(({ lat, lon }) => [lat, lon])), [
    [[51, -1], [52, -2], [53, -3]],
    [[51, -1], [52, -2], [53, -4]],
  ]);
  assert.deepEqual(routes.map((route) => route.confidence), [0.8, 0.6]);
  assert.equal(routes[0]?.nodes[0]?.nodeId, 'node-0', 'canonical hop metadata is retained');
  assert.equal(routes[0]?.nodes[2]?.nodeId, null, 'observer endpoints remain valid coordinate-only nodes');
});
