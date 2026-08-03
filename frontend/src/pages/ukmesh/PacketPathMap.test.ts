import assert from 'node:assert/strict';
import test from 'node:test';
import { buildAerialPaths, type LazyPath, type MultiObserverBetaResponse } from './PacketPathMap.js';

const response: MultiObserverBetaResponse = {
  packetHash: 'ABC',
  network: 'uk',
  canonicalPath: [],
  observers: [{ observerId: 'rx-a' }, { observerId: 'rx-b' }],
  confidence: 0.9,
  results: [
    {
      ok: true,
      packetHash: 'ABC',
      network: 'uk',
      mode: 'resolved',
      canonicalPath: [],
      observers: [{ observerId: 'rx-a' }],
      confidence: 0.9,
      purplePath: [[51, -2], [52, -1], [53, 0]],
    },
    {
      ok: true,
      packetHash: 'ABC',
      network: 'uk',
      mode: 'resolved',
      canonicalPath: [],
      observers: [{ observerId: 'rx-b' }],
      confidence: 0.6,
      purplePath: [[51, -2], [52, -1], [53, 1]],
    },
  ],
};

test('packet map emits every observer route under one packet-scoped edge identity', () => {
  const paths = buildAerialPaths([response], []);

  assert.equal(paths.length, 2);
  assert.deepEqual(paths.map((path) => path.id), ['canonical-ABC', 'canonical-ABC']);
  assert.deepEqual(paths.map((path) => path.confidence), [0.9, 0.6]);
  assert.deepEqual(paths.map((path) => path.nodes.map((node) => node.position)), [
    [[-2, 51], [-1, 52], [0, 53]],
    [[-2, 51], [-1, 52], [1, 53]],
  ]);
});

test('lazy observer branches also share a stable packet scope', () => {
  const lazyPath = (observerLon: number): LazyPath => ({
    canonicalPath: [
      { position: 0, hash: 'AA', nodeId: 'aa', name: 'A', lat: 51, lon: -2, appearances: 2, totalObservations: 2, ambiguous: false, isObserver: false },
      { position: 1, hash: 'BB', nodeId: 'bb', name: 'B', lat: 52, lon: -1, appearances: 2, totalObservations: 2, ambiguous: false, isObserver: false },
      { position: 2, hash: 'RX', nodeId: null, name: 'Observer', lat: 53, lon: observerLon, appearances: 1, totalObservations: 1, ambiguous: false, isObserver: true },
    ],
    coordinates: [[51, -2], [52, -1], [53, observerLon]],
    matchedHops: 2,
    totalHops: 2,
    observerIds: [],
  });

  const paths = buildAerialPaths([], [lazyPath(0), lazyPath(1)], 'packet-abc');
  assert.deepEqual(paths.map((path) => path.id), [
    'hash-traced-packet-abc',
    'hash-traced-packet-abc',
  ]);
});
