import assert from 'node:assert/strict';
import test from 'node:test';
import {
  decodeBetaCanonicalGroup,
  groupCompatibleObservations,
  projectCanonicalPathForObserver,
  type BetaObserverEntry,
  type BetaSharedDecode,
} from './sharedDecoder.js';
import type { BetaResolveContext, MeshNode, PathPacket } from './types.js';

const meshNode = (node_id: string, lat: number, lon: number, iata: string | null = null): MeshNode => ({
  node_id,
  name: node_id,
  lat,
  lon,
  iata,
  role: 2,
  elevation_m: null,
  last_seen: null,
});

function packet(rx_node_id: string, path_hashes: string[], packet_type = 0): PathPacket {
  return {
    packet_hash: 'packet',
    rx_node_id,
    src_node_id: null,
    packet_type,
    hop_count: path_hashes.length,
    path_hashes,
    path_hash_size_bytes: 1,
  };
}

function entry(observerId: string, hops: string[]): BetaObserverEntry {
  return {
    observerId,
    packet: packet(observerId, hops),
    rx: meshNode(observerId, 51.3, 0),
    hashes: hops,
    hops,
  };
}

function context(nodes: MeshNode[]): BetaResolveContext {
  return {
    loadedAt: 0,
    visibilityGeneration: 1,
    nodesById: new Map(nodes.map((node) => [node.node_id, node])),
    repeaterNodes: nodes,
    linkMetrics: new Map(),
    mlPrefixScores: new Map(),
    learningModel: {
      prefixProbabilities: new Map(),
      transitionProbabilities: new Map(),
      edgeScores: new Map(),
      motifProbabilities: new Map(),
      confidenceScale: 1,
      confidenceBias: 0,
      bucketHours: 6,
    },
  };
}

test('groups only prefix-compatible observer paths under one canonical decode', () => {
  const short = entry('short', ['AA']);
  const long = entry('long', ['AA', 'BB']);
  const divergent = entry('divergent', ['CC']);
  const groups = groupCompatibleObservations([short, divergent, long]);

  assert.equal(groups.length, 2);
  assert.deepEqual(groups[0]?.canonicalHashes, ['AA', 'BB']);
  assert.deepEqual(new Set(groups[0]?.members), new Set([short, long]));
  assert.deepEqual(groups[1]?.canonicalHashes, ['CC']);
});

test('beta canonical groups use the shared decoder evidence model', () => {
  const candidates = [
    meshNode('AA01', 51.0, 0.02),
    meshNode('AA02', 51.0, 0.03),
    meshNode('BB01', 51.2, 0.02),
    meshNode('BB02', 51.2, 0.03),
  ];
  const betaContext = context(candidates);
  betaContext.learningModel.prefixProbabilities.set('unknown|AA||AA01', 0.8);
  betaContext.learningModel.prefixProbabilities.set('unknown|AA||AA02', 0.7);
  betaContext.learningModel.prefixProbabilities.set('unknown|BB|AA|BB01', 0.8);
  betaContext.learningModel.prefixProbabilities.set('unknown|BB|AA|BB02', 0.7);
  betaContext.learningModel.transitionProbabilities.set('unknown|AA01|BB01', 1);

  const observer = entry('RX', ['AA', 'BB']);
  observer.rx = meshNode('RX', 51.3, 0.02);
  const decoded = decodeBetaCanonicalGroup(
    { canonicalHashes: observer.hops, members: [observer] },
    betaContext,
  );

  assert.equal(decoded.hops.get(0)?.nodeId, 'AA01');
  assert.equal(decoded.hops.get(1)?.nodeId, 'BB01');
});

test('observer projection breaks at unresolved hops and never invents a connecting path', () => {
  const observer = entry('RX', ['AA', 'BB', 'CC']);
  observer.packet.packet_type = 4;
  observer.packet.src_node_id = 'SRC';
  observer.rx = meshNode('RX', 51.3, 0);
  const source = meshNode('SRC', 50.9, 0);
  const betaContext = context([source, observer.rx]);
  const decoded: BetaSharedDecode = {
    canonicalHashes: observer.hops,
    hops: new Map([
      [0, { hash: 'AA', nodeId: 'AA01', name: null, lat: 51, lon: 0, margin: Infinity, ambiguous: false }],
      [1, { hash: 'BB', nodeId: null, name: null, lat: null, lon: null, margin: 0, ambiguous: false }],
      [2, { hash: 'CC', nodeId: 'CC01', name: null, lat: 51.2, lon: 0, margin: Infinity, ambiguous: false }],
    ]),
    hopConfidences: new Map([[0, 1], [1, 0], [2, 1]]),
  };

  const projection = projectCanonicalPathForObserver(observer, decoded, betaContext, source);

  assert.deepEqual(projection.purplePath, [[51.2, 0], [51.3, 0]]);
  assert.deepEqual(projection.extraPurplePaths, [[[50.9, 0], [51, 0]]]);
  assert.equal(projection.remainingHops, 1);
  assert.equal(projection.resolvedHopCount, 2);
  assert.ok(Math.abs(projection.confidence - (2 / 3)) < 1e-12);
});
