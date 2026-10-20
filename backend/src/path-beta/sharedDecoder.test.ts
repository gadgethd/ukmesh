import assert from 'node:assert/strict';
import test from 'node:test';
import {
  decodeBetaCanonicalGroup,
  decodeBetaCanonicalGroupWithHeldPath,
  groupCompatibleObservations,
  projectCanonicalPathForObserver,
  type BetaObserverEntry,
  type BetaSharedDecode,
} from './sharedDecoder.js';
import type { BetaResolveContext, MeshNode, PathPacket } from './types.js';
import { buildChampionPriors } from './championPriors.js';

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

test('source and learned route-position evidence resolve an otherwise equal one-byte collision', () => {
  const source = meshNode('SRC', 51, 1);
  const good = meshNode('AA01', 51.1, 1);
  const other = meshNode('AA02', 51.1, 1);
  const observer = entry('RX', ['AA']);
  observer.rx = meshNode('RX', 51.2, 1);
  observer.packet.src_node_id = source.node_id;
  const ctx = context([source, other, good, observer.rx]);
  ctx.learningModel.champion = buildChampionPriors({
    positionPrefixes: [{ prefix: 'AA', position: 0, node_id: good.node_id, count: 10 }],
    corridors: [{ src_node_id: 'SRC', rx_node_id: 'RX', position: 0, node_id: good.node_id, count: 4 }],
    transitions: [{ from_node_id: 'SRC', to_node_id: good.node_id, count: 10 }],
    positionTransitions: [{ position: 0, from_node_id: 'SRC', to_node_id: good.node_id, count: 10 }],
  });
  assert.equal(decodeBetaCanonicalGroup({ canonicalHashes: ['AA'], members: [observer] }, ctx).hops.get(0)?.nodeId, good.node_id);
});

test('confirmed identities survive missing coordinates and terrain disagreement', () => {
  const absent = meshNode('AA0001', 0, 0);
  const remote = meshNode('BB0002', 57, 1);
  const observer = entry('RX', ['AA0001', 'BB0002']);
  observer.rx = meshNode('RX', 51, 1);
  const ctx = context([absent, remote, observer.rx]);
  const result = decodeBetaCanonicalGroup({ canonicalHashes: observer.hops, members: [observer] }, ctx);
  assert.deepEqual([...result.hops.values()].map((hop) => hop.nodeId), ['AA0001', 'BB0002']);
  assert.equal(result.hops.get(0)?.lat, null);
  assert.equal(result.hops.get(0)?.lon, null);
});

test('later evidence can refine a held hop at the beginning of a long route', () => {
  const nodes = ['AA01', 'AA02', 'BB01', 'CC01', 'DD01'].map((id) => meshNode(id, 51, 1));
  const observer = entry('RX', ['AA', 'BB', 'CC', 'DD']);
  observer.rx = meshNode('RX', 51.1, 1);
  const ctx = context([...nodes, observer.rx]);
  ctx.learningModel.prefixProbabilities.set('unknown|AA||AA02', 1);
  const result = decodeBetaCanonicalGroupWithHeldPath({ canonicalHashes: observer.hops, members: [observer] }, ctx,
    ['AA01', 'BB01', 'CC01', 'DD01']);
  assert.equal(result.refined, true);
  assert.equal(result.hops.get(0)?.nodeId, 'AA02');
});

test('a missing-position candidate needs a real directed edge to its observer', () => {
  const missing = meshNode('AA01', 0, 0);
  const plausible = meshNode('AA02', 51.1, 1);
  const observer = entry('RX', ['AA']);
  observer.rx = meshNode('RX', 51.2, 1);
  const ctx = context([missing, plausible, observer.rx]);
  const rows = {
    positionPrefixes: [{ prefix: 'AA', position: 0, node_id: 'AA01', count: 1000 }],
    corridors: [], transitions: [], positionTransitions: [],
  };
  ctx.learningModel.champion = buildChampionPriors(rows);
  assert.equal(decodeBetaCanonicalGroup({ canonicalHashes: ['AA'], members: [observer] }, ctx).hops.get(0)?.nodeId, 'AA02');
  ctx.learningModel.champion = buildChampionPriors({ ...rows, transitions: [{ from_node_id: 'AA01', to_node_id: 'RX', count: 10 }] });
  const hop = decodeBetaCanonicalGroup({ canonicalHashes: ['AA'], members: [observer] }, ctx).hops.get(0);
  assert.equal(hop?.nodeId, 'AA01');
  assert.equal(hop?.lat, null);
});

test('a relay sharing the source prefix does not erase the source endpoint', () => {
  const source = meshNode('AA0001', 51, 1);
  const relay = meshNode('AA0002', 51.1, 1);
  const observer = entry('RX', ['AA']);
  observer.rx = meshNode('RX', 51.2, 1);
  const ctx = context([source, relay, observer.rx]);
  const decoded: BetaSharedDecode = {
    canonicalHashes: ['AA'], hopConfidences: new Map([[0, 1]]),
    hops: new Map([[0, { hash: 'AA', nodeId: relay.node_id, name: relay.name, lat: relay.lat, lon: relay.lon, margin: 1, ambiguous: false }]]),
  };
  assert.deepEqual(projectCanonicalPathForObserver(observer, decoded, ctx, source).purplePath,
    [[51, 1], [51.1, 1], [51.2, 1]]);
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

test('merges multibyte confirmations into a longer one-byte canonical path', () => {
  const coarse = entry('coarse', ['AA', 'BB', 'CC']);
  const confirmed = entry('confirmed', ['AA01', 'BB02']);
  const groups = groupCompatibleObservations([coarse, confirmed]);

  assert.equal(groups.length, 1);
  assert.deepEqual(groups[0]?.canonicalHashes, ['AA01', 'BB02', 'CC']);
  assert.deepEqual(new Set(groups[0]?.members), new Set([coarse, confirmed]));
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

test('locks confirmed multibyte hops and decodes the remaining one-byte gap', () => {
  const candidates = [
    meshNode('AA01', 51.0, 0.02),
    meshNode('AAFF', 51.0, 0.03),
    meshNode('BB01', 51.2, 0.02),
    meshNode('BB02', 51.2, 0.03),
  ];
  const betaContext = context(candidates);
  betaContext.learningModel.prefixProbabilities.set('unknown|BB|AA|BB01', 0.2);
  betaContext.learningModel.prefixProbabilities.set('unknown|BB|AA|BB02', 0.9);

  const coarse = entry('coarse', ['AA', 'BB']);
  coarse.rx = meshNode('coarse', 51.3, 0.03);
  const confirmed = entry('confirmed', ['AA01']);
  confirmed.rx = meshNode('confirmed', 51.05, 0.02);
  const [group] = groupCompatibleObservations([coarse, confirmed]);
  const decoded = decodeBetaCanonicalGroup(group!, betaContext);

  assert.deepEqual(decoded.canonicalHashes, ['AA01', 'BB']);
  assert.equal(decoded.hops.get(0)?.nodeId, 'AA01');
  assert.equal(decoded.hops.get(0)?.margin, Infinity);
  assert.equal(decoded.hops.get(1)?.nodeId, 'BB02');
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

test('refinement keeps a physical held path when an alternative misses the margin', () => {
  const heldA = meshNode('AA01', 51.0, 0.1);
  const heldB = meshNode('BB01', 51.1, 0.1);
  const alternativeB = meshNode('BB02', 51.1, 0.1);
  const observer = entry('RX', ['AA', 'BB']);
  observer.rx = meshNode('RX', 51.2, 0.1);
  const betaContext = context([heldA, heldB, alternativeB, observer.rx]);
  betaContext.learningModel.prefixProbabilities.set('unknown|BB|AA|BB01', 0.70);
  betaContext.learningModel.prefixProbabilities.set('unknown|BB|AA|BB02', 0.80);

  const decoded = decodeBetaCanonicalGroupWithHeldPath(
    { canonicalHashes: observer.hops, members: [observer] },
    betaContext,
    ['AA01', 'BB01'],
  );

  assert.equal(decoded.held, true);
  assert.equal(decoded.physical, true);
  assert.equal(decoded.refined, false);
  assert.deepEqual([...decoded.hops.values()].map((hop) => hop.nodeId), ['AA01', 'BB01']);
});

test('an impossible held final hop triggers a full re-solve', () => {
  const impossible = meshNode('AA01', 52.1, 0.1);
  const replacement = meshNode('AA02', 51.1, 0.1);
  const observer = entry('RX', ['AA']);
  observer.rx = meshNode('RX', 51.0, 0.1);
  const betaContext = context([impossible, replacement, observer.rx]);

  const decoded = decodeBetaCanonicalGroupWithHeldPath(
    { canonicalHashes: observer.hops, members: [observer] },
    betaContext,
    ['AA01'],
  );

  assert.equal(decoded.held, false);
  assert.equal(decoded.physical, false);
  assert.equal(decoded.hops.get(0)?.nodeId, 'AA02');
});
