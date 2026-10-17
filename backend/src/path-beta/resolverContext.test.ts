import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildOneBytePrefixProbabilities,
  canReusePathContext,
  mergeObserverEvidence,
  preparePacketObservation,
  prepareObservationEntries,
  decodeLazyPathEntries,
  type PreparedPacketObservation,
} from './resolver.js';
import type { PathPacket } from './types.js';
import type { BetaResolveContext, MeshNode } from './types.js';
import { buildChampionPriors } from './championPriors.js';

function prepared(size: number, hops: string[]): PreparedPacketObservation {
  const packet: PathPacket = {
    packet_hash: 'packet',
    rx_node_id: 'observer',
    src_node_id: null,
    packet_type: 0,
    hop_count: hops.length,
    path_hashes: hops,
    path_hash_size_bytes: size,
  };
  return { packet, rx: null, hashes: hops, rawHops: hops, hops, ignoreForPathing: false };
}

test('retains same-observer multibyte anchors while keeping the longer path tail', () => {
  const merged = mergeObserverEvidence(
    prepared(1, ['AA', 'BB', 'CC']),
    prepared(2, ['AA01', 'BB02']),
  );

  assert.deepEqual(merged.hops, ['AA01', 'BB02', 'CC']);
  assert.equal(merged.packet.path_hash_size_bytes, 1, 'the longer observation remains the projection row');
});

const receiver: MeshNode = { node_id: 'AA0011', name: null, lat: 51, lon: 1, role: 2, iata: null, elevation_m: null, last_seen: null };

test('RX prefix collisions retain the final relay; TX strips the appended observer', () => {
  const raw = { ...prepared(1, ['BB', 'AA']).packet, rx_node_id: receiver.node_id };
  assert.deepEqual(preparePacketObservation({ ...raw, direction: 'rx' }, receiver).hops, ['BB', 'AA']);
  assert.deepEqual(preparePacketObservation({ ...raw, direction: 'tx' }, receiver).hops, ['BB']);
});

test('direct-routing destinations and malformed middle hashes cannot become observed relay chains', () => {
  assert.equal(preparePacketObservation({ ...prepared(1, ['AA', 'BB']).packet, route_type: 2 }, receiver).ignoreForPathing, true);
  const bad = preparePacketObservation(prepared(1, ['AA', 'zz', 'BB']).packet, receiver);
  assert.equal(bad.ignoreForPathing, true);
  assert.equal(bad.rawHops.length, 3);
});

test('keeps distinct routes at the same observer while de-duplicating identical receptions', () => {
  const a = { ...prepared(3, ['BB0011', 'CC0011']).packet, rx_node_id: receiver.node_id };
  const b = { ...a, path_hashes: ['BB0022', 'CC0022'] };
  const entries = prepareObservationEntries([a, b, a], {
    nodesById: new Map([[receiver.node_id, receiver]]),
  } as BetaResolveContext);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries.map((entry) => entry.hops), [a.path_hashes, b.path_hashes]);
  const long = { ...a, path_hash_size_bytes: 1, hop_count: 40, path_hashes: new Array(40).fill('BB') };
  assert.equal(prepareObservationEntries([long], { nodesById: new Map([[receiver.node_id, receiver]]) } as BetaResolveContext)[0]?.hops.length, 40);
});

test('packet-detail projections end at their own observers without creating cross-observer edges', () => {
  const relayA = { ...receiver, node_id: 'BB0011' };
  const relayB = { ...receiver, node_id: 'CC0022' };
  const rxLong = { ...receiver, node_id: 'DD0033' };
  const ctx: BetaResolveContext = {
    loadedAt: 0, visibilityGeneration: 0,
    nodesById: new Map([relayA, relayB, receiver, rxLong].map((node) => [node.node_id, node])),
    repeaterNodes: [relayA, relayB, receiver, rxLong], linkMetrics: new Map(),
    learningModel: { prefixProbabilities: new Map(), transitionProbabilities: new Map(), edgeScores: new Map(), motifProbabilities: new Map(),
      confidenceScale: 1, confidenceBias: 0, bucketHours: 6,
      champion: buildChampionPriors({ positionPrefixes: [], corridors: [], transitions: [], positionTransitions: [] }) },
  };
  const short = { ...prepared(3, ['BB0011']).packet, rx_node_id: receiver.node_id, direction: 'rx' };
  const long = { ...prepared(3, ['BB0011', 'CC0022']).packet, rx_node_id: rxLong.node_id, direction: 'rx' };
  const result = decodeLazyPathEntries('packet', prepareObservationEntries([short, long], ctx), ctx)!;
  assert.equal(result.observerCount, 2);
  assert.deepEqual(result.paths.find((path) => path.observerIds[0] === receiver.node_id)?.canonicalPath.map((node) => node.nodeId), ['BB0011', receiver.node_id]);
  assert.deepEqual(result.paths.find((path) => path.observerIds[0] === rxLong.node_id)?.canonicalPath.map((node) => node.nodeId), ['BB0011', 'CC0022', rxLong.node_id]);
});

test('collapses multibyte path facts into calibrated one-byte prefix priors', () => {
  const probabilities = buildOneBytePrefixProbabilities([
    { prefix: 'AA01', receiver_region: 'LON', prev_prefix: 'BB01', node_id: 'node-a', count: 3 },
    { prefix: 'AA02', receiver_region: 'LON', prev_prefix: 'BBFF', node_id: 'node-a', count: 2 },
    { prefix: 'AAFF', receiver_region: 'LON', prev_prefix: 'BB02', node_id: 'node-b', count: 5 },
    { prefix: 'CC01', receiver_region: 'LON', prev_prefix: null, node_id: 'node-c', count: 4 },
  ]);

  assert.equal(probabilities.get('LON|AA|BB|node-a'), 0.5);
  assert.equal(probabilities.get('LON|AA|BB|node-b'), 0.5);
  assert.equal(probabilities.get('LON|CC||node-c'), 1);
  assert.equal([...probabilities.keys()].some((key) => key.includes('AA01')), false);
});

test('interactive path context reuse expires normally', () => {
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 7,
    ageMs: 1,
    pinForBatch: false,
  }), true);
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 7,
    ageMs: 60 * 60_000,
    pinForBatch: false,
  }), false);
});

test('batch path context stays pinned only within the same visibility generation', () => {
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 7,
    ageMs: 60 * 60_000,
    pinForBatch: true,
  }), true);
  assert.equal(canReusePathContext({
    cachedVisibilityGeneration: 7,
    currentVisibilityGeneration: 8,
    ageMs: 1,
    pinForBatch: true,
  }), false);
});
