import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildOneBytePrefixProbabilities,
  canReusePathContext,
  mergeObserverEvidence,
  type PreparedPacketObservation,
} from './resolver.js';
import type { PathPacket } from './types.js';

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
