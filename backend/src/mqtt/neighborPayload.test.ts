import assert from 'node:assert/strict';
import test from 'node:test';
import { extractNeighborNodes } from './neighborPayload.js';

test('extracts the raw neighbor array, including an empty sample', () => {
  const nodes = [{ id: 'ABCD', rssi: -90, snr: 4 }];
  assert.deepEqual(extractNeighborNodes({ nodes }), nodes);
  assert.deepEqual(extractNeighborNodes({ nodes: [] }), []);
});

test('accepts the official firmware UK-spelling envelope', () => {
  const neighbours = [{ id: '5F318F39', snr_db: -4.5, heard_secs_ago: 3863 }];
  assert.deepEqual(extractNeighborNodes({ neighbours }), neighbours);
  assert.deepEqual(extractNeighborNodes({ neighbours: [] }), []);
});

test('rejects malformed neighbor envelopes', () => {
  assert.equal(extractNeighborNodes(null), null);
  assert.equal(extractNeighborNodes([]), null);
  assert.equal(extractNeighborNodes({}), null);
  assert.equal(extractNeighborNodes({ nodes: {} }), null);
});
