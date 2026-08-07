import assert from 'node:assert/strict';
import test from 'node:test';
import { extractNeighborNodes } from './neighborPayload.js';

test('extracts the raw neighbor array, including an empty sample', () => {
  const nodes = [{ id: 'ABCD', rssi: -90, snr: 4 }];
  assert.deepEqual(extractNeighborNodes({ nodes }), nodes);
  assert.deepEqual(extractNeighborNodes({ nodes: [] }), []);
});

test('rejects malformed neighbor envelopes', () => {
  assert.equal(extractNeighborNodes(null), null);
  assert.equal(extractNeighborNodes([]), null);
  assert.equal(extractNeighborNodes({}), null);
  assert.equal(extractNeighborNodes({ nodes: {} }), null);
});
