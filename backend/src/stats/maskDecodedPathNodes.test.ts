import assert from 'node:assert/strict';
import test from 'node:test';
import { maskDecodedPathNodes } from './maskDecodedPathNodes.js';

test('decoded path output omits private nodes instead of deriving nearby geometry', () => {
  const nodes = maskDecodedPathNodes([
    { ord: 1, node_id: 'private', name: 'Home 🚫', lat: 54.1, lon: -1.2 },
    { ord: 2, node_id: 'public', name: 'Hilltop', lat: 54.2, lon: -1.3 },
  ]);

  assert.deepEqual(nodes, [
    { ord: 2, node_id: 'public', name: 'Hilltop', lat: 54.2, lon: -1.3 },
  ]);
});
