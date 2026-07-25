import assert from 'node:assert/strict';
import test from 'node:test';
import { maskDecodedPathNodes } from './maskDecodedPathNodes.js';

test('decoded path masking suppresses the complete path when any relay is private', () => {
  assert.deepEqual(maskDecodedPathNodes([
    {
      ord: 1,
      node_id: 'a'.repeat(64),
      name: 'Public relay',
      lat: 51,
      lon: -1,
    },
    {
      ord: 2,
      node_id: 'b'.repeat(64),
      name: 'Private relay 🚫',
      lat: 52,
      lon: -2,
    },
  ]), []);
});

test('decoded path masking preserves allowlisted fields for entirely public paths', () => {
  assert.deepEqual(maskDecodedPathNodes([{
    ord: 1,
    node_id: 'a'.repeat(64),
    name: 'Public relay',
    lat: 51,
    lon: -1,
    last_seen: '2026-07-25T00:00:00Z',
  }]), [{
    ord: 1,
    node_id: 'a'.repeat(64),
    name: 'Public relay',
    lat: 51,
    lon: -1,
  }]);
});
