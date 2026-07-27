import assert from 'node:assert/strict';
import test from 'node:test';
import { isPrivateNode, redactPrivateNode } from './privateNode.js';

test('private-node fallback exposes no stable identity, geometry, or extra fields', () => {
  const redacted = redactPrivateNode({
    node_id: '001122334455',
    name: 'Home 🚫',
    iata: 'ABC',
    public_key: 'secret',
    lat: 54.123,
    lon: -1.234,
    last_seen: '2026-01-01',
  });

  assert.deepEqual(redacted, {
    node_id: 'private',
    name: 'Private Node',
    iata: null,
    public_key: null,
    lat: null,
    lon: null,
  });
  assert.equal(isPrivateNode(redacted.name), false);
});

test('public nodes are returned unchanged', () => {
  const node = { node_id: 'public', name: 'Hilltop', lat: 54, lon: -1 };
  assert.equal(redactPrivateNode(node), node);
});
