import assert from 'node:assert/strict';
import test from 'node:test';
import { redactPrivateNode } from './privateNode.js';

test('private node redaction never derives public geometry from exact coordinates', () => {
  const privateNode = redactPrivateNode({
    node_id: `abcdef123456${'0'.repeat(52)}`,
    name: 'Regression Private 🚫',
    lat: 52.123456,
    lon: -1.234567,
    iata: 'ABC',
    public_key: 'secret',
  });
  assert.deepEqual(privateNode, {
    node_id: 'private',
    name: 'Private Node',
    lat: null,
    lon: null,
    iata: null,
    public_key: null,
  });
});
