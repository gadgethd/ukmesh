import assert from 'node:assert/strict';
import test from 'node:test';
import { loadInitialMapNodes } from './initialStateNodes.js';

test('initial map lookup unions only the supplied packet/message endpoints', async () => {
  const signal = new AbortController().signal;
  const fresh = { node_id: 'AA', name: 'Fresh', lat: 54, lon: -1, role: 2 };
  const nodes = await loadInitialMapNodes(async (...args) => {
    assert.deepEqual(args, ['ukmesh', 'OBSERVER', 'slim', signal, 'northeast', {
      mapVisible: true, referencedNodeIds: ['AA', 'BB', 'CC'],
    }]);
    return [fresh];
  }, 'ukmesh', 'OBSERVER', [
    { src_node_id: ' aa ', rx_node_id: 'bb' },
    { src_node_id: 'CC', rx_node_id: 'BB' },
    { src_node_id: null, rx_node_id: undefined },
  ], 'northeast', signal);
  assert.deepEqual(nodes, [fresh]);
});

test('empty initial traffic still opts into map visibility', async () => {
  await loadInitialMapNodes(async (...args) => {
    assert.deepEqual(args, [undefined, undefined, 'slim', undefined, undefined, {
      mapVisible: true, referencedNodeIds: [],
    }]);
    return [];
  }, undefined, undefined, []);
});

test('initial map endpoint union retains the private-name safety check', async () => {
  const nodes = await loadInitialMapNodes(async () => [
    { node_id: 'AA', name: 'Public', lat: 54, lon: -1, role: 2 },
    { node_id: 'BB', name: 'Private 🚫', lat: 54, lon: -1, role: 2 },
  ], 'ukmesh', undefined, [{ src_node_id: 'BB' }]);
  assert.deepEqual(nodes.map((node) => node.node_id), ['AA']);
});
