import assert from 'node:assert/strict';
import test from 'node:test';
import { analyzeTopology, shapeTopology } from './topology.js';

test('shapeTopology ranks hubs and preserves link evidence', () => {
  const topology = shapeTopology([
    {
      node_a_id: 'A'.repeat(64), node_b_id: 'B'.repeat(64),
      name_a: 'Alpha', name_b: 'Bravo', lat_a: 52, lon_a: -1, lat_b: 53, lon_b: -2,
      observed_count: '10', multibyte_observed_count: '4', last_observed: '2026-07-11', itm_path_loss_db: 110,
    },
    {
      node_a_id: 'A'.repeat(64), node_b_id: 'C'.repeat(64),
      name_a: 'Alpha', name_b: 'Charlie', lat_a: 52, lon_a: -1, lat_b: 54, lon_b: -3,
      observed_count: '7', multibyte_observed_count: '2', last_observed: '2026-07-11', itm_path_loss_db: null,
    },
  ]);
  assert.equal(topology.nodes[0]?.name, 'Alpha');
  assert.equal(topology.nodes[0]?.degree, 2);
  assert.equal(topology.nodes[0]?.observations, 17);
  assert.equal(topology.links[0]?.strongObservations, 4);
  assert.deepEqual(topology.analysis.bridgeNodeIds, ['A'.repeat(64)]);
});

test('analyzeTopology identifies components, articulation points, and isolated nodes', () => {
  const nodes = ['A', 'B', 'C', 'D', 'E'].map((nodeId) => ({
    nodeId, name: nodeId, lat: null, lon: null, degree: 0, observations: 0,
  }));
  const link = (source: string, target: string) => ({
    source, target, observations: 1, strongObservations: 1, pathLossDb: null, lastObserved: '2026-07-11',
  });
  const analysis = analyzeTopology(nodes, [link('A', 'B'), link('B', 'C'), link('C', 'A'), link('C', 'D')]);
  assert.equal(analysis.connectedComponents, 2);
  assert.deepEqual(analysis.bridgeNodeIds, ['C']);
  assert.deepEqual(analysis.isolatedNodeIds, ['E']);
});

test('shapeTopology includes bounded standalone repeater candidates', () => {
  const topology = shapeTopology([], [{ node_id: 'solo', name: 'Solo', lat: 51, lon: -1 }]);
  assert.equal(topology.nodes[0]?.degree, 0);
  assert.deepEqual(topology.analysis.isolatedNodeIds, ['solo']);
});

test('shapeTopology omits opted-out nodes and their dependent links', () => {
  const id = 'D'.repeat(64);
  const topology = shapeTopology([{
    node_a_id: id, node_b_id: 'E'.repeat(64),
    name_a: 'Secret 🚫', name_b: 'Public', lat_a: 52, lon_a: -1, lat_b: 53, lon_b: -2,
    observed_count: 1, multibyte_observed_count: 1, last_observed: '2026-07-11', itm_path_loss_db: null,
  }]);
  assert.equal(topology.nodes.some((node) => node.nodeId === id), false);
  assert.deepEqual(topology.links, []);
});
