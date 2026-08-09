import assert from 'node:assert/strict';
import test from 'node:test';
import { analyzeTopology, createTopologyLoader, shapeTopology } from './topology.js';

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

test('shapeTopology omits links involving opted-out nodes', () => {
  const id = 'D'.repeat(64);
  const topology = shapeTopology([{
    node_a_id: id, node_b_id: 'E'.repeat(64),
    name_a: 'Secret 🚫', name_b: 'Public', lat_a: 52, lon_a: -1, lat_b: 53, lon_b: -2,
    observed_count: 1, multibyte_observed_count: 1, last_observed: '2026-07-11', itm_path_loss_db: null,
  }]);
  assert.deepEqual(topology.nodes, []);
  assert.deepEqual(topology.links, []);
});

const combinedRows = [{
  row_kind: 0,
  row_order: '1',
  node_a_id: 'A',
  node_b_id: 'B',
  name_a: 'Alpha',
  name_b: 'Bravo',
  lat_a: 52,
  lon_a: -1,
  lat_b: 53,
  lon_b: -2,
  iata_a: 'LHR',
  iata_b: 'MAN',
  observed_count: 10,
  multibyte_observed_count: 4,
  last_observed: '2026-08-09T12:00:00.000Z',
  itm_path_loss_db: 110,
  standalone_node_id: null,
  standalone_name: null,
  standalone_lat: null,
  standalone_lon: null,
  standalone_iata: null,
}];

test('complete topology DTO cache singleflights misses and invalidates by privacy generation', async () => {
  let queryCalls = 0;
  let generation = 5;
  let combinedSql = '';
  const loader = createTopologyLoader({
    query: (async (sql: string) => {
      queryCalls += 1;
      combinedSql = sql;
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { rows: combinedRows };
    }) as never,
    networkFilters: () => ({ params: [], nodesAlias: () => '' }) as never,
    getPublicVisibilityGeneration: async () => generation,
    now: () => new Date('2026-08-09T12:30:00.000Z'),
  });
  try {
    const [first, concurrent] = await Promise.all([
      loader.load('ukmesh', 300),
      loader.load('ukmesh', 300),
    ]);
    assert.equal(queryCalls, 1);
    assert.match(combinedSql, /selected_links AS MATERIALIZED/);
    assert.match(combinedSql, /standalone_nodes AS MATERIALIZED/);
    assert.equal(combinedSql.match(/WITH recent_links AS MATERIALIZED/g)?.length, 1);
    assert.strictEqual(first, concurrent);
    assert.equal(first.generatedAt, '2026-08-09T12:30:00.000Z');
    assert.equal(first.summary.observations, 10);

    assert.strictEqual(await loader.load('ukmesh', 300), first);
    assert.equal(queryCalls, 1);

    generation = 6;
    const afterPrivacyChange = await loader.load('ukmesh', 300);
    assert.equal(queryCalls, 2);
    assert.notStrictEqual(afterPrivacyChange, first);
  } finally {
    loader.shutdown();
  }
});

test('failed combined topology loads are never cached', async () => {
  let queryCalls = 0;
  const loader = createTopologyLoader({
    query: (async () => {
      queryCalls += 1;
      if (queryCalls === 1) throw new Error('database unavailable');
      return { rows: combinedRows };
    }) as never,
    networkFilters: () => ({ params: [], nodesAlias: () => '' }) as never,
    getPublicVisibilityGeneration: async () => 1,
  });
  try {
    await assert.rejects(loader.load('ukmesh', 300), /database unavailable/);
    const recovered = await loader.load('ukmesh', 300);
    assert.equal(queryCalls, 2);
    assert.equal(recovered.links.length, 1);
  } finally {
    loader.shutdown();
  }
});
