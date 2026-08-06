import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildTopologyLinksGeoJSON,
  buildTopologyNodesGeoJSON,
  type TopologyMapLink,
  type TopologyMapNode,
} from './topologyOverlay.js';

const nodes: TopologyMapNode[] = [
  { nodeId: 'A', name: 'Alpha', lat: 52, lon: -1, degree: 3, observations: 12 },
  { nodeId: 'B', name: 'Bravo', lat: 53, lon: -2, degree: 1, observations: 4 },
  { nodeId: 'unlocated', name: 'No position', lat: null, lon: null, degree: 1, observations: 1 },
];

const links: TopologyMapLink[] = [
  { source: 'A', target: 'B', observations: 12, strongObservations: 4, pathLossDb: 118, lastObserved: '2026-08-06' },
  { source: 'A', target: 'unlocated', observations: 3, strongObservations: 1, pathLossDb: null, lastObserved: '2026-08-06' },
  { source: 'A', target: 'missing', observations: 2, strongObservations: 0, pathLossDb: null, lastObserved: '2026-08-06' },
];

test('topology link GeoJSON is geographic and excludes endpoints without coordinates', () => {
  const geojson = buildTopologyLinksGeoJSON(nodes, links, 'A', false);
  assert.equal(geojson.features.length, 1);
  assert.deepEqual(geojson.features[0]?.geometry.coordinates, [[-1, 52], [-2, 53]]);
  assert.equal(geojson.features[0]?.properties?.['color'], '#f8fafc');
  assert.equal(geojson.features[0]?.properties?.['strong_observations'], 4);
});

test('topology map can filter to multibyte-backed relationships', () => {
  const geojson = buildTopologyLinksGeoJSON(nodes, links, null, true);
  assert.equal(geojson.features.length, 1);
  assert.equal(geojson.features[0]?.properties?.['color'], '#22d3ee');
});

test('topology node GeoJSON carries selection and graph analysis state', () => {
  const geojson = buildTopologyNodesGeoJSON(nodes, 'B', new Set(['A']), new Set(['B']));
  assert.equal(geojson.features.length, 2);
  const alpha = geojson.features.find((feature) => feature.properties?.['node_id'] === 'A');
  const bravo = geojson.features.find((feature) => feature.properties?.['node_id'] === 'B');
  assert.equal(alpha?.properties?.['bridge'], true);
  assert.equal(bravo?.properties?.['selected'], true);
  assert.equal(bravo?.properties?.['isolated'], true);
});
