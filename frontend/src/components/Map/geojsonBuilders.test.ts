import assert from 'node:assert/strict';
import test from 'node:test';

import type { NodeCoverage } from '../../hooks/useCoverage.js';
import { buildCoverageGeoJSON } from './geojsonBuilders.js';

const polygon = (offset: number): NodeCoverage['geom'] => ({
  type: 'Polygon',
  coordinates: [[
    [offset, offset],
    [offset + 1, offset],
    [offset + 1, offset + 1],
    [offset, offset],
  ]],
});

test('RF coverage emits one independently styled feature per signal band', () => {
  const coverage: NodeCoverage = {
    node_id: 'AA'.repeat(32),
    geom: polygon(0),
    strength_geoms: {
      green: polygon(1),
      amber: polygon(2),
      red: polygon(3),
    },
  };

  const result = buildCoverageGeoJSON([coverage]);

  assert.deepEqual(
    result.features.map((feature) => feature.properties?.band),
    ['red', 'amber', 'green'],
  );
  assert.ok(result.features.every(
    (feature) => feature.properties?.node_id === coverage.node_id,
  ));
});

test('legacy single-polygon coverage remains visible as the green band', () => {
  const coverage: NodeCoverage = {
    node_id: 'BB'.repeat(32),
    geom: polygon(0),
  };

  const result = buildCoverageGeoJSON([coverage]);

  assert.equal(result.features.length, 1);
  assert.equal(result.features[0]?.properties?.band, 'green');
});
