import assert from 'node:assert/strict';
import test from 'node:test';
import type { Filters } from '../components/FilterPanel/FilterPanel.js';
import { resolveTerrainCoverageConflict } from './mapLayerCompatibility.js';

const filters = (terrain: boolean, coverage: boolean): Filters => ({
  livePackets: true,
  links: false,
  terrain,
  clientNodes: false,
  packetHistory: false,
  coverage,
  heatmap: false,
  betaPaths: false,
  betaPathThreshold: 0.45,
  hexClashes: false,
  hexClashMaxHops: 3,
});

test('enabling RF coverage turns off 3D terrain', () => {
  assert.deepEqual(
    resolveTerrainCoverageConflict(filters(true, false), filters(true, true)),
    filters(false, true),
  );
});

test('enabling 3D terrain turns off RF coverage', () => {
  assert.deepEqual(
    resolveTerrainCoverageConflict(filters(false, true), filters(true, true)),
    filters(true, false),
  );
});

test('ambiguous persisted state and shared URLs prefer RF coverage', () => {
  assert.deepEqual(
    resolveTerrainCoverageConflict(filters(false, false), filters(true, true)),
    filters(false, true),
  );
});

test('compatible layer combinations are unchanged', () => {
  for (const next of [filters(false, false), filters(true, false), filters(false, true)]) {
    assert.equal(resolveTerrainCoverageConflict(filters(false, false), next), next);
  }
});
