import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { pathLearningInputHash } from './rebuild.js';

const baseInput = {
  modelNetwork: 'ukmesh',
  sourceNetwork: 'ukmesh',
  windowStart: new Date('2026-08-01T00:00:00.000Z'),
  windowEnd: new Date('2026-08-08T00:00:00.000Z'),
  privacyGeneration: 5,
  nodes: [{ node_id: 'AA11', lat: 51, lon: -1, elevation_m: null, iata: 'LHR' }],
  links: [{
    node_a_id: 'AA11', node_b_id: 'BB22', itm_path_loss_db: 110,
    count_a_to_b: 3, count_b_to_a: 2,
  }],
  packets: [{
    time: '2026-08-07T12:00:00.000Z', rx_node_id: 'AA11',
    src_node_id: 'BB22', path_hashes: ['CC'],
  }],
};

test('path-learning input hash fences bounds, source rows, and privacy generation', () => {
  const baseline = pathLearningInputHash(baseInput);
  assert.match(baseline, /^[0-9a-f]{64}$/);
  assert.equal(pathLearningInputHash(structuredClone(baseInput)), baseline);
  assert.notEqual(pathLearningInputHash({
    ...baseInput,
    windowEnd: new Date('2026-08-08T01:00:00.000Z'),
  }), baseline);
  assert.notEqual(pathLearningInputHash({ ...baseInput, privacyGeneration: 6 }), baseline);
  assert.notEqual(pathLearningInputHash({
    ...baseInput,
    packets: [{ ...baseInput.packets[0]!, path_hashes: ['DD'] }],
  }), baseline);
});

test('rebuild has no full-table-per-network replacement path and retains all fallback', async () => {
  const source = await readFile(new URL('./rebuild.ts', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /DELETE FROM path_[a-z_]+_priors WHERE network = \$1/);
  assert.doesNotMatch(source, /NOW\(\) - INTERVAL '30 days'/);
  assert.match(source, /rebuildNetwork\('all', undefined, windowStart, windowEnd\)/);
  assert.match(source, /skipped-empty-selected-window/);
  assert.match(source, /updated_at = NOW\(\)/);
});
