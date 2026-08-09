import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PATH_LEARNING_DELTA_DEFINITIONS,
  pathLearningDeltaMergeSql,
} from './deltaPublication.js';

test('every path-learning table publishes semantic deltas from a staged generation', () => {
  assert.equal(PATH_LEARNING_DELTA_DEFINITIONS.length, 7);
  assert.ok(PATH_LEARNING_DELTA_DEFINITIONS.some((definition) => (
    definition.target === 'path_prefix_priors'
  )));
  assert.ok(PATH_LEARNING_DELTA_DEFINITIONS.some((definition) => (
    definition.target === 'path_edge_priors'
  )));
  for (const definition of PATH_LEARNING_DELTA_DEFINITIONS) {
    const sql = pathLearningDeltaMergeSql(definition);
    assert.match(sql, new RegExp(`FROM ${definition.stage} desired`));
    assert.match(sql, /IS DISTINCT FROM/);
    assert.match(sql, /AND NOT EXISTS/);
    assert.match(sql, /ON CONFLICT/);
    assert.match(sql, new RegExp(`desired\\.${definition.keys[0]} = ${definition.target}\\.${definition.keys[0]}`));
    assert.doesNotMatch(sql, new RegExp(`DELETE FROM ${definition.target}\\s+WHERE network = \\$1\\s+RETURNING`));
  }
});

test('delta publication indexes every staged generation before absence deletes', async () => {
  const source = await import('node:fs/promises').then((fs) => (
    fs.readFile(new URL('./deltaPublication.ts', import.meta.url), 'utf8')
  ));
  assert.match(source, /CREATE INDEX \$\{definition\.stage\}_keys_idx/);
  assert.match(source, /ANALYZE \$\{definition\.stage\}/);
});
