import assert from 'node:assert/strict';
import test from 'node:test';
import { networkFilters } from './networkFilters.js';

test('explicit test packet scopes retain the standard meshcore-test topic', () => {
  const filters = networkFilters('test');
  const clause = filters.packetsAlias('p');

  assert.deepEqual(filters.params, ['test']);
  assert.match(clause, /p\.network = \$1/);
  assert.doesNotMatch(clause, /meshcore-test/);
});

test('public packet scopes keep legacy test-topic rows out', () => {
  const clause = networkFilters('ukmesh').packetsAlias('p');

  assert.match(clause, /p\.network = ANY\(\$1\)/);
  assert.match(clause, /split_part\(p\.topic, '\/', 1\) <> 'meshcore-test'/);
});
