import assert from 'node:assert/strict';
import test from 'node:test';
import { UKMESH_NETWORKS } from '../../networks.js';
import { networkFilters, publicNetworkFilters } from './networkFilters.js';

test('explicit test packet scopes retain the standard meshcore-test topic', () => {
  const filters = networkFilters('test');
  const clause = filters.packetsAlias('p');

  assert.deepEqual(filters.params, ['test']);
  assert.match(clause, /p\.network = \$1/);
  assert.doesNotMatch(clause, /meshcore-test/);
});

test('public packet scopes keep legacy test-topic rows out', () => {
  const filters = networkFilters('ukmesh');
  const clause = filters.packetsAlias('p');

  assert.match(clause, /p\.network = ANY\(\$1\)/);
  assert.match(clause, /NULLIF\(p\.topic_prefix, ''\)/);
  assert.match(clause, /split_part\(p\.topic, '\/', 1\)/);
  assert.match(filters.packets, /NULLIF\(topic_prefix, ''\)/);
  assert.match(filters.packets, /split_part\(topic, '\/', 1\)/);
  assert.match(filters.packets, /visibility_ok IS TRUE/);
  assert.match(filters.packets, /private_node_prefixes private_prefix/);
  assert.match(filters.packets, /packets\.network/);
  assert.match(filters.packets, /unnest\(COALESCE\(packets\.path_hashes/);
  assert.doesNotMatch(filters.packets, /private_node\.name LIKE '%🚫%'/);
});

test('owned public visibility scopes carry observer filtering', () => {
  const filters = publicNetworkFilters({
    access: 'public',
    network: 'ukmesh',
    observer: 'observer-1',
  });

  assert.deepEqual(filters.params, [UKMESH_NETWORKS, 'observer-1']);
  assert.match(filters.packets, /rx_node_id = \$2/);
});
