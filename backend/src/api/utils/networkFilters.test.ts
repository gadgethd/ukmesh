import assert from 'node:assert/strict';
import test from 'node:test';
import { UKMESH_NETWORKS } from '../../networks.js';
import { networkFilters, nodeAliasArraySql, publicNetworkFilters } from './networkFilters.js';

test('requested identities resolve once to an indexable alias array', () => {
  const sql = nodeAliasArraySql('$4');
  assert.match(sql, /WITH requested_identity AS MATERIALIZED/);
  assert.match(sql, /source_node_id = UPPER\(BTRIM\(\$4\)\)/);
  assert.match(sql, /requested\.canonical_node_id = alias\.canonical_node_id/);
  assert.doesNotMatch(sql, /meshcore_canonical_node_id/);
});

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
  assert.match(filters.packets, /is_private IS NOT TRUE/);
  assert.match(filters.packets, /packet_visibility_materialization_state/);
  assert.match(filters.packets, /cached_visibility\.visibility_generation = current_visibility\.generation/);
  assert.doesNotMatch(filters.packets, /private_node_prefixes|unnest\(/);
  assert.doesNotMatch(filters.packets, /private_node\.name LIKE '%🚫%'/);
});

test('includePrivacy:false omits materialized visibility conditions', () => {
  const filters = networkFilters('ukmesh', undefined, { includePrivacy: false });
  assert.match(filters.packets, /network = ANY\(\$1\)/);
  assert.doesNotMatch(filters.packets, /visibility_ok IS TRUE/);
  assert.doesNotMatch(filters.packets, /packet_visibility_materialization_state/);
  const defaulted = networkFilters('ukmesh');
  assert.match(defaulted.packets, /visibility_ok IS TRUE/);
});

test('owned public visibility scopes carry observer filtering', () => {
  const filters = publicNetworkFilters({
    access: 'public',
    network: 'ukmesh',
    observer: 'observer-1',
  });

  assert.deepEqual(filters.params, [UKMESH_NETWORKS, 'observer-1']);
  assert.match(filters.packets, /rx_node_id = ANY\(ARRAY\(/);
  assert.match(filters.packets, /source_node_id = UPPER\(BTRIM\(\$2\)\)/);
  assert.doesNotMatch(filters.packets, /meshcore_canonical_node_id/);
});
