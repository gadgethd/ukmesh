import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const migration = readFileSync(
  new URL('./migrations/042_packet_visibility_fence.sql', import.meta.url),
  'utf8',
);

test('packet visibility materialization is fenced across privacy identity changes', () => {
  assert.match(migration, /packet_visibility_materialization_state/);
  assert.match(migration, /FOR UPDATE/);
  assert.match(migration, /nodes_packet_visibility_serialization/);
  assert.match(migration, /nodes_private_prefix_materialization sorts before this trigger/);
  assert.match(migration, /TG_TABLE_NAME = 'node_identity_aliases'/);
  assert.match(migration, /TG_TABLE_NAME = 'private_node_prefixes' AND pg_trigger_depth\(\) > 1/);
  assert.match(migration, /deliberately leave the materialization\s+-- generation stale/);
});
