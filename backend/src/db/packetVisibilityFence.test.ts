import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const migration = readFileSync(
  new URL('./migrations/042_packet_visibility_fence.sql', import.meta.url),
  'utf8',
);
const asyncMigration = readFileSync(
  new URL('./migrations/056_async_privacy_rematerialization.sql', import.meta.url),
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

test('node triggers enqueue rematerialization and leave paired generation advancement to worker', () => {
  assert.match(asyncMigration, /CREATE TABLE IF NOT EXISTS privacy_rematerialization_queue/);
  assert.match(asyncMigration, /VALUES \(changed_node_id, changed_network, 'packets'\)/);
  assert.match(asyncMigration, /VALUES \(changed_node_id, changed_network, 'packet_paths'\)/);
  assert.doesNotMatch(asyncMigration, /UPDATE packets p|UPDATE packet_paths path/);
  assert.match(asyncMigration, /CREATE TRIGGER nodes_public_visibility_generation/);
  assert.doesNotMatch(asyncMigration, /SET generation = generation \+ 1/);
});
