import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  loadMigrationFiles,
  migrationChecksum,
  validateMigrationPrefixes,
} from './migrations.js';

test('migration checksums are stable and change with file contents', () => {
  assert.equal(
    migrationChecksum('SELECT 1;\n'),
    'b4e0497804e46e0a0b0b8c31975b062152d551bac49c3c2e80932567b4085dcd',
  );
  assert.notEqual(migrationChecksum('SELECT 1;\n'), migrationChecksum('SELECT 2;\n'));
});

test('migration prefixes reject accidental duplicates but preserve shipped 016 history', () => {
  assert.doesNotThrow(() => validateMigrationPrefixes([
    '016_private_prefixes.sql',
    '016_stale_mqtt_observer_cleanup.sql',
  ]));
  assert.throws(
    () => validateMigrationPrefixes(['023_first.sql', '023_second.sql']),
    /duplicate migration prefix 023/,
  );
  assert.throws(() => validateMigrationPrefixes(['migration.sql']), /numeric prefix/);
});

test('concurrent index operations require the explicit non-transactional directive', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'meshcore-migrations-'));
  try {
    writeFileSync(path.join(dir, '023_index.sql'), 'CREATE INDEX CONCURRENTLY idx ON t (id);\n');
    assert.throws(() => loadMigrationFiles(dir), /non-transactional directive/);
    writeFileSync(
      path.join(dir, '023_index.sql'),
      '-- meshcore:migration-mode non-transactional\n'
      + 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON t (id);\n',
    );
    const [migration] = loadMigrationFiles(dir);
    assert.equal(migration?.transactional, false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// Validate the real tree as well as synthetic names: merging independent
// branches must never ship a new duplicate prefix or lose either legacy file.
test('merged migration tree preserves shipped names and rejects new prefix collisions', () => {
  const names = loadMigrationFiles().map((migration) => migration.name);
  for (const name of [
    '052_node_identity_packets_raw_columns.sql', '052_owner_packet_sharing.sql',
    '055_message_tags.sql', '055_public_feed.sql',
    '061_node_status_sample_status.sql', '062_silent_failure_recovery.sql',
    '066_prefix_visibility_statement_fence.sql',
  ]) assert.ok(names.includes(name), `${name} is missing`);
  assert.throws(() => validateMigrationPrefixes([
    ...names, '062_accidental_collision.sql',
  ]), /duplicate migration prefix 062/);
});

// The fence must survive the P1-4 schema moves. main ships the row fence as 074
// (renamed from 071), 087 replaces its function with the statement body, 113
// moves the prefix registry to core and 108/128 leave read-only views at the
// old public names. This test pins the merged ordering and the post-move
// relation names that make the re-assert apply on top of all of them.
test('merged tree re-asserts the prefix fence above 132 against post-move relations', () => {
  const migrations = loadMigrationFiles();
  const names = migrations.map((migration) => migration.name);
  const fenceName = '134_reassert_prefix_visibility_fence.sql';
  const fence = migrations.find((migration) => migration.name === fenceName)
    ?? assert.fail(`${fenceName} is missing`);

  const prefixOf = (name: string): number => Number(/^(\d+)_/.exec(name)?.[1]);
  assert.ok(prefixOf(fenceName) > prefixOf('132_restore_reader_schema_usage.sql'),
    'the re-assert must run after 132');
  assert.equal(prefixOf(fenceName), Math.max(...names.map(prefixOf)),
    'the re-assert must be the migration head');
  assert.ok(prefixOf(fenceName) > prefixOf('128_corescope_privacy_compat.sql'));

  // Simulate the merged tree: main's 074 joins this branch's files with no
  // duplicate prefix, sorts before the moves, and the re-assert sorts last.
  const mergedNames = [...names, '074_prefix_visibility_row_fence.sql']
    .sort((a, b) => a.localeCompare(b));
  assert.doesNotThrow(() => validateMigrationPrefixes(mergedNames));
  assert.ok(!names.includes('074_prefix_visibility_row_fence.sql'),
    '074 comes from main and must not be re-added here');
  for (const earlier of [
    '074_prefix_visibility_row_fence.sql',
    '087_move_privacy_visibility_cluster.sql',
    '108_compat_visibility_views.sql',
    '113_move_core_private_node_prefixes.sql',
    '128_corescope_privacy_compat.sql',
    '132_restore_reader_schema_usage.sql',
  ]) {
    assert.ok(
      mergedNames.indexOf(earlier) < mergedNames.indexOf(fenceName),
      `${earlier} must sort before ${fenceName}`,
    );
  }

  // Post-move relations only: the public names are read-only compat views and
  // ALTER/CREATE TRIGGER against them fails (the original 074 failure). Prose
  // that records the history is stripped so only executable SQL is checked.
  const sqlCode = fence.sql.replace(/^\s*--.*$/gm, '');
  assert.match(sqlCode, /core\.private_node_prefixes/);
  assert.match(sqlCode, /ops\.public_visibility_state/);
  assert.doesNotMatch(
    sqlCode,
    /public\.(?:private_node_prefixes|public_visibility_state|packet_visibility_materialization_state)\b/,
  );
  assert.doesNotMatch(
    sqlCode,
    /(?:FROM|JOIN|INTO|UPDATE|TABLE|ON)\s+(?:private_node_prefixes|public_visibility_state|packet_visibility_materialization_state)\b/,
  );
  assert.doesNotMatch(sqlCode, /new_prefix_rows|old_prefix_rows/,
    'a ROW trigger cannot use transition tables');

  // Idempotent re-assert: replace in place, drop before create, and only stamp
  // the marker when it is still unset.
  assert.match(sqlCode, /CREATE OR REPLACE FUNCTION public\.bump_visibility_for_changed_prefixes\(\)/);
  assert.match(sqlCode, /ADD COLUMN IF NOT EXISTS prefix_fence_xid/);
  assert.match(sqlCode, /DROP TRIGGER IF EXISTS private_node_prefixes_visibility_insert_rows\s+ON core\.private_node_prefixes/);
  assert.match(sqlCode, /AFTER INSERT ON core\.private_node_prefixes\s+FOR EACH ROW EXECUTE FUNCTION public\.bump_visibility_for_changed_prefixes\(\)/);
  assert.match(sqlCode, /to_regclass\('core\.private_node_prefixes'\)/);
  assert.match(sqlCode, /state\.prefix_fence_xid IS NULL/);
});
