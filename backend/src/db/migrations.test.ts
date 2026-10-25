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
