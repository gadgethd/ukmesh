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
