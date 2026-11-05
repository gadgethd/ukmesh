import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  BASE_SCHEMA_BOOTSTRAP_APPROVAL,
  decideLedgerBootstrap,
  loadMigrationFiles,
  migrationChecksum,
  parseBaseSchemaShape,
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

const shippedBaseSchema = parseBaseSchemaShape(
  readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8'),
);

test('base schema parsing reads only the relations the init hook actually creates', () => {
  assert.equal(shippedBaseSchema.relations.size, 33);
  for (const relation of ['public.nodes', 'public.packets', 'public.packet_hourly_stats']) {
    assert.ok(shippedBaseSchema.relations.has(relation), `${relation} missing from base.sql`);
  }
  // Indexes and hypertable calls are not relations and must not leak in.
  assert.ok(!shippedBaseSchema.relations.has('public.packets_hash_idx'));
  assert.ok(!shippedBaseSchema.relations.has('public.schema_migrations'));
  assert.deepEqual([...shippedBaseSchema.schemas], ['public']);
});

test('a database seeded from the shipped base schema still needs the explicit bootstrap approval', () => {
  const database = { schemas: new Set(['public']), relations: shippedBaseSchema.relations };
  assert.deepEqual(
    decideLedgerBootstrap({ approval: '', baseSchema: shippedBaseSchema, database }),
    {
      action: 'refuse',
      reason: 'public.schema_migrations is missing on a non-empty database; '
        + 'refusing to create a new ledger or replay historical migrations '
        + '(only a deliberate, documented base-schema bootstrap via '
        + 'MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL may initialise one)',
    },
  );
  assert.equal(
    decideLedgerBootstrap({ approval: 'true', baseSchema: shippedBaseSchema, database }).action,
    'refuse',
  );
  assert.equal(
    decideLedgerBootstrap({ approval: 'BOOTSTRAP-LEDGER-FROM-BASE-SCHEMA', baseSchema: shippedBaseSchema, database }).action,
    'refuse',
  );
  assert.deepEqual(
    decideLedgerBootstrap({
      approval: BASE_SCHEMA_BOOTSTRAP_APPROVAL,
      baseSchema: shippedBaseSchema,
      database,
    }),
    { action: 'initialise-from-base-schema' },
  );
});

test('the bootstrap approval never covers a database that drifted from the base schema', () => {
  const approval = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  const driftedRelations = new Set([...shippedBaseSchema.relations, 'public.ml_gold_paths']);
  const drifted = decideLedgerBootstrap({
    approval,
    baseSchema: shippedBaseSchema,
    database: { schemas: new Set(['public']), relations: driftedRelations },
  });
  assert.equal(drifted.action, 'refuse');
  assert.match(
    drifted.action === 'refuse' ? drifted.reason : '',
    /does not match the shipped base schema \(unexpected public\.ml_gold_paths\)/,
  );

  const partialRelations = new Set([...shippedBaseSchema.relations].filter((name) => name !== 'public.nodes'));
  const partial = decideLedgerBootstrap({
    approval,
    baseSchema: shippedBaseSchema,
    database: { schemas: new Set(['public']), relations: partialRelations },
  });
  assert.equal(partial.action, 'refuse');
  assert.match(
    partial.action === 'refuse' ? partial.reason : '',
    /missing public\.nodes/,
  );

  const extraSchema = decideLedgerBootstrap({
    approval,
    baseSchema: shippedBaseSchema,
    database: {
      schemas: new Set(['public', 'core']),
      relations: shippedBaseSchema.relations,
    },
  });
  assert.equal(extraSchema.action, 'refuse');
  assert.match(
    extraSchema.action === 'refuse' ? extraSchema.reason : '',
    /unexpected schema\(s\) core/,
  );
});

test('an empty database needs no approval, and an unverifiable bootstrap is refused', () => {
  assert.deepEqual(
    decideLedgerBootstrap({
      approval: '',
      baseSchema: shippedBaseSchema,
      database: { schemas: new Set(['public']), relations: new Set() },
    }),
    { action: 'initialise-empty' },
  );
  const unverifiable = decideLedgerBootstrap({
    approval: BASE_SCHEMA_BOOTSTRAP_APPROVAL,
    baseSchema: null,
    database: { schemas: new Set(['public']), relations: shippedBaseSchema.relations },
  });
  assert.equal(unverifiable.action, 'refuse');
  assert.match(
    unverifiable.action === 'refuse' ? unverifiable.reason : '',
    /base schema is unavailable in this image/,
  );
});
