import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  BASE_SCHEMA_BOOTSTRAP_APPROVAL,
  decideLedgerBootstrap,
  decidePostMoveSupersession,
  loadMigrationFiles,
  migrationChecksum,
  POST_MOVE_SUPERSESSIONS,
  parseBaseSchemaShape,
  validateMigrationPrefixes,
  type PostMoveObservation,
  type PostMoveSupersession,
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

const FENCE_SPEC: PostMoveSupersession = {
  name: '074_prefix_visibility_row_fence.sql',
  replacement: '134_reassert_prefix_visibility_fence.sql',
  postMoveTables: ['ops.public_visibility_state', 'core.private_node_prefixes'],
  displacedViews: ['public.public_visibility_state', 'public.private_node_prefixes'],
  reason: 'test fixture',
};

const PRE_MOVE: Record<string, string | null> = {
  'ops.public_visibility_state': null,
  'core.private_node_prefixes': null,
  'public.public_visibility_state': 'r',
  'public.private_node_prefixes': 'r',
};

const POST_MOVE: Record<string, string | null> = {
  'ops.public_visibility_state': 'r',
  'core.private_node_prefixes': 'r',
  'public.public_visibility_state': 'v',
  'public.private_node_prefixes': 'v',
};

const observe = (
  relations: Record<string, string | null>,
  replacementPresent = true,
): PostMoveObservation => ({ relations, replacementPresent });

test('the post-move fence supersession requires the moved catalog, not a filename', () => {
  // Pre-move: 074's own targets are still tables, so it must run.
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe(PRE_MOVE)).superseded, false);
  // Fully post-move: real tables in ops/core behind read-only public views.
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe(POST_MOVE)).superseded, true);
  // A half move must never be treated as done: either relation still being a
  // table means 074 can install part of the fence and has to run.
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe({
    ...POST_MOVE, 'core.private_node_prefixes': 'r', 'public.private_node_prefixes': 'r',
  })).superseded, false);
  // A missing post-move home is not "moved", however the public name looks.
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe({
    ...POST_MOVE, 'ops.public_visibility_state': null,
  })).superseded, false);
  // A materialized view is still a read-only projection, not the fence target.
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe({
    ...POST_MOVE, 'public.private_node_prefixes': 'm',
  })).superseded, true);
  // Dropping the target without a real post-move home is not a supersession.
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe({
    ...POST_MOVE, 'public.private_node_prefixes': null,
  })).superseded, false);
});

test('a post-move database never skips 074 without its replacement in the tree', () => {
  const decision = decidePostMoveSupersession(FENCE_SPEC, observe(POST_MOVE, false));
  assert.equal(decision.superseded, false);
  assert.match(decision.refusal ?? '', /refusing to skip the fence/);
  assert.equal(decidePostMoveSupersession(FENCE_SPEC, observe(PRE_MOVE, false)).refusal, undefined);
});

test('the reviewed supersession names the shipped fence pair and its evidence', () => {
  const [spec] = POST_MOVE_SUPERSESSIONS;
  assert.deepEqual(POST_MOVE_SUPERSESSIONS.map((entry) => entry.name), [
    '074_prefix_visibility_row_fence.sql',
  ]);
  // 074 ships on main and is merged into this branch by the rebase train, so it
  // is absent here; the replacement must exist, and the pair must sort 074 ->
  // 134 with every move in between.
  const names = loadMigrationFiles().map((migration) => migration.name);
  assert.ok(names.includes(spec!.replacement), `${spec!.replacement} must exist`);
  const merged = [...names, spec!.name].sort((a, b) => a.localeCompare(b));
  assert.ok(merged.indexOf(spec!.name) < merged.indexOf(spec!.replacement));
  const decision = decidePostMoveSupersession(spec!, observe(POST_MOVE));
  assert.equal(decision.superseded, true);
  assert.match(String((decision.evidence as { reason: string }).reason), /moved to ops\/core/);
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
  // Later migrations may legitimately sit above the re-assert (135 does);
  // only its ordering against the P1-4 moves is a durable invariant.
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
