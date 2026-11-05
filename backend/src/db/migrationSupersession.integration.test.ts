import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { loadMigrationFiles, migrationChecksum, runMigrations, type MigrationFile } from './migrations.js';

// 074 installed the prefix row fence while the privacy cluster still lived in
// public. 074 itself ships on main, so it is not part of this branch's tree;
// the merged tree is what has to replay. On a database that already ran
// 087/113 (and 108/128) those public names are read-only compatibility views and
// the historical file cannot execute, while 134 re-asserts the same fence on the
// real post-move relations. The runner must therefore skip 074 only when the
// catalog proves the move happened, record why, and continue the same pass.
//
// The historical file is rebuilt here with 074's name and its exact targets: the
// test owns the fixture, and 134 is the real shipped file.
const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
const fence = '074_prefix_visibility_row_fence.sql';
const replacement = '134_reassert_prefix_visibility_fence.sql';
const replacementMigration = new Map(loadMigrationFiles().map((migration) => [migration.name, migration]))
  .get(replacement) ?? assert.fail(`${replacement} must exist in the migration tree`);

// Minimal stand-in for main's 074: it must fail on a post-move database because
// it ALTERs public.public_visibility_state and puts a row trigger on
// public.private_node_prefixes, exactly like the shipped file.
const HISTORICAL_FENCE_SQL = `
SET LOCAL lock_timeout = '5s';
ALTER TABLE public_visibility_state ADD COLUMN prefix_fence_xid XID8;
DROP TRIGGER IF EXISTS private_node_prefixes_visibility_rows ON private_node_prefixes;
CREATE TRIGGER private_node_prefixes_visibility_rows
AFTER INSERT OR UPDATE OR DELETE ON private_node_prefixes
FOR EACH ROW EXECUTE FUNCTION bump_visibility_for_changed_prefixes();
`.trim();

const historicalFence: MigrationFile = {
  name: fence,
  sql: HISTORICAL_FENCE_SQL,
  checksumSha256: migrationChecksum(HISTORICAL_FENCE_SQL),
  transactional: true,
};

const PRE_MOVE_FIXTURE = `
  CREATE TABLE public.nodes (node_id TEXT PRIMARY KEY);
  CREATE TABLE public.public_visibility_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    generation BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  INSERT INTO public.public_visibility_state (singleton) VALUES (TRUE);
  CREATE TABLE public.packet_visibility_materialization_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    visibility_generation BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  CREATE TABLE public.private_node_prefixes (
    node_id TEXT NOT NULL,
    network TEXT NOT NULL,
    prefix_size_bytes SMALLINT NOT NULL,
    prefix BYTEA NOT NULL
  );
  CREATE FUNCTION public.bump_visibility_for_changed_prefixes()
  RETURNS TRIGGER LANGUAGE plpgsql AS $$
  BEGIN
    UPDATE public_visibility_state SET generation = generation + 1 WHERE singleton;
    RETURN NULL;
  END;
  $$;
`;

const POST_MOVE_FIXTURE = `
  CREATE SCHEMA ops;
  CREATE SCHEMA core;
  CREATE TABLE core.nodes (node_id TEXT PRIMARY KEY);
  CREATE TABLE ops.public_visibility_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    generation BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    prefix_fence_xid XID8
  );
  INSERT INTO ops.public_visibility_state (singleton) VALUES (TRUE);
  CREATE TABLE ops.packet_visibility_materialization_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    visibility_generation BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  CREATE TABLE core.private_node_prefixes (
    node_id TEXT NOT NULL,
    network TEXT NOT NULL,
    prefix_size_bytes SMALLINT NOT NULL,
    prefix BYTEA NOT NULL
  );
  -- 108/128 shape: the public names survive only as read-only projections.
  CREATE VIEW public.public_visibility_state AS SELECT * FROM ops.public_visibility_state;
  CREATE VIEW public.private_node_prefixes AS SELECT * FROM core.private_node_prefixes;
`;

async function withDatabase(
  label: string,
  fixture: string,
  body: (pool: pg.Pool) => Promise<void>,
): Promise<void> {
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `${label}_${process.pid}_${Date.now()}`;
  const url = new URL(endpoint!);
  url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  try {
    await admin.query(`CREATE DATABASE "${name}"`);
    await pool.query(fixture);
    // Every real database already carries the ledger; the runner legitimately
    // refuses to invent one over a non-empty schema. Start from an empty ledger
    // with the shipped columns so both paths replay the same way.
    await pool.query(`CREATE TABLE public.schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      checksum_sha256 TEXT,
      execution_mode TEXT NOT NULL DEFAULT 'transactional'
    )`);
    await body(pool);
  } finally {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  }
}

const fenceFiles: MigrationFile[] = [historicalFence, replacementMigration];

test('074 still applies on a pre-move database and installs the row fence', {
  skip: endpoint ? false : 'owned loopback TEST_INGEST_DATABASE_URL required',
  timeout: 120_000,
}, async () => {
  assert.ok(['127.0.0.1', 'localhost'].includes(new URL(endpoint!).hostname));

  await withDatabase('supersede_pre_move', PRE_MOVE_FIXTURE, async (pool) => {
    // Only the historical fence is pending on this database: the moves it would
    // collide with have not happened yet, so it must run exactly as shipped.
    const executed = await runMigrations(pool, [historicalFence]);
    assert.deepEqual(executed, [fence]);

    const ledger = await pool.query<{ name: string; execution_mode: string }>(
      'SELECT name, execution_mode FROM public.schema_migrations ORDER BY name',
    );
    assert.deepEqual(ledger.rows, [{ name: fence, execution_mode: 'transactional' }]);
    assert.deepEqual(
      (await pool.query(
        'SELECT disposition FROM public.schema_migration_compatibility WHERE migration_name = $1',
        [fence],
      )).rows,
      [],
      'an applied migration is not recorded as superseded',
    );

    // 074 really executed: the column exists and its row trigger fences writes.
    assert.equal(
      (await pool.query(
        `SELECT COUNT(*)::int AS count FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'public_visibility_state'
            AND column_name = 'prefix_fence_xid'`,
      )).rows[0].count,
      1,
    );
    const triggers = await pool.query<{ tgname: string; tgtype: number }>(
      `SELECT tgname, tgtype::int FROM pg_trigger
        WHERE tgrelid = 'public.private_node_prefixes'::regclass AND NOT tgisinternal
        ORDER BY tgname`,
    );
    assert.deepEqual(triggers.rows.map((row) => row.tgname), ['private_node_prefixes_visibility_rows']);
    // bit 0 of tgtype is TRIGGER_TYPE_ROW.
    assert.equal(triggers.rows[0]!.tgtype & 1, 1);
    await pool.query(
      `INSERT INTO public.private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
       VALUES ('AABB', 'ukmesh', 1, '\\xAA')`,
    );
    assert.deepEqual(
      (await pool.query('SELECT generation::text AS generation FROM public.public_visibility_state')).rows,
      [{ generation: '2' }],
    );
  });
});

test('074 is recorded superseded on a post-move database and the run continues to 134', {
  skip: endpoint ? false : 'owned loopback TEST_INGEST_DATABASE_URL required',
  timeout: 120_000,
}, async () => {
  await withDatabase('supersede_post_move', POST_MOVE_FIXTURE, async (pool) => {
    const executed = await runMigrations(pool, fenceFiles);
    assert.deepEqual(executed, [`${fence} -> ${replacement} (superseded-post-move)`, replacement]);

    const ledger = await pool.query<{ name: string; execution_mode: string }>(
      'SELECT name, execution_mode FROM public.schema_migrations ORDER BY name',
    );
    assert.deepEqual(ledger.rows, [
      { name: fence, execution_mode: 'superseded-post-move' },
      { name: replacement, execution_mode: 'transactional' },
    ]);

    // The audit row must say why, not just that something was skipped.
    const audit = await pool.query<{
      disposition: string;
      replacement_name: string;
      evidence: {
        reason: string;
        postMoveTables: { relation: string; kind: string }[];
        displacedViews: { relation: string; kind: string }[];
      } | null;
    }>(
      `SELECT disposition, replacement_name, evidence
         FROM public.schema_migration_compatibility WHERE migration_name = $1`,
      [fence],
    );
    assert.equal(audit.rows.length, 1);
    assert.equal(audit.rows[0]!.disposition, 'superseded-post-move');
    assert.equal(audit.rows[0]!.replacement_name, replacement);
    const evidence = audit.rows[0]!.evidence!;
    assert.match(evidence.reason, /moved to ops\/core/);
    assert.deepEqual(evidence.postMoveTables, [
      { relation: 'ops.public_visibility_state', kind: 'r' },
      { relation: 'core.private_node_prefixes', kind: 'r' },
    ]);
    assert.deepEqual(evidence.displacedViews, [
      { relation: 'public.public_visibility_state', kind: 'v' },
      { relation: 'public.private_node_prefixes', kind: 'v' },
    ]);

    // 134 ran and installed the fence on the canonical relations.
    const triggers = await pool.query<{ tgname: string }>(
      `SELECT tgname FROM pg_trigger
        WHERE tgrelid = 'core.private_node_prefixes'::regclass AND NOT tgisinternal
        ORDER BY tgname`,
    );
    assert.deepEqual(triggers.rows.map((row) => row.tgname), [
      'private_node_prefixes_visibility_delete_rows',
      'private_node_prefixes_visibility_insert_rows',
      'private_node_prefixes_visibility_truncate',
      'private_node_prefixes_visibility_update_rows',
    ]);
    await pool.query(
      `INSERT INTO core.private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
       VALUES ('AABB', 'ukmesh', 1, '\\xAA')`,
    );
    assert.deepEqual(
      (await pool.query('SELECT generation::text AS generation FROM ops.public_visibility_state')).rows,
      [{ generation: '2' }],
    );

    // The ledger is authoritative: a replay supersedes nothing and re-runs nothing.
    assert.deepEqual(await runMigrations(pool, fenceFiles), []);
  });
});

test('a post-move database refuses to skip 074 when its replacement is absent', {
  skip: endpoint ? false : 'owned loopback TEST_INGEST_DATABASE_URL required',
  timeout: 120_000,
}, async () => {
  await withDatabase('supersede_no_replacement', POST_MOVE_FIXTURE, async (pool) => {
    await assert.rejects(
      runMigrations(pool, [historicalFence]),
      /is pending on a post-move database but its replacement .* is not in the tree/,
    );
    assert.deepEqual(
      (await pool.query('SELECT name FROM public.schema_migrations')).rows,
      [],
      'a refused supersession leaves no ledger row behind',
    );
    assert.deepEqual(
      (await pool.query('SELECT migration_name FROM public.schema_migration_compatibility')).rows,
      [],
    );
  });
});

test('the reviewed supersession never fires from a filename alone', () => {
  const source = readFileSync(new URL('./migrations.ts', import.meta.url), 'utf8');
  const unconditional =
    /const EMPTY_DATABASE_SUPERSESSIONS = new Map<string, string>\(\[([\s\S]*?)\]\);/.exec(source);
  assert.ok(unconditional, 'empty-database supersession list must stay explicit');
  assert.doesNotMatch(unconditional![1]!, /074/);
  assert.match(source, /name: '074_prefix_visibility_row_fence\.sql'/);
  assert.match(source, /replacement: '134_reassert_prefix_visibility_fence\.sql'/);
});
