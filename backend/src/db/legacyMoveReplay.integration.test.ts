import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import {
  BASE_SCHEMA_BOOTSTRAP_APPROVAL,
  BASE_SCHEMA_BOOTSTRAP_ENV,
  loadMigrationFiles,
  runMigrations,
} from './migrations.js';

// The P1-4 moves re-home relations that no migration in this tree creates:
// `message_tags_local`, the `path_sim_*` simulation tables, the derived
// analytics tables and the privacy cluster all exist only on databases
// provisioned before the move. A database rebuilt from the shipped base schema
// has none of them, so an unguarded `ALTER TABLE ... SET SCHEMA` stops the band
// at 078 and no fresh volume can ever reach head. These two tests pin both
// directions: the band completes without the legacy relations, and the guarded
// moves still do their full job when the relations are there.
const endpoint = process.env.TEST_INGEST_DATABASE_URL;
const baseSql = readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8');
const roleBootstrap = [
  readFileSync(new URL('../../../scripts/bootstrap-backend-roles.sql', import.meta.url), 'utf8'),
  readFileSync(new URL('../../../scripts/bootstrap-reader-roles.sql', import.meta.url), 'utf8'),
];
const allMigrations = loadMigrationFiles();
const HEAD = '135_packet_identity_backfill_chunks.sql';
// Everything before 078 is unaffected by this change; splitting the band there
// lets the second test insert the legacy relations at the point they would have
// existed on a live database.
const firstBlockEnd = allMigrations.findIndex((migration) => migration.name.startsWith('078_'));
const firstBlock = allMigrations.slice(0, firstBlockEnd);

// Stand-ins for the pre-move relations. Their exact columns do not matter to a
// schema move; the index, comment and grant do, because a move has to carry
// them across and a rewrite that dropped them would be a silent regression.
const legacyRelations = `
CREATE TABLE public.message_tags_local (
  packet_hash TEXT PRIMARY KEY,
  packet_time TIMESTAMPTZ NOT NULL,
  network     TEXT NOT NULL DEFAULT 'ukmesh',
  tags        JSONB NOT NULL
);
CREATE INDEX message_tags_local_packet_time_idx ON public.message_tags_local (packet_time DESC);
COMMENT ON TABLE public.message_tags_local IS 'legacy local-only classifier cache';

CREATE TABLE public.path_sim_evolution_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  generation BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE public.path_sim_population (
  id BIGSERIAL PRIMARY KEY,
  genome JSONB NOT NULL
);
CREATE TABLE public.path_simulation_runs (
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status TEXT NOT NULL DEFAULT 'running'
);
CREATE TABLE public.last_hop_snr_daily (
  day DATE NOT NULL,
  observer TEXT NOT NULL,
  samples BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (day, observer)
);
CREATE TABLE public.multihop_paths_weekly (
  week DATE NOT NULL,
  path_hash TEXT NOT NULL,
  observations BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (week, path_hash)
);
GRANT SELECT ON public.message_tags_local TO corescope_ro;
GRANT SELECT ON public.last_hop_snr_daily TO ukmesh_planner_reader;
`;

interface Harness {
  pool: pg.Pool;
  database: string;
  admin: pg.Pool;
}

async function withDatabase(
  t: { after: (fn: () => Promise<void>) => void },
  label: string,
): Promise<Harness> {
  const admin = new pg.Pool({ connectionString: endpoint, max: 2 });
  const database = `${label}_${process.pid}_${Date.now()}`.toLowerCase();
  await admin.query(`CREATE DATABASE ${database}`);
  const url = new URL(endpoint!);
  url.pathname = `/${database}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    await admin.end();
  });
  return { pool, database, admin };
}

/** The CI init hook: shipped base schema, then the shipped operator role scripts. */
async function seedFreshVolume(harness: Harness): Promise<void> {
  await harness.pool.query(baseSql);
  for (const script of roleBootstrap) await harness.pool.query(script);
}

test('a fresh base-schema database replays the whole band to head 135', {
  skip: endpoint ? false : 'owned PostgreSQL required',
  timeout: 300000,
}, async (t) => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const savedApproval = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  t.after(() => {
    if (savedApproval === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = savedApproval;
  });

  const harness = await withDatabase(t, 'fresh_replay');
  await seedFreshVolume(harness);

  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  const applied = await runMigrations(harness.pool, allMigrations);

  // Every shipped migration is either executed or explicitly superseded; the
  // band may not stop early on a missing legacy relation.
  assert.equal(applied.length, allMigrations.length);
  const ledger = await harness.pool.query<{ name: string; execution_mode: string }>(
    'SELECT name, execution_mode FROM public.schema_migrations ORDER BY name',
  );
  assert.equal(ledger.rows.length, allMigrations.length);
  assert.equal(ledger.rows.at(-1)?.name, HEAD);
  assert.equal(Math.max(...ledger.rows.map((row) => Number(row.name.slice(0, 3)))), 135);

  // Nothing is invented to satisfy a move, and the export schema 087 needs is
  // created rather than assumed.
  const shape = await harness.pool.query<Record<string, string | null>>(`
    SELECT to_regclass('public.message_tags_local')::text AS legacy_tags,
           to_regclass('mesh.message_tags_local')::text AS moved_tags,
           to_regclass('ops.path_simulation_runs')::text AS moved_ops,
           to_regclass('analytics.multihop_paths_weekly')::text AS moved_analytics,
           to_regclass('ops.public_visibility_state')::text AS moved_visibility,
           to_regclass('planner_export.visibility_fence_v1')::text AS export_view
  `);
  assert.deepEqual(shape.rows[0], {
    legacy_tags: null,
    moved_tags: null,
    moved_ops: null,
    moved_analytics: null,
    moved_visibility: 'ops.public_visibility_state',
    export_view: 'planner_export.visibility_fence_v1',
  });
  // The export view is readable, so its source relation really is in place.
  assert.equal((await harness.pool.query('SELECT count(*)::int AS n FROM planner_export.visibility_fence_v1')).rows[0]?.n, 1);

  // A second pass is a no-op: the replay is idempotent once the ledger is full.
  assert.deepEqual(await runMigrations(harness.pool, allMigrations), []);
});

test('the guarded moves still move the legacy relations and carry their grants', {
  skip: endpoint ? false : 'owned PostgreSQL required',
  timeout: 300000,
}, async (t) => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const savedApproval = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  t.after(() => {
    if (savedApproval === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = savedApproval;
  });

  const harness = await withDatabase(t, 'legacy_moves');
  await seedFreshVolume(harness);

  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  await runMigrations(harness.pool, firstBlock);

  // The legacy relations appear exactly as they do on a live database: the
  // base-schema bootstrap has already been recorded, so the runner no longer
  // sees them as drift.
  await harness.pool.query(legacyRelations);
  // An operator-provisioned export schema must survive 087 untouched.
  await harness.pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'legacy_planner_owner') THEN
        CREATE ROLE legacy_planner_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
      END IF;
    END
    $$;
    CREATE SCHEMA IF NOT EXISTS planner_export AUTHORIZATION legacy_planner_owner;
    GRANT USAGE ON SCHEMA planner_export TO ukmesh_planner_reader;
  `);

  const applied = await runMigrations(harness.pool, allMigrations);
  assert.equal(applied.length, allMigrations.length - firstBlock.length);

  const ledger = await harness.pool.query<{ name: string }>(
    'SELECT name FROM public.schema_migrations ORDER BY name',
  );
  assert.equal(ledger.rows.at(-1)?.name, HEAD);

  // Every guarded move actually ran: no public.* source names remain, and each
  // relation is in its intended schema.
  const moved = await harness.pool.query<Record<string, string | null>>(`
    SELECT to_regclass('public.message_tags_local')::text AS legacy_tags,
           to_regclass('mesh.message_tags_local')::text AS mesh_tags,
           to_regclass('public.path_sim_evolution_state')::text AS legacy_sim_state,
           to_regclass('ops.path_sim_evolution_state')::text AS ops_sim_state,
           to_regclass('public.path_sim_population')::text AS legacy_sim_pop,
           to_regclass('ops.path_sim_population')::text AS ops_sim_pop,
           to_regclass('public.path_simulation_runs')::text AS legacy_sim_runs,
           to_regclass('ops.path_simulation_runs')::text AS ops_sim_runs,
           to_regclass('public.last_hop_snr_daily')::text AS legacy_snr,
           to_regclass('analytics.last_hop_snr_daily')::text AS analytics_snr,
           to_regclass('public.multihop_paths_weekly')::text AS legacy_multihop,
           to_regclass('analytics.multihop_paths_weekly')::text AS analytics_multihop,
           to_regclass('planner_export.visibility_fence_v1')::text AS export_view
  `);
  assert.deepEqual(moved.rows[0], {
    legacy_tags: null,
    mesh_tags: 'mesh.message_tags_local',
    legacy_sim_state: null,
    ops_sim_state: 'ops.path_sim_evolution_state',
    legacy_sim_pop: null,
    ops_sim_pop: 'ops.path_sim_population',
    legacy_sim_runs: null,
    ops_sim_runs: 'ops.path_simulation_runs',
    legacy_snr: null,
    analytics_snr: 'analytics.last_hop_snr_daily',
    legacy_multihop: null,
    analytics_multihop: 'analytics.multihop_paths_weekly',
    export_view: 'planner_export.visibility_fence_v1',
  });
  // ops.public_visibility_state moved with the rest of the cluster.
  assert.equal(
    (await harness.pool.query("SELECT to_regclass('ops.public_visibility_state')::text AS reg")).rows[0]?.reg,
    'ops.public_visibility_state',
  );

  // A move preserves everything attached to the relation: the index, the
  // comment and the grants must all still be there under the new schema.
  const carried = await harness.pool.query<{
    index: string | null;
    comment: string | null;
    acl: string | null;
  }>(`
    SELECT to_regclass('mesh.message_tags_local_packet_time_idx')::text AS index,
           obj_description('mesh.message_tags_local'::regclass, 'pg_class') AS comment,
           (SELECT array_to_string(relacl, ' ') FROM pg_class
             WHERE oid = 'mesh.message_tags_local'::regclass) AS acl
  `);
  assert.equal(carried.rows[0]?.index, 'mesh.message_tags_local_packet_time_idx');
  assert.equal(carried.rows[0]?.comment, 'legacy local-only classifier cache');
  assert.match(carried.rows[0]?.acl ?? '', /corescope_ro=r\//);
  assert.match(
    (await harness.pool.query<{ acl: string }>(
      `SELECT array_to_string(relacl, ' ') AS acl FROM pg_class
        WHERE oid = 'analytics.last_hop_snr_daily'::regclass`,
    )).rows[0]?.acl ?? '',
    /ukmesh_planner_reader=r\//,
  );

  // The provisioned export schema keeps its owner and grants: CREATE SCHEMA IF
  // NOT EXISTS must not re-own or re-grant it.
  const exportSchema = await harness.pool.query<{ owner: string; acl: string }>(`
    SELECT pg_get_userbyid(nspowner) AS owner, array_to_string(nspacl, ' ') AS acl
      FROM pg_namespace WHERE nspname = 'planner_export'
  `);
  assert.equal(exportSchema.rows[0]?.owner, 'legacy_planner_owner');
  assert.match(exportSchema.rows[0]?.acl ?? '', /ukmesh_planner_reader=U\//);
});

test('a database without the legacy relations is never given one', {
  skip: endpoint ? false : 'owned PostgreSQL required',
  timeout: 120000,
}, async (t) => {
  // The guard skips the move; it must not create a stand-in relation to move.
  const harness = await withDatabase(t, 'no_legacy');
  await harness.pool.query('CREATE SCHEMA IF NOT EXISTS ops');
  await harness.pool.query('CREATE SCHEMA IF NOT EXISTS mesh');
  await harness.pool.query('CREATE SCHEMA IF NOT EXISTS analytics');

  for (const name of [
    '078_move_message_tags_local.sql',
    '080_move_ops_unreferenced_tables.sql',
    '081_move_analytics_unreferenced_tables.sql',
  ]) {
    const migration = allMigrations.find((candidate) => candidate.name === name);
    assert.ok(migration, `${name} is missing from the migration tree`);
    await harness.pool.query(migration.sql);
  }

  const absent = await harness.pool.query<{ n: number }>(`
    SELECT count(*)::int AS n
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname IN ('message_tags_local','path_sim_evolution_state',
                         'path_sim_population','path_simulation_runs',
                         'last_hop_snr_daily','multihop_paths_weekly')
  `);
  assert.equal(absent.rows[0]?.n, 0);
});
