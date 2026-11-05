import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import {
  BASE_SCHEMA_BOOTSTRAP_APPROVAL,
  BASE_SCHEMA_BOOTSTRAP_ENV,
  loadMigrationFiles,
  runMigrations,
} from './migrations.js';

// The CI smoke seeds a fresh volume with `schema/base.sql` and then runs
// `db-migrate`. That database is non-empty and has no ledger, which the runner
// must refuse for any production-like database — and accept only for the
// explicit, shape-verified base-schema bootstrap.
const endpoint = process.env.TEST_INGEST_DATABASE_URL;
const baseSql = readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8');
const allMigrations = loadMigrationFiles();

test('a fresh base-schema volume is refused without approval and bootstrapped with it', {
  skip: endpoint ? false : 'owned PostgreSQL required',
  timeout: 120000,
}, async (t) => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const admin = new pg.Pool({ connectionString: endpoint });
  const suffix = `${process.pid}_${Date.now()}`;
  const created: string[] = [];

  async function scratchDatabase(label: string): Promise<string> {
    const name = `${label}_${suffix}`;
    await admin.query(`CREATE DATABASE ${name}`);
    created.push(name);
    const url = new URL(endpoint!);
    url.pathname = `/${name}`;
    return url.toString();
  }

  async function seededPool(label: string): Promise<pg.Pool> {
    const pool = new pg.Pool({ connectionString: await scratchDatabase(label), max: 2 });
    await pool.query(baseSql);
    return pool;
  }

  const savedApproval = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  t.after(async () => {
    if (savedApproval === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = savedApproval;
    for (const name of created) await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin.end();
  });

  // 1. Seeded volume, no approval: the production assertion still fails closed
  //    and leaves no half-created ledger behind.
  delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  const refused = await seededPool('ci_refused');
  await assert.rejects(
    runMigrations(refused, allMigrations),
    /public\.schema_migrations is missing on a non-empty database; refusing to create a new ledger/,
  );
  assert.equal(
    (await refused.query("SELECT to_regclass('public.schema_migrations')::text AS reg")).rows[0]?.reg,
    null,
  );
  await refused.end();

  // 2. Seeded volume with the explicit approval: the ledger is created and the
  //    replay starts from the base schema baseline. A deterministic subset keeps
  //    this test independent of migrations that assume live-only legacy tables.
  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  const bootstrapped = await seededPool('ci_bootstrap');
  const subset = allMigrations.slice(0, 2);
  assert.equal((await runMigrations(bootstrapped, subset)).length, subset.length);
  const ledger = await bootstrapped.query<{
    name: string;
    checksum_sha256: string;
    execution_mode: string;
  }>('SELECT name, checksum_sha256, execution_mode FROM public.schema_migrations ORDER BY name');
  assert.deepEqual(ledger.rows.map((row) => row.name), subset.map((migration) => migration.name));
  assert.deepEqual(
    ledger.rows.map((row) => row.checksum_sha256),
    subset.map((migration) => migration.checksumSha256),
  );
  assert.deepEqual(ledger.rows.map((row) => row.execution_mode), subset.map(() => 'transactional'));
  assert.ok(
    (await bootstrapped.query<{ reg: string | null }>(
      "SELECT to_regclass('public.schema_migration_compatibility')::text AS reg",
    )).rows[0]?.reg,
  );
  // The approval leaves a permanent, queryable audit row naming the baseline.
  const audit = await bootstrapped.query<{ disposition: string; checksum_sha256: string; replacement_name: string }>(
    `SELECT disposition, checksum_sha256, replacement_name
       FROM public.schema_migration_compatibility
      WHERE migration_name = 'schema/base.sql'`,
  );
  assert.deepEqual(audit.rows, [{
    disposition: 'bootstrap-base-schema',
    checksum_sha256: createHash('sha256')
      .update(readFileSync(new URL('./schema/base.sql', import.meta.url)))
      .digest('hex'),
    replacement_name: 'migration-ledger-origin',
  }]);
  // A second pass is a no-op: the bootstrap is one-way and the ledger is authoritative.
  assert.deepEqual(await runMigrations(bootstrapped, subset), []);
  await bootstrapped.end();

  // 3. A production-like unledgered database (base schema plus one extra
  //    relation) must stay refused even with the approval set.
  const drifted = await seededPool('ci_drift');
  await drifted.query('CREATE TABLE public.ml_gold_paths (id bigserial PRIMARY KEY)');
  await assert.rejects(
    runMigrations(drifted, allMigrations),
    /does not match the shipped base schema \(unexpected public\.ml_gold_paths\)/,
  );
  await drifted.end();
});

test('the supersession probe tolerates relations the base schema has not moved yet', {
  skip: endpoint ? false : 'owned PostgreSQL required',
  timeout: 120000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: endpoint });
  const name = `ci_probe_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE ${name}`);
  const url = new URL(endpoint!);
  url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  const savedApproval = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  t.after(async () => {
    if (savedApproval === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = savedApproval;
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin.end();
  });

  await pool.query(baseSql);
  assert.equal(
    (await pool.query("SELECT to_regclass('core.nodes')::text AS reg")).rows[0]?.reg,
    null,
  );
  const pending = allMigrations.find(
    (migration) => migration.name === '011_invalidate_pre_visibility_path_cache.sql',
  );
  assert.ok(pending, '011 is missing from the migration tree');
  assert.deepEqual(await runMigrations(pool, [pending]), [
    '011_invalidate_pre_visibility_path_cache.sql -> 044_health_current_remove_path_history.sql',
  ]);
  const recorded = await pool.query<{ execution_mode: string }>(
    'SELECT execution_mode FROM public.schema_migrations WHERE name = $1',
    [pending.name],
  );
  assert.deepEqual(recorded.rows, [{ execution_mode: 'superseded-empty' }]);
});
