import fs from 'node:fs';
import type pg from 'pg';
import {
  BASE_SCHEMA_BOOTSTRAP_APPROVAL,
  BASE_SCHEMA_BOOTSTRAP_ENV,
  runMigrations,
  type MigrationFile,
} from '../migrations.js';

/**
 * The fixture that mirrors how a fresh deployment is built.
 *
 * A production volume is seeded from `schema/base.sql` by the TimescaleDB init
 * hook and the operator role scripts run in the same hook, so the first
 * `db:migrate` legitimately sees a non-empty database with no ledger and with
 * the eight bootstrap roles already present. That is the *only* path the
 * runner's base-schema bootstrap approval exists for, and the approval is not
 * a bypass: the runner still verifies the database matches the shipped
 * `base.sql` exactly before it creates a ledger.
 *
 * Integration suites that create a database, seed `base.sql`, and then replay
 * the migration band are building that same fresh deployment, so they have to
 * satisfy the same two preconditions instead of relying on the runner to
 * create a ledger over an arbitrary non-empty database. Doing it here keeps
 * one copy of the rules: a fixture that forgets either precondition still
 * fails loudly, and no suite can quietly relax the guard for the others.
 *
 * The scripts are the shipped operator bootstrap files, not a test-only copy,
 * so a role that 072_backend_grants.sql requires can never drift out of sync
 * with what the fixtures provision.
 */
const BASE_SCHEMA_URL = new URL('../schema/base.sql', import.meta.url);
const OPERATOR_ROLE_SCRIPT_URLS = [
  new URL('../../../../scripts/bootstrap-backend-roles.sql', import.meta.url),
  new URL('../../../../scripts/bootstrap-reader-roles.sql', import.meta.url),
];

/**
 * The namespaces a fresh deployment ends up with after the P1 move band.
 *
 * Fixture bookkeeping SQL keeps its pre-move spelling (`packets`, `nodes`,
 * `packet_daily_stats`), which the old `public` names can no longer serve: some
 * are gone and the rest are read-only compatibility views that hide private
 * rows, so a fixture that counts private rows through them sees zero. Pointing
 * the fixture pool at the post-move namespaces keeps those reads meaningful
 * without rewriting every assertion. Product SQL is fully qualified and is
 * checked separately by the application-SQL allowlist test, so this cannot hide
 * an unqualified statement in shipped code.
 */
export const POST_MOVE_SEARCH_PATH = '-c search_path=mesh,core,analytics,ops,public';

let rolesProvisioned = false;

function readScript(url: URL): string {
  try {
    return fs.readFileSync(url, 'utf8');
  } catch {
    throw new Error(
      `fixture bootstrap script is missing: ${url.pathname}; run the integration `
      + 'suites from a repository checkout that includes scripts/',
    );
  }
}

/** Seed the shipped base schema, exactly as the database init hook does. */
export async function applyBaseSchema(pool: pg.Pool): Promise<void> {
  await pool.query(readScript(BASE_SCHEMA_URL));
}

/**
 * Run the operator role bootstrap, exactly as the init hook does.
 *
 * `072_backend_grants.sql` fails closed when any of the eight roles is
 * absent, so a database built by a fixture needs them too. Both scripts are
 * idempotent and cluster-wide, so this runs once per process.
 */
export async function provisionOperatorRoles(pool: pg.Pool): Promise<void> {
  if (rolesProvisioned) return;
  for (const url of OPERATOR_ROLE_SCRIPT_URLS) {
    await pool.query(readScript(url));
  }
  rolesProvisioned = true;
}

/**
 * Declare the approved base-schema bootstrap for this process.
 *
 * The value is the exact approval string the runner accepts; the runner still
 * refuses unless the database shape matches `base.sql` relation for relation.
 * Returns a restore function so a suite that asserts the refusal can drop the
 * approval again.
 */
export function approveBaseSchemaBootstrap(): () => void {
  const previous = process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
  process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = BASE_SCHEMA_BOOTSTRAP_APPROVAL;
  return () => {
    if (previous === undefined) delete process.env[BASE_SCHEMA_BOOTSTRAP_ENV];
    else process.env[BASE_SCHEMA_BOOTSTRAP_ENV] = previous;
  };
}

/**
 * Turn a freshly created, empty database into the state a fresh deployment
 * reaches: base schema applied, operator roles present, band replayed.
 */
export async function bootstrapBaseSchemaFixture(
  pool: pg.Pool,
  migrations?: readonly MigrationFile[],
): Promise<string[]> {
  await applyBaseSchema(pool);
  await provisionOperatorRoles(pool);
  const restore = approveBaseSchemaBootstrap();
  try {
    return await runMigrations(pool, migrations);
  } finally {
    restore();
  }
}
