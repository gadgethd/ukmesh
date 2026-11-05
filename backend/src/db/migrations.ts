import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import type pg from 'pg';
import { resolveDbAssetPath } from './assets.js';

const MIGRATIONS_TABLE = 'public.schema_migrations';
const MIGRATION_COMPATIBILITY_TABLE = 'public.schema_migration_compatibility';
const MIGRATION_LOCK_NAME = 'meshcore-analytics-schema-migrations-v1';
const NON_TRANSACTIONAL_DIRECTIVE = '-- meshcore:migration-mode non-transactional';
// 050 originally contained its own BEGIN/COMMIT, which violated runner
// atomicity. Accept only that exact shipped checksum while replacing the
// ledger checksum with the transaction-safe file; every other drift fails.
const APPROVED_MIGRATION_CONTENT_REVISIONS = new Map<string, ReadonlySet<string>>([
  ['050_packet_paths_and_retention.sql', new Set([
    '89cab60d6577d7d0d0865addfed5a36b39d069be549aa04f5839f73005af4885',
  ])],
]);
export const NONEMPTY_PRIVATE_PREFIX_SUPERSESSION_APPROVAL =
  'supersede-016-and-017-with-authoritative-privacy-and-026';
// A fresh volume seeds `base.sql` through the database init hook, so the first
// runner pass legitimately sees a non-empty database with no ledger. That
// bootstrap is never silent: the caller must name it exactly, and the runner
// only accepts it when every relation and schema in the database is exactly
// what the shipped `base.sql` declares. Anything else keeps failing closed.
export const BASE_SCHEMA_BOOTSTRAP_ENV = 'MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL';
export const BASE_SCHEMA_BOOTSTRAP_APPROVAL = 'bootstrap-ledger-from-base-schema';
const BASE_SCHEMA_APPROVED_SCHEMAS: ReadonlySet<string> = new Set(['public']);
// TimescaleDB owns its own catalog/information namespaces; they are created by
// `CREATE EXTENSION timescaledb`, not by base.sql, so both sides ignore them.
const TIMESCALE_SCHEMA_PREFIXES = ['_timescaledb', 'timescaledb_'];
const EMPTY_DATABASE_SUPERSESSIONS = new Map<string, string>([
  ['011_invalidate_pre_visibility_path_cache.sql', '044_health_current_remove_path_history.sql'],
  ['015_public_visibility_generation.sql', '044_health_current_remove_path_history.sql'],
  ['016_private_prefixes.sql', '026_private_visibility_schema.sql'],
]);

// A migration may only be superseded when the schema it targets has provably
// moved. Name matching alone is never enough: the same file must still run on a
// database that has not reached the move, so every requirement below is a fact
// about the live catalog rather than a filename or a ledger entry. The
// replacement must also be present in the tree, because the point of skipping
// the historical file is that the replacement installs the same guarantee.
export interface PostMoveSupersession {
  readonly name: string;
  readonly replacement: string;
  /** Relations that must exist as real tables (post-move homes). */
  readonly postMoveTables: readonly string[];
  /** Relations that must exist only as read-only projections (view or matview). */
  readonly displacedViews: readonly string[];
  readonly reason: string;
}

export const POST_MOVE_SUPERSESSIONS: readonly PostMoveSupersession[] = [
  {
    // 074 installed the row-level prefix fence while the privacy cluster still
    // lived in public. 087/113 moved the tables to ops/core and 108/128 left
    // read-only views at the old names, so ALTER TABLE and CREATE TRIGGER can
    // no longer reach them. 134 re-asserts the same fence against the post-move
    // relations; the run must continue past 074 so it can.
    name: '074_prefix_visibility_row_fence.sql',
    replacement: '134_reassert_prefix_visibility_fence.sql',
    postMoveTables: ['ops.public_visibility_state', 'core.private_node_prefixes'],
    displacedViews: ['public.public_visibility_state', 'public.private_node_prefixes'],
    reason: 'privacy cluster moved to ops/core and the public names are compatibility views',
  },
];

export type RelationShapes = Readonly<Record<string, string | null>>;

export interface PostMoveObservation {
  readonly relations: RelationShapes;
  readonly replacementPresent: boolean;
}

export interface PostMoveDecision {
  readonly superseded: boolean;
  /** Set when the condition matched but the tree cannot deliver the guarantee. */
  readonly refusal?: string;
  readonly evidence: Readonly<Record<string, unknown>>;
}

const TABLE_KINDS: ReadonlySet<string> = new Set(['r', 'p']);
const VIEW_KINDS: ReadonlySet<string> = new Set(['v', 'm']);

/**
 * Decide whether a historical migration is provably superseded by its
 * post-move replacement. Pure: callers pass the observed catalog shapes, so
 * every branch is covered by a unit test and the runner only records what it
 * was told.
 */
export function decidePostMoveSupersession(
  spec: PostMoveSupersession,
  observation: PostMoveObservation,
): PostMoveDecision {
  const tables = spec.postMoveTables.map((relation) => ({
    relation,
    kind: observation.relations[relation] ?? null,
  }));
  const views = spec.displacedViews.map((relation) => ({
    relation,
    kind: observation.relations[relation] ?? null,
  }));
  const tablesMoved = tables.every(({ kind }) => kind !== null && TABLE_KINDS.has(kind));
  const namesDisplaced = views.every(({ kind }) => kind !== null && VIEW_KINDS.has(kind));
  const evidence = {
    migration: spec.name,
    replacement: spec.replacement,
    reason: spec.reason,
    postMoveTables: tables,
    displacedViews: views,
  };

  if (!tablesMoved || !namesDisplaced) {
    return { superseded: false, evidence };
  }
  if (!observation.replacementPresent) {
    return {
      superseded: false,
      refusal: `${spec.name} is pending on a post-move database but its replacement `
        + `${spec.replacement} is not in the tree; refusing to skip the fence`,
      evidence,
    };
  }
  return { superseded: true, evidence };
}

async function observeRelations(
  client: pg.PoolClient,
  relations: readonly string[],
): Promise<RelationShapes> {
  if (relations.length === 0) return {};
  const pairs = relations.map((relation) => {
    const [schema, name] = relation.split('.');
    return { schema, name };
  });
  const result = await client.query<{ schema: string; name: string; kind: string | null }>(
    `SELECT requested.schema, requested.name, c.relkind::text AS kind
       FROM unnest($1::text[], $2::text[]) AS requested(schema, name)
       LEFT JOIN pg_namespace n ON n.nspname = requested.schema
       LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = requested.name`,
    [pairs.map((pair) => pair.schema), pairs.map((pair) => pair.name)],
  );
  const shapes: Record<string, string | null> = {};
  for (const relation of relations) shapes[relation] = null;
  for (const row of result.rows) {
    shapes[`${row.schema}.${row.name}`] = row.kind;
  }
  return shapes;
}

// These filenames predate prefix enforcement and have both shipped. They remain
// explicit so a fresh database and an existing ledger see the same history,
// while every newly introduced duplicate prefix fails closed.
const LEGACY_DUPLICATE_PREFIXES = new Map<string, ReadonlySet<string>>([
  ['016', new Set([
    '016_private_prefixes.sql',
    '016_stale_mqtt_observer_cleanup.sql',
  ])],
  // Both pairs shipped on the live deployment before prefix enforcement was
  // added; the ledger holds both rows and a fresh database must replay the
  // same history. Filenames are the ledger key, so they cannot be renumbered
  // without re-executing applied migrations.
  ['052', new Set([
    '052_node_identity_packets_raw_columns.sql',
    '052_owner_packet_sharing.sql',
  ])],
  ['055', new Set([
    '055_message_tags.sql',
    '055_public_feed.sql',
  ])],
]);

export interface MigrationFile {
  name: string;
  sql: string;
  checksumSha256: string;
  transactional: boolean;
}

function migrationsDir(): string {
  return resolveDbAssetPath('migrations');
}

export function migrationChecksum(contents: string | Buffer): string {
  return createHash('sha256').update(contents).digest('hex');
}

export function validateMigrationPrefixes(names: string[]): void {
  const groups = new Map<string, string[]>();
  for (const name of names) {
    const match = /^(\d+)_.*\.sql$/.exec(name);
    if (!match?.[1]) {
      throw new Error(`migration filename must use a numeric prefix: ${name}`);
    }
    const values = groups.get(match[1]) ?? [];
    values.push(name);
    groups.set(match[1], values);
  }

  for (const [prefix, values] of groups) {
    if (values.length < 2) continue;
    const expected = LEGACY_DUPLICATE_PREFIXES.get(prefix);
    const actual = new Set(values);
    const isExactLegacySet = expected
      && expected.size === actual.size
      && [...expected].every((name) => actual.has(name));
    if (!isExactLegacySet) {
      throw new Error(`duplicate migration prefix ${prefix}: ${values.sort().join(', ')}`);
    }
  }
}

export function loadMigrationFiles(dir = migrationsDir()): MigrationFile[] {
  if (!fs.existsSync(dir)) return [];
  const names = fs.readdirSync(dir)
    .filter((file) => file.endsWith('.sql'))
    .sort((a, b) => a.localeCompare(b));
  validateMigrationPrefixes(names);

  return names.map((name) => {
    const bytes = fs.readFileSync(path.join(dir, name));
    const sql = bytes.toString('utf8').trim();
    const transactional = !sql.startsWith(NON_TRANSACTIONAL_DIRECTIVE);
    if (/\b(?:CREATE|DROP|REINDEX)\s+INDEX\s+CONCURRENTLY\b/i.test(sql) && transactional) {
      throw new Error(
        `${name} uses CONCURRENTLY without the audited non-transactional directive`,
      );
    }
    return {
      name,
      sql,
      checksumSha256: migrationChecksum(bytes),
      transactional,
    };
  });
}

export interface DatabaseShape {
  schemas: ReadonlySet<string>;
  /** Fully qualified `<schema>.<relation>` names for tables, views and materialized views. */
  relations: ReadonlySet<string>;
}

export type LedgerBootstrapDecision =
  | { action: 'initialise-empty' }
  | { action: 'initialise-from-base-schema' }
  | { action: 'refuse'; reason: string };

function isTimescaleSchema(schema: string): boolean {
  return TIMESCALE_SCHEMA_PREFIXES.some((prefix) => schema.startsWith(prefix));
}

/**
 * Relations and schemas declared by the shipped base schema. Parsing the DDL
 * keeps the guard tied to the file that is actually applied to a fresh volume,
 * so it cannot silently drift from an expected-name list maintained by hand.
 */
export function parseBaseSchemaShape(sql: string): DatabaseShape {
  const relations = new Set<string>();
  const pattern =
    /^[ \t]*CREATE[ \t]+(?:TABLE|MATERIALIZED[ \t]+VIEW|VIEW)[ \t]+(?:IF[ \t]+NOT[ \t]+EXISTS[ \t]+)?(?:public\.)?"?([A-Za-z_][A-Za-z0-9_]*)"?/gim;
  for (const match of sql.matchAll(pattern)) {
    const name = match[1];
    if (!name) continue;
    relations.add(`public.${name.toLowerCase()}`);
  }
  return { schemas: BASE_SCHEMA_APPROVED_SCHEMAS, relations };
}

function describeNames(names: string[], limit = 8): string {
  const shown = names.slice(0, limit).join(', ');
  return names.length > limit ? `${shown}, +${names.length - limit} more` : shown;
}

/**
 * Decide what an absent canonical ledger means. Called only when
 * `public.schema_migrations` does not exist, so the caller already knows the
 * database is either empty (create a ledger) or seeded from the base schema
 * (require the exact bootstrap approval and verify the shape).
 */
export function decideLedgerBootstrap(input: {
  approval: string;
  baseSchema: DatabaseShape | null;
  database: DatabaseShape;
}): LedgerBootstrapDecision {
  const { approval, baseSchema, database } = input;
  if (database.relations.size === 0) return { action: 'initialise-empty' };

  const refusal =
    'public.schema_migrations is missing on a non-empty database; '
    + 'refusing to create a new ledger or replay historical migrations';
  if (approval !== BASE_SCHEMA_BOOTSTRAP_APPROVAL) {
    return {
      action: 'refuse',
      reason: `${refusal} (only a deliberate, documented base-schema bootstrap via `
        + `${BASE_SCHEMA_BOOTSTRAP_ENV} may initialise one)`,
    };
  }
  if (!baseSchema) {
    return {
      action: 'refuse',
      reason: `${refusal} (the shipped base schema is unavailable in this image, `
        + 'so the bootstrap cannot be verified)',
    };
  }

  const missing = [...baseSchema.relations].filter((name) => !database.relations.has(name)).sort();
  const unexpected = [...database.relations]
    .filter((name) => !baseSchema.relations.has(name))
    .sort();
  const unexpectedSchemas = [...database.schemas]
    .filter((schema) => !baseSchema.schemas.has(schema))
    .sort();
  if (missing.length > 0 || unexpected.length > 0 || unexpectedSchemas.length > 0) {
    const differences = [
      missing.length > 0 ? `missing ${describeNames(missing)}` : '',
      unexpected.length > 0 ? `unexpected ${describeNames(unexpected)}` : '',
      unexpectedSchemas.length > 0 ? `unexpected schema(s) ${describeNames(unexpectedSchemas)}` : '',
    ].filter(Boolean);
    return {
      action: 'refuse',
      reason: `${refusal}: the database does not match the shipped base schema `
        + `(${differences.join('; ')})`,
    };
  }
  return { action: 'initialise-from-base-schema' };
}

interface BaseSchemaBaseline {
  shape: DatabaseShape;
  checksumSha256: string;
}

let cachedBaseSchema: BaseSchemaBaseline | null | undefined;

function loadBaseSchema(): BaseSchemaBaseline | null {
  if (cachedBaseSchema !== undefined) return cachedBaseSchema;
  try {
    const bytes = fs.readFileSync(path.join(resolveDbAssetPath('schema'), 'base.sql'));
    const parsed = parseBaseSchemaShape(bytes.toString('utf8'));
    cachedBaseSchema = parsed.relations.size > 0
      ? { shape: parsed, checksumSha256: migrationChecksum(bytes) }
      : null;
  } catch {
    cachedBaseSchema = null;
  }
  return cachedBaseSchema;
}

// A database's authored rows may live in the pre-move `public` relations or in
// the current `core`/`mesh` ones, depending on how far its history has
// progressed. Probes must never assume a relation exists: at the point some
// migrations run, the later move has not happened yet.
const AUTHORITATIVE_ROW_RELATIONS = [
  'core.nodes',
  'mesh.packets',
  'public.nodes',
  'public.packets',
] as const;

async function databaseHasAuthoredRows(client: pg.PoolClient): Promise<boolean> {
  const present = await client.query<{ name: string }>(
    'SELECT name FROM unnest($1::text[]) AS name WHERE to_regclass(name) IS NOT NULL',
    [[...AUTHORITATIVE_ROW_RELATIONS]],
  );
  for (const row of present.rows) {
    // The identifier is re-checked against the fixed allowlist before it is
    // interpolated; it never comes from user input.
    if (!(AUTHORITATIVE_ROW_RELATIONS as readonly string[]).includes(row.name)) continue;
    const probe = await client.query<{ has_rows: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM ${row.name} LIMIT 1) AS has_rows`,
    );
    if (probe.rows[0]?.has_rows) return true;
  }
  return false;
}

async function readDatabaseShape(client: pg.PoolClient): Promise<DatabaseShape> {
  const relations = await client.query<{ schema: string; relation: string }>(`
    SELECT n.nspname AS schema, c.relname AS relation
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND n.nspname !~ '^pg_'
  `);
  const schemas = await client.query<{ schema: string }>(`
    SELECT nspname AS schema
    FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND nspname !~ '^pg_'
  `);
  return {
    schemas: new Set(
      schemas.rows.map((row) => row.schema).filter((schema) => !isTimescaleSchema(schema)),
    ),
    relations: new Set(
      relations.rows
        .filter((row) => !isTimescaleSchema(row.schema))
        .map((row) => `${row.schema}.${row.relation}`),
    ),
  };
}

async function assertCanonicalLedger(
  client: pg.PoolClient,
): Promise<'ledger-present' | 'initialise-empty' | 'initialise-from-base-schema'> {
  const shadows = await client.query<{ schema: string }>(`
    SELECT n.nspname AS schema
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'schema_migrations'
      AND c.relkind IN ('r', 'p')
      AND n.nspname <> 'public'
    ORDER BY n.nspname
  `);
  if (shadows.rows.length > 0) {
    throw new Error(
      `shadow migration ledger(s) found in: ${shadows.rows.map((row) => row.schema).join(', ')}; `
      + 'refusing to run against an ambiguous ledger',
    );
  }

  const canonical = await client.query<{ reg: string | null }>(
    "SELECT to_regclass('public.schema_migrations')::text AS reg",
  );
  if (canonical.rows[0]?.reg) return 'ledger-present';

  const database = await readDatabaseShape(client);
  const baseline = loadBaseSchema();
  const decision = decideLedgerBootstrap({
    approval: String(process.env[BASE_SCHEMA_BOOTSTRAP_ENV] ?? '').trim(),
    baseSchema: baseline?.shape ?? null,
    database,
  });
  if (decision.action === 'refuse') throw new Error(decision.reason);
  if (decision.action === 'initialise-from-base-schema') {
    console.warn(
      '[db-migrate] approved base-schema bootstrap: the database matches the shipped '
      + `base schema (${database.relations.size} relations, no ledger); creating `
      + 'public.schema_migrations and replaying every migration from the baseline',
    );
  }
  return decision.action;
}

export async function runMigrations(
  pool: pg.Pool,
  // Tests drive a deterministic subset; production always uses the shipped tree.
  migrations: readonly MigrationFile[] = loadMigrationFiles(),
): Promise<string[]> {
  if (migrations.length < 1) return [];

  const client = await pool.connect();
  let locked = false;
  try {
    await client.query(
      'SELECT pg_advisory_lock(hashtext(current_database()), hashtext($1))',
      [MIGRATION_LOCK_NAME],
    );
    locked = true;

    const ledgerState = await assertCanonicalLedger(client);

    await client.query(`
      CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
        name TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        checksum_sha256 TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'transactional'
      )
    `);
    await client.query(
      `ALTER TABLE ${MIGRATIONS_TABLE}
       ADD COLUMN IF NOT EXISTS checksum_sha256 TEXT`,
    );
    await client.query(
      `ALTER TABLE ${MIGRATIONS_TABLE}
       ADD COLUMN IF NOT EXISTS execution_mode TEXT NOT NULL DEFAULT 'transactional'`,
    );
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${MIGRATION_COMPATIBILITY_TABLE} (
        migration_name TEXT PRIMARY KEY,
        checksum_sha256 TEXT NOT NULL,
        disposition TEXT NOT NULL,
        replacement_name TEXT,
        evidence JSONB,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    // Audit rows written by earlier runner versions predate structured evidence.
    await client.query(
      `ALTER TABLE ${MIGRATION_COMPATIBILITY_TABLE}
       ADD COLUMN IF NOT EXISTS evidence JSONB`,
    );

    if (ledgerState === 'initialise-from-base-schema') {
      // Permanent, queryable record of the approved bootstrap: which base.schema
      // baseline this ledger was initialised over, verified by checksum.
      const baseline = loadBaseSchema();
      if (baseline) {
        await client.query(
          `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
            (migration_name, checksum_sha256, disposition, replacement_name)
           VALUES ('schema/base.sql', $1, 'bootstrap-base-schema', 'migration-ledger-origin')
           ON CONFLICT (migration_name) DO UPDATE SET
             checksum_sha256 = EXCLUDED.checksum_sha256,
             disposition = EXCLUDED.disposition,
             replacement_name = EXCLUDED.replacement_name,
             recorded_at = NOW()`,
          [baseline.checksumSha256],
        );
      }
    }

    const appliedResult = await client.query<{
      name: string;
      checksum_sha256: string | null;
    }>(
      `SELECT name, checksum_sha256 FROM ${MIGRATIONS_TABLE}`,
    );
    const applied = new Map(
      appliedResult.rows.map((row) => [row.name, row.checksum_sha256]),
    );

    for (const migration of migrations) {
      const recorded = applied.get(migration.name);
      if (recorded === undefined) continue;
      if (recorded === null) {
        await client.query(
          `UPDATE ${MIGRATIONS_TABLE}
           SET checksum_sha256 = $2
           WHERE name = $1 AND checksum_sha256 IS NULL`,
          [migration.name, migration.checksumSha256],
        );
        continue;
      }
      if (recorded !== migration.checksumSha256) {
        if (APPROVED_MIGRATION_CONTENT_REVISIONS.get(migration.name)?.has(recorded)) {
          await client.query('BEGIN');
          try {
            await client.query(
              `UPDATE ${MIGRATIONS_TABLE}
                  SET checksum_sha256 = $2
                WHERE name = $1 AND checksum_sha256 = $3`,
              [migration.name, migration.checksumSha256, recorded],
            );
            await client.query(
              `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
                (migration_name, checksum_sha256, disposition, replacement_name)
               VALUES ($1, $2, 'content-revision', $3)
               ON CONFLICT (migration_name) DO UPDATE SET
                 checksum_sha256 = EXCLUDED.checksum_sha256,
                 disposition = EXCLUDED.disposition,
                 replacement_name = EXCLUDED.replacement_name,
                 recorded_at = NOW()`,
              [migration.name, migration.checksumSha256, `replaces:${recorded}`],
            );
            await client.query('COMMIT');
          } catch (error) {
            await client.query('ROLLBACK');
            throw error;
          }
          applied.set(migration.name, migration.checksumSha256);
          continue;
        }
        throw new Error(
          `migration checksum drift for ${migration.name}: `
          + `database=${recorded} repository=${migration.checksumSha256}`,
        );
      }
    }

    const executed: string[] = [];
    for (const migration of migrations) {
      if (applied.has(migration.name) || !migration.sql) continue;

      const replacement = EMPTY_DATABASE_SUPERSESSIONS.get(migration.name);
      if (replacement) {
        const hasAuthoredRows = await databaseHasAuthoredRows(client);
        if (hasAuthoredRows) {
          const approval = String(
            process.env['MIGRATION_016_PRIVATE_PREFIXES_APPROVAL'] ?? '',
          ).trim();
          const hasLegacySibling = applied.has('016_stale_mqtt_observer_cleanup.sql');
          const materialization = migrations.find(
            (candidate) => candidate.name === '017_packet_visibility_materialization.sql',
          );
          const hasSchemaReplacement = migrations.some(
            (candidate) => candidate.name === replacement,
          );
          if (
            migration.name !== '016_private_prefixes.sql'
            || approval !== NONEMPTY_PRIVATE_PREFIX_SUPERSESSION_APPROVAL
            || !hasLegacySibling
            || !materialization
            || !hasSchemaReplacement
          ) {
            throw new Error(
              `${migration.name} is pending on a non-empty database; `
              + `follow the reviewed supersession/backfill runbook and supply its exact approval `
              + `instead of auto-skipping it`,
            );
          }

          const replacementChain =
            '026_private_visibility_schema.sql+runtime-authoritative-privacy-sql';
          await client.query('BEGIN');
          try {
            await client.query(
              `INSERT INTO ${MIGRATIONS_TABLE}
                (name, checksum_sha256, execution_mode)
               VALUES ($1, $2, 'superseded-existing')`,
              [migration.name, migration.checksumSha256],
            );
            await client.query(
              `INSERT INTO ${MIGRATIONS_TABLE}
                (name, checksum_sha256, execution_mode)
               VALUES ($1, $2, 'superseded-existing')`,
              [materialization.name, materialization.checksumSha256],
            );
            await client.query(
              `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
                (migration_name, checksum_sha256, disposition, replacement_name)
               VALUES ($1, $2, 'superseded-existing', $3)
               ON CONFLICT (migration_name) DO UPDATE SET
                 checksum_sha256 = EXCLUDED.checksum_sha256,
                 disposition = EXCLUDED.disposition,
                 replacement_name = EXCLUDED.replacement_name,
                 recorded_at = NOW()`,
              [migration.name, migration.checksumSha256, replacementChain],
            );
            await client.query(
              `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
                (migration_name, checksum_sha256, disposition, replacement_name)
               VALUES ($1, $2, 'superseded-existing', $3)
               ON CONFLICT (migration_name) DO UPDATE SET
                 checksum_sha256 = EXCLUDED.checksum_sha256,
                 disposition = EXCLUDED.disposition,
                 replacement_name = EXCLUDED.replacement_name,
                 recorded_at = NOW()`,
              [materialization.name, materialization.checksumSha256, replacementChain],
            );
            await client.query('COMMIT');
          } catch (error) {
            await client.query('ROLLBACK');
            throw error;
          }
          applied.set(materialization.name, materialization.checksumSha256);
          executed.push(`${migration.name} -> ${replacementChain}`);
          continue;
        }
        await client.query('BEGIN');
        try {
          await client.query(
            `INSERT INTO ${MIGRATIONS_TABLE}
              (name, checksum_sha256, execution_mode)
             VALUES ($1, $2, 'superseded-empty')`,
            [migration.name, migration.checksumSha256],
          );
          await client.query(
            `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
              (migration_name, checksum_sha256, disposition, replacement_name)
             VALUES ($1, $2, 'superseded-empty', $3)
             ON CONFLICT (migration_name) DO UPDATE SET
               checksum_sha256 = EXCLUDED.checksum_sha256,
               disposition = EXCLUDED.disposition,
               replacement_name = EXCLUDED.replacement_name,
               recorded_at = NOW()`,
            [migration.name, migration.checksumSha256, replacement],
          );
          await client.query('COMMIT');
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        }
        executed.push(`${migration.name} -> ${replacement}`);
        continue;
      }

      const postMove = POST_MOVE_SUPERSESSIONS.find((spec) => spec.name === migration.name);
      if (postMove) {
        const relations = await observeRelations(client, [
          ...postMove.postMoveTables,
          ...postMove.displacedViews,
        ]);
        const decision = decidePostMoveSupersession(postMove, {
          relations,
          replacementPresent: migrations.some(
            (candidate) => candidate.name === postMove.replacement,
          ),
        });
        if (decision.refusal) throw new Error(decision.refusal);
        if (decision.superseded) {
          // The audit row and the ledger row must agree, so both are written in
          // one transaction: a database can never claim to have skipped the
          // fence without also recording the schema shape that justified it.
          await client.query('BEGIN');
          try {
            await client.query(
              `INSERT INTO ${MIGRATIONS_TABLE}
                (name, checksum_sha256, execution_mode)
               VALUES ($1, $2, 'superseded-post-move')`,
              [migration.name, migration.checksumSha256],
            );
            await client.query(
              `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
                (migration_name, checksum_sha256, disposition, replacement_name, evidence)
               VALUES ($1, $2, 'superseded-post-move', $3, $4)
               ON CONFLICT (migration_name) DO UPDATE SET
                 checksum_sha256 = EXCLUDED.checksum_sha256,
                 disposition = EXCLUDED.disposition,
                 replacement_name = EXCLUDED.replacement_name,
                 evidence = EXCLUDED.evidence,
                 recorded_at = NOW()`,
              [
                migration.name,
                migration.checksumSha256,
                postMove.replacement,
                JSON.stringify(decision.evidence),
              ],
            );
            await client.query('COMMIT');
          } catch (error) {
            await client.query('ROLLBACK');
            throw error;
          }
          console.log(
            `[db-migrate] ${migration.name} superseded by ${postMove.replacement}: `
            + `${postMove.reason}`,
          );
          executed.push(
            `${migration.name} -> ${postMove.replacement} (superseded-post-move)`,
          );
          continue;
        }
      }

      if (migration.transactional) {
        try {
          await client.query('BEGIN');
          await client.query(migration.sql);
          await client.query(
            `INSERT INTO ${MIGRATIONS_TABLE}
              (name, checksum_sha256, execution_mode)
             VALUES ($1, $2, 'transactional')`,
            [migration.name, migration.checksumSha256],
          );
          await client.query('COMMIT');
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        }
      } else {
        // PostgreSQL forbids operations such as CREATE INDEX CONCURRENTLY in a
        // transaction. The global advisory lock still prevents a second runner;
        // idempotent SQL is required because execution and ledger insertion
        // cannot be one atomic transaction in this audited mode.
        await client.query(migration.sql);
        await client.query(
          `INSERT INTO ${MIGRATIONS_TABLE}
            (name, checksum_sha256, execution_mode)
           VALUES ($1, $2, 'non-transactional')`,
          [migration.name, migration.checksumSha256],
        );
      }
      executed.push(migration.name);
    }

    for (const migrationName of [
      '016_private_prefixes.sql',
      '016_stale_mqtt_observer_cleanup.sql',
      '018_api_performance_indexes.sql',
    ]) {
      const migration = migrations.find((candidate) => candidate.name === migrationName);
      if (!migration) continue;
      await client.query(
        `INSERT INTO ${MIGRATION_COMPATIBILITY_TABLE}
          (migration_name, checksum_sha256, disposition, replacement_name)
         SELECT name, checksum_sha256, execution_mode, NULL
         FROM ${MIGRATIONS_TABLE}
         WHERE name = $1
         ON CONFLICT (migration_name) DO NOTHING`,
        [migrationName],
      );
    }
    return executed;
  } finally {
    if (locked) {
      await client.query(
        'SELECT pg_advisory_unlock(hashtext(current_database()), hashtext($1))',
        [MIGRATION_LOCK_NAME],
      ).catch((error: unknown) => {
        console.error(
          '[db-migrate] failed to release advisory lock:',
          error instanceof Error ? error.message : error,
        );
      });
    }
    client.release();
  }
}
