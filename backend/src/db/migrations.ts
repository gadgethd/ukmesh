import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import type pg from 'pg';
import { resolveDbAssetPath } from './assets.js';

const MIGRATIONS_TABLE = 'schema_migrations';
const MIGRATION_COMPATIBILITY_TABLE = 'schema_migration_compatibility';
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
const EMPTY_DATABASE_SUPERSESSIONS = new Map<string, string>([
  ['011_invalidate_pre_visibility_path_cache.sql', '044_health_current_remove_path_history.sql'],
  ['015_public_visibility_generation.sql', '044_health_current_remove_path_history.sql'],
  ['016_private_prefixes.sql', '026_private_visibility_schema.sql'],
]);

// These filenames predate prefix enforcement and have both shipped. They remain
// explicit so a fresh database and an existing ledger see the same history,
// while every newly introduced duplicate prefix fails closed.
const LEGACY_DUPLICATE_PREFIXES = new Map<string, ReadonlySet<string>>([
  ['016', new Set([
    '016_private_prefixes.sql',
    '016_stale_mqtt_observer_cleanup.sql',
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

export async function runMigrations(pool: pg.Pool): Promise<string[]> {
  const migrations = loadMigrationFiles();
  if (migrations.length < 1) return [];

  const client = await pool.connect();
  let locked = false;
  try {
    await client.query(
      'SELECT pg_advisory_lock(hashtext(current_database()), hashtext($1))',
      [MIGRATION_LOCK_NAME],
    );
    locked = true;

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
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

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
        const contents = await client.query<{ has_rows: boolean }>(`
          SELECT
            EXISTS (SELECT 1 FROM nodes LIMIT 1)
            OR EXISTS (SELECT 1 FROM packets LIMIT 1) AS has_rows
        `);
        if (contents.rows[0]?.has_rows) {
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
