import pg from 'pg';
import { databaseConfig, databaseUrlForRole } from '../platform/config/database.js';
import { runMigrations } from '../db/migrations.js';

const { Pool } = pg;

async function main(): Promise<void> {
  const databaseUrl = databaseUrlForRole('migrate', process.env, process.env['DATABASE_URL']);
  if (!databaseUrl) {
    throw new Error('MESHCORE_MIGRATE_DATABASE_URL or DATABASE_URL is required');
  }

  const pool = new Pool({
    connectionString: databaseUrl,
    application_name: `${databaseConfig.applicationName}-migrate`,
    options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
    max: 1,
    idleTimeoutMillis: databaseConfig.idleTimeoutMs,
    connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
    statement_timeout: 0,
    query_timeout: 0,
  });

  try {
    if (databaseConfig.schema) {
      await pool.query(`CREATE SCHEMA IF NOT EXISTS "${databaseConfig.schema}"`);
    }
    const applied = await runMigrations(pool);
    console.log(
      `[db-migrate] ${applied.length > 0 ? `applied: ${applied.join(', ')}` : 'no pending migrations'}`,
    );
  } finally {
    await pool.end();
  }
}

main().catch((err: unknown) => {
  console.error('[db-migrate] failed:', err instanceof Error ? err.message : err);
  process.exit(1);
});
