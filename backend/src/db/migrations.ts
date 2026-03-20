import fs from 'node:fs';
import path from 'node:path';
import type pg from 'pg';
import { resolveDbAssetPath } from './assets.js';

const MIGRATIONS_TABLE = 'schema_migrations';

function migrationsDir(): string {
  return resolveDbAssetPath('migrations');
}

export async function runMigrations(pool: pg.Pool): Promise<string[]> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  const dir = migrationsDir();
  if (!fs.existsSync(dir)) return [];

  const files = fs.readdirSync(dir)
    .filter((file) => file.endsWith('.sql'))
    .sort((a, b) => a.localeCompare(b));

  if (files.length < 1) return [];

  const appliedResult = await pool.query<{ name: string }>(
    `SELECT name FROM ${MIGRATIONS_TABLE}`,
  );
  const applied = new Set(appliedResult.rows.map((row) => row.name));
  const executed: string[] = [];

  for (const file of files) {
    if (applied.has(file)) continue;
    const sql = fs.readFileSync(path.join(dir, file), 'utf8').trim();
    if (!sql) continue;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query(
        `INSERT INTO ${MIGRATIONS_TABLE} (name) VALUES ($1)`,
        [file],
      );
      await client.query('COMMIT');
      executed.push(file);
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  return executed;
}
