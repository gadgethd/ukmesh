import assert from 'node:assert/strict';
import test from 'node:test';
import { loadMigrationFiles } from './migrations.js';

const TARGET_QUALIFIER = /^(?:public|core|mesh|analytics|archive|ops|pg_catalog|information_schema|timescaledb_information|timescaledb_experimental|_timescaledb|unnest|pg_)/i;
const DML_TARGET = /\b(?:FROM|JOIN|INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-z_][a-z0-9_]*)/gi;

function unqualifiedTargets(sql: string): string[] {
  const body = sql
    .replace(/--[^\n]*/g, '')
    .replace(/'([^']|'')*'/g, "''");
  const found: string[] = [];
  for (const match of body.matchAll(DML_TARGET)) {
    const target = match[1];
    if (!target || TARGET_QUALIFIER.test(target)) continue;
    found.push(target);
  }
  return found;
}

test('new migrations after 071 use qualified target tables', () => {
  const migrations = loadMigrationFiles()
    .filter((migration) => Number.parseInt(migration.name.slice(0, 3), 10) >= 71);
  assert.ok(migrations.length > 0, 'expected 071+ migrations');

  for (const migration of migrations) {
    assert.deepEqual(
      unqualifiedTargets(migration.sql),
      [],
      `${migration.name} contains an unqualified DML target`,
    );
  }
});
