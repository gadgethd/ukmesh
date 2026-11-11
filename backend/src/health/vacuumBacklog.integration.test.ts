import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import {
  DATABASE_MAINTENANCE_SQL,
  DATABASE_VACUUM_THRESHOLD_SQL,
} from './vacuumBacklog.js';

// CI uses its existing disposable PostgreSQL service. Local verification can
// use a worktree-local PGlite installation without any server or credentials.
// Every server query is read-only; no schema, migrations or fixtures are written.
const connectionString = process.env['TEST_VACUUM_DATABASE_URL'];
const moduleUrl = process.env['TEST_VACUUM_PGLITE_MODULE'];
let db: { query(sql: string, values?: unknown[]): Promise<{ rows: Record<string, unknown>[] }> };
let close: () => Promise<void>;

before(async () => {
  if (moduleUrl) {
    const { PGlite } = await import(moduleUrl);
    const instance = await PGlite.create();
    db = instance;
    close = () => instance.close();
  } else {
    assert.ok(connectionString, 'Set TEST_VACUUM_DATABASE_URL or TEST_VACUUM_PGLITE_MODULE');
    const { Client } = await import('pg');
    const client = new Client({
      connectionString,
      application_name: 'ukmesh-vacuum-policy-tests',
      options: '-c default_transaction_read_only=on -c statement_timeout=10000',
    });
    await client.connect();
    db = client;
    close = () => client.end();
  }
});

after(async () => { if (close) await close(); });

type Scenario = {
  name: string;
  rows: number;
  dead: number;
  options?: string[];
  enabled?: boolean;
  trackCounts?: boolean;
  base?: number;
  scale?: number;
  threshold: number;
  backlog: boolean;
};

const scenarios: Scenario[] = [
  { name: 'incident: multibyte churn below its trigger', rows: 2_469_879, dead: 285_751, threshold: 494_025.8, backlog: false },
  { name: 'incident: hourly stats below its trigger', rows: 909_590, dead: 108_462, threshold: 181_968, backlog: false },
  { name: 'incident: observer sightings below their trigger', rows: 233_931, dead: 35_693, threshold: 46_836.2, backlog: false },
  { name: 'incident: share deliveries below their trigger', rows: 234_784, dead: 27_720, threshold: 47_006.8, backlog: false },
  { name: 'incident: public feed below its trigger', rows: 98_132, dead: 18_527, threshold: 19_676.4, backlog: false },
  { name: 'large table crossing its trigger still alerts', rows: 2_469_879, dead: 494_026, threshold: 494_025.8, backlog: true },
  { name: 'exact trigger is not exceeded', rows: 100_000, dead: 20_050, threshold: 20_050, backlog: false },
  { name: 'one dead row above trigger alerts', rows: 100_000, dead: 20_051, threshold: 20_050, backlog: true },
  { name: 'table scale overrides server default', rows: 1_000_000, dead: 60_000, options: ['autovacuum_vacuum_scale_factor=0.05'], threshold: 50_050, backlog: true },
  { name: 'table base overrides server default', rows: 100_000, dead: 22_001, options: ['autovacuum_vacuum_threshold=2000'], threshold: 22_000, backlog: true },
  { name: 'zero-valued overrides are honored', rows: 1_000_000, dead: 10_001, options: ['autovacuum_vacuum_threshold=0', 'autovacuum_vacuum_scale_factor=0'], threshold: 10_000, backlog: true },
  { name: 'both overrides win over changed defaults', rows: 1_000_000, dead: 51_001, base: 10_000, scale: 0.5, options: ['autovacuum_vacuum_threshold=1000', 'autovacuum_vacuum_scale_factor=0.05'], threshold: 51_000, backlog: true },
  { name: 'server threshold and scale are used as fallback', rows: 100_000, dead: 31_001, base: 1000, scale: 0.3, threshold: 31_000, backlog: true },
  { name: 'unrelated table options do not change trigger', rows: 100_000, dead: 20_051, options: ['autovacuum_analyze_scale_factor=0.01', 'fillfactor=80'], threshold: 20_050, backlog: true },
  { name: 'dead-only tables are not excluded', rows: 0, dead: 50_001, threshold: 10_000, backlog: true },
  { name: 'unknown row estimate is treated as zero', rows: -1, dead: 10_001, threshold: 10_000, backlog: true },
  { name: 'noise floor excludes small transient churn', rows: 100, dead: 10_000, threshold: 10_000, backlog: false },
  { name: 'disabled table vacuum cannot hide dead rows', rows: 1_000_000, dead: 10_001, options: ['autovacuum_enabled=false'], threshold: 10_000, backlog: true },
  { name: 'disabled server vacuum cannot be reenabled by table', rows: 1_000_000, dead: 10_001, enabled: false, options: ['autovacuum_enabled=true'], threshold: 10_000, backlog: true },
  { name: 'disabled statistics collection uses conservative floor', rows: 1_000_000, dead: 10_001, trackCounts: false, threshold: 10_000, backlog: true },
];

for (const scenario of scenarios) {
  test(scenario.name, async () => {
    const result = await db.query(`WITH c AS (
      SELECT $1::real AS reltuples, $2::text[] AS reloptions
    ), settings AS (
      SELECT $3::boolean AS autovacuum_enabled, $4::boolean AS track_counts,
        $5::double precision AS vacuum_threshold, $6::double precision AS vacuum_scale_factor
    ), threshold AS (
      SELECT ${DATABASE_VACUUM_THRESHOLD_SQL} AS value FROM c CROSS JOIN settings
    ) SELECT value, $7::bigint > value AS needs_vacuum FROM threshold`, [
      scenario.rows, scenario.options ?? null, scenario.enabled ?? true,
      scenario.trackCounts ?? true, scenario.base ?? 50, scenario.scale ?? 0.2,
      scenario.dead,
    ]);
    assert.ok(Math.abs(Number(result.rows[0]?.value) - scenario.threshold) < 0.000001);
    assert.equal(result.rows[0]?.needs_vacuum, scenario.backlog);
  });
}

test('the complete health catalog query executes inside a read-only transaction', async () => {
  await db.query('BEGIN READ ONLY');
  try {
    assert.equal((await db.query("SELECT current_setting('transaction_read_only') AS mode")).rows[0]?.mode, 'on');
    const result = await db.query(DATABASE_MAINTENANCE_SQL);
    assert.equal(result.rows.length, 1);
    assert.ok(Number(result.rows[0]?.database_size_bytes) > 0);
    assert.ok(Number(result.rows[0]?.dead_rows) >= 0);
    assert.ok(Number(result.rows[0]?.tables_needing_vacuum) >= 0);
  } finally {
    await db.query('ROLLBACK');
  }
});
