import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDatabaseRuntime, type DatabaseRuntime } from './databaseRuntime.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];

test('two database runtimes own independent role pools, transactions and shutdown', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const names = [1, 2].map(index => `runtime_factory_${process.pid}_${Date.now()}_${index}`);
  const runtimes: DatabaseRuntime[] = [];
  t.after(async () => {
    await Promise.all(runtimes.map(runtime => runtime.close()));
    for (const name of names) await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });
  for (const [index, name] of names.entries()) {
    await admin.query(`CREATE DATABASE "${name}"`);
    const url = new URL(endpoint!); url.pathname = '/' + name;
    runtimes.push(createDatabaseRuntime({ DATABASE_URL: url.toString(),
      DATABASE_APPLICATION_NAME: name, DATABASE_POOL_MAX: '1', DATABASE_SCHEMA: 'public',
      DATABASE_STATEMENT_TIMEOUT_MS: String(2_000 + index * 1_000), MQTT_INGEST_STATEMENT_TIMEOUT_MS: '200' }));
  }
  const [first, second] = runtimes as [DatabaseRuntime, DatabaseRuntime];
  const pids = new Set<number>();
  for (const [index, runtime] of runtimes.entries()) {
    await runtime.query('CREATE TABLE owned (value integer)');
    await runtime.withTransaction(async query => { await query('INSERT INTO owned VALUES ($1)', [index + 1]); });
    await assert.rejects(runtime.withTransaction(async query => {
      await query('INSERT INTO owned VALUES (99)'); throw new Error('owned rollback');
    }), /owned rollback/);
    await runtime.withIngestTransaction(async query => { await query('INSERT INTO owned VALUES (10)'); });
    const rows = await runtime.namedQuery('owned_values', 'SELECT value FROM owned ORDER BY value');
    assert.deepEqual(rows.rows, [{ value: index + 1 }, { value: 10 }]);
    const roleRows = await Promise.all([runtime.query, runtime.analyticsQuery, runtime.ingestQuery].map(query =>
      query<{ pid: number; database: string; application: string; timeout: string }>(`SELECT
        pg_backend_pid() AS pid, current_database() AS database,
        current_setting('application_name') AS application, current_setting('statement_timeout') AS timeout`)));
    assert.deepEqual(roleRows.map(result => result.rows[0]!.database), Array(3).fill(names[index]));
    assert.deepEqual(roleRows.map(result => result.rows[0]!.application),
      [names[index], `${names[index]}-analytics`, `${names[index]}-ingest`]);
    assert.deepEqual(roleRows.map(result => result.rows[0]!.timeout), [`${2 + index}s`, '15min', '200ms']);
    for (const result of roleRows) pids.add(result.rows[0]!.pid);
  }
  assert.equal(pids.size, 6, 'all three role pools belong to their individual runtime');
  await Promise.all([first.close(), first.close()]);
  for (const execute of [first.query, first.analyticsQuery, first.ingestQuery]) {
    await assert.rejects(execute('SELECT 1'), /Cannot use a pool after calling end/);
  }
  assert.deepEqual((await second.query('SELECT value FROM owned ORDER BY value')).rows, [{ value: 2 }, { value: 10 }]);
  await second.analyticsQuery('SELECT 1');
  await second.ingestQuery('SELECT 1');
});
