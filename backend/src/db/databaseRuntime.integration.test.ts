import assert from 'node:assert/strict';
import test from 'node:test';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import { createDatabaseRuntime, type DatabaseRuntime } from './databaseRuntime.js';
import { dbExecutionDuration, dbPoolWaitDuration } from '../metrics/database.js';
import { BOUNDED_READ_PREPARATION } from './preparedStatements.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];

test('two database runtimes own independent role pools, transactions and shutdown', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  const names: string[] = [];
  const runtimes: DatabaseRuntime[] = [];
  for (let index = 0; index < 2; index++) {
    const fixture = await createDisposableDatabase(t, endpoint!, 'runtime_factory');
    names.push(fixture.name);
    const analyticsUrl = new URL(fixture.url);
    analyticsUrl.searchParams.set('options', [analyticsUrl.searchParams.get('options'),
      '-c default_transaction_read_only=on'].filter(Boolean).join(' '));
    const runtime = createDatabaseRuntime({ DATABASE_URL: fixture.url,
      DATABASE_APPLICATION_NAME: fixture.name, DATABASE_POOL_MAX: '1', DATABASE_SCHEMA: 'public',
      DATABASE_STATEMENT_TIMEOUT_MS: String(2_000 + index * 1_000), MQTT_INGEST_STATEMENT_TIMEOUT_MS: '200',
      ...(index === 0 ? { MESHCORE_WORKER_DATABASE_URL: analyticsUrl.href,
        DATABASE_ANALYTICS_RANDOM_PAGE_COST: '2.5', DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE: '1GB' } : {}) });
    fixture.addCleanup(() => runtime.close());
    runtimes.push(runtime);
  }
  const [first, second] = runtimes as [DatabaseRuntime, DatabaseRuntime];
  const pids = new Set<number>();
  const plannerSql = `SELECT current_setting('random_page_cost') AS random_page_cost,
    current_setting('effective_cache_size') AS effective_cache_size`;
  const serverPlanner = (await first.query(plannerSql)).rows[0];
  assert.notEqual(serverPlanner!.random_page_cost, '2.5', 'native planner oracle needs a distinct server cost');
  assert.notEqual(serverPlanner!.effective_cache_size, '1GB', 'native planner oracle needs a distinct server cache estimate');
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
    const plannerRows = await Promise.all([runtime.query, runtime.analyticsQuery, runtime.ingestQuery]
      .map(query => query(plannerSql)));
    assert.deepEqual(plannerRows.map(result => result.rows[0]), index === 0
      ? [serverPlanner, { random_page_cost: '2.5', effective_cache_size: '1GB' }, serverPlanner]
      : [serverPlanner, serverPlanner, serverPlanner]);
    if (index === 0) assert.equal((await runtime.analyticsQuery("SHOW default_transaction_read_only")).rows[0]!.default_transaction_read_only, 'on');
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
  assert.deepEqual((await second.analyticsQuery(plannerSql)).rows[0], serverPlanner);
});

test('database telemetry separates pool wait, execution, failure and cancellation without leaking connections', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const runtime = createDatabaseRuntime({ DATABASE_URL: endpoint, DATABASE_POOL_MAX: '1' });
  t.after(() => runtime.close());
  const sum = async () => (await dbPoolWaitDuration.get()).values
    .filter(row => row.metricName.endsWith('_sum') && row.labels['pool'] === 'oltp')
    .reduce((total, row) => total + row.value, 0);
  const count = async (outcome: string) => (await dbExecutionDuration.get()).values
    .filter(row => row.metricName.endsWith('_count') && row.labels['operation'] === 'node-map'
      && row.labels['pool'] === 'oltp' && row.labels['outcome'] === outcome)
    .reduce((total, row) => total + row.value, 0);
  const before = await sum(), executed = await count('success');
  const held = await runtime.pool.connect();
  const waiting = runtime.query('/* ukmesh:node-map */ SELECT 1', [], undefined, BOUNDED_READ_PREPARATION);
  await new Promise(resolve => setTimeout(resolve, 40)); held.release();
  await waiting;
  assert.ok(await sum() - before >= .025, 'queued checkout time is measured');
  assert.equal(await count('success'), executed + 1, 'one SQL execution excludes queue wait');
  const queuedHolder = await runtime.pool.connect();
  const controller = new AbortController();
  const queued = runtime.query('/* ukmesh:node-map */ SELECT 2', [], controller.signal, BOUNDED_READ_PREPARATION);
  controller.abort(new Error('queued cancellation')); queuedHolder.release();
  await assert.rejects(queued, /queued cancellation/);
  assert.equal(await count('success'), executed + 1, 'cancelled queued work executes no SQL');
  const aborted = await count('aborted');
  await assert.rejects(runtime.query('/* ukmesh:node-map */ SELECT pg_sleep(5)', [], AbortSignal.timeout(25),
    BOUNDED_READ_PREPARATION));
  assert.equal(await count('aborted'), aborted + 1);
  const failed = await count('failure');
  await assert.rejects(runtime.query('/* ukmesh:node-map */ SELECT * FROM nonexistent_lookup_fixture', [], undefined,
    BOUNDED_READ_PREPARATION));
  assert.equal(await count('failure'), failed + 1);
  assert.equal((await runtime.query('SELECT 7 AS value')).rows[0].value, 7);
  assert.equal(runtime.pool.waitingCount, 0);
  assert.equal(runtime.pool.totalCount, runtime.pool.idleCount);
});
