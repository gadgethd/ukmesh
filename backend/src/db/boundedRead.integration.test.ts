import assert from 'node:assert/strict';
import test from 'node:test';
import type pg from 'pg';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import { createDatabaseRuntime } from './databaseRuntime.js';
import { BOUNDED_READ_PREPARATION } from './preparedStatements.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
const preparedSql = `SELECT name,statement,(generic_plans+custom_plans)::int AS executions
  FROM pg_prepared_statements WHERE starts_with(name,'read_v1_') ORDER BY name`;

test('queued transaction reads cap names per physical client, preserve overflow and reset on replacement', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'bounded_read_capacity');
  const runtime = createDatabaseRuntime({ DATABASE_URL: fixture.url, DATABASE_POOL_MAX: '2' });
  fixture.addCleanup(() => runtime.close());
  let held: pg.PoolClient | undefined;
  fixture.addCleanup(() => { held?.release(); });
  const sql = (index: number) => `SELECT $1::int AS value /* read shape ${index} */`;
  const results = await runtime.withTransaction(query => Promise.all(Array.from({ length: 40 }, (_, index) =>
    query(sql(index), [index], undefined, BOUNDED_READ_PREPARATION))));
  assert.deepEqual(results.map(result => result.rows[0]!['value']), Array.from({ length: 40 }, (_, index) => index));
  const prepared = (await runtime.query(preparedSql)).rows;
  assert.equal(prepared.length, 32, 'queued calls reserve only32names before any completion');
  assert.deepEqual(prepared.map(row => row['statement']).sort(), Array.from({ length: 32 }, (_, index) => sql(index)).sort());
  assert.deepEqual((await runtime.query(sql(0), [900], undefined, BOUNDED_READ_PREPARATION)).rows, [{ value: 900 }]);
  assert.deepEqual((await runtime.query(sql(39), [939], undefined, BOUNDED_READ_PREPARATION)).rows, [{ value: 939 }]);
  const afterOverflow = (await runtime.query(preparedSql)).rows;
  assert.equal(afterOverflow.length, 32);
  assert.equal(afterOverflow.find(row => row['statement'] === sql(0))!['executions'], 2, 'hot definition survives overflow');
  assert.ok(afterOverflow.every(row => row['statement'] !== sql(39)), 'overflow never prepares a server definition');
  assert.deepEqual((await runtime.namedQuery('existing_named_interface', 'SELECT $1::int AS value', [123])).rows,
    [{ value: 123 }], 'the existing namedQuery interface remains independent');

  held = await runtime.pool.connect();
  const firstPid = (await held.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  await runtime.query(sql(39), [239], undefined, BOUNDED_READ_PREPARATION);
  assert.equal((await runtime.query(preparedSql)).rows.length, 1, 'the second physical client has its own capacity');
  assert.equal((await held.query(preparedSql)).rows.length, 32, 'the held first connection retains its definitions');
  const secondPid = (await runtime.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  assert.notEqual(secondPid, firstPid);

  held.release(true);
  held = undefined;
  held = await runtime.pool.connect();
  assert.equal((await held.query('SELECT pg_backend_pid() AS pid')).rows[0].pid, secondPid);
  await runtime.query(sql(40), [340], undefined, BOUNDED_READ_PREPARATION);
  const replacementPid = (await runtime.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  assert.notEqual(replacementPid, firstPid);
  assert.notEqual(replacementPid, secondPid);
  const replacement = (await runtime.query(preparedSql)).rows;
  assert.deepEqual(replacement.map(row => row['statement']), [sql(40)], 'replacement is not keyed by the pool or reused PID slot');
  held.release();
  held = undefined;
});

test('prepared reads see fresh parameters and committed visibility, with unchanged transaction rollback and errors', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'bounded_read_freshness');
  const runtime = createDatabaseRuntime({ DATABASE_URL: fixture.url, DATABASE_POOL_MAX: '1' });
  fixture.addCleanup(() => runtime.close());
  await runtime.query('CREATE TABLE bounded_rows(id integer PRIMARY KEY,label text,visible boolean NOT NULL)');
  await runtime.query("INSERT INTO bounded_rows VALUES(1,'first',true),(2,'private',false),(3,'third',true)");
  const sql = 'SELECT id,label FROM bounded_rows WHERE visible AND id>$1 ORDER BY id LIMIT $2';
  for (let index = 0; index < 6; index++) {
    assert.deepEqual((await runtime.query(sql, [index % 2, 1], undefined, BOUNDED_READ_PREPARATION)).rows,
      index % 2 ? [{ id: 3, label: 'third' }] : [{ id: 1, label: 'first' }]);
  }
  await runtime.query("UPDATE bounded_rows SET visible=(id<>1),label=CASE WHEN id=2 THEN 'now public' ELSE label END");
  assert.deepEqual((await runtime.query(sql, [0, 10], undefined, BOUNDED_READ_PREPARATION)).rows,
    [{ id: 2, label: 'now public' }, { id: 3, label: 'third' }], 'definition reuse never reuses read results');
  const originalError = new Error('owned transaction rollback');
  await assert.rejects(runtime.withTransaction(async query => {
    await query('UPDATE bounded_rows SET visible=false WHERE id=2');
    assert.deepEqual((await query(sql, [0, 10], undefined, BOUNDED_READ_PREPARATION)).rows,
      [{ id: 3, label: 'third' }], 'transaction preparation uses its already-owned client');
    throw originalError;
  }), error => error === originalError);
  assert.deepEqual((await runtime.query(sql, [0, 10], undefined, BOUNDED_READ_PREPARATION)).rows,
    [{ id: 2, label: 'now public' }, { id: 3, label: 'third' }]);
  const beforeAbort = (await runtime.query(preparedSql)).rows;
  const controller = new AbortController();
  const reason = new Error('already aborted read');
  controller.abort(reason);
  await assert.rejects(runtime.query('SELECT pg_sleep(5)', [], controller.signal, BOUNDED_READ_PREPARATION),
    error => error === reason);
  assert.deepEqual((await runtime.query(preparedSql)).rows, beforeAbort, 'aborted-before-checkout work reserves no definition');
  await assert.rejects(runtime.query('SELECT * FROM missing_bounded_read_fixture', [], undefined,
    BOUNDED_READ_PREPARATION), error => (error as { code?: string }).code === '42P01');
  assert.deepEqual((await runtime.query(sql, [2, 1], undefined, BOUNDED_READ_PREPARATION)).rows,
    [{ id: 3, label: 'third' }], 'an SQL failure is not retried and leaves successful statement reuse intact');
  const prepared = (await runtime.query(preparedSql)).rows;
  assert.equal(prepared.length, 1);
  assert.equal(prepared[0]!['statement'], sql);
  assert.equal(runtime.pool.waitingCount, 0);
  assert.equal(runtime.pool.totalCount, runtime.pool.idleCount);
});
