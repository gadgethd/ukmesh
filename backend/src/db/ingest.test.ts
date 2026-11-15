import assert from 'node:assert/strict';
import test from 'node:test';
import type pg from 'pg';
import { createIngestDatabase, ingestPoolOptions } from './ingestExecution.js';
import { dbPoolWaitDuration, dbQueriesTotal, dbQueryDuration } from '../metrics/database.js';
import type { FixedPreparedStatementName } from './contracts.js';

test('ingest has a nonzero timeout independent of the general/analytics pool', () => {
  const defaults = ingestPoolOptions({ DATABASE_STATEMENT_TIMEOUT_MS: '900000' });
  assert.equal(defaults.statement_timeout, 10_000);
  assert.equal(defaults.query_timeout, 11_000);
  assert.equal(defaults.max, 2);
  assert.equal(ingestPoolOptions({ DATABASE_STATEMENT_TIMEOUT_MS: '0' }).statement_timeout, 10_000);
  assert.equal(ingestPoolOptions({ MQTT_INGEST_STATEMENT_TIMEOUT_MS: '100' }).statement_timeout, 100);
  for (const value of ['0', '-1', '99', '30001', 'NaN', '1.5']) {
    assert.throws(() => ingestPoolOptions({ MQTT_INGEST_STATEMENT_TIMEOUT_MS: value }), /must be an integer/);
  }
});

function fixture(error: Error & { code?: string }, rollbackFails = false) {
  const calls: string[] = [];
  const releases: boolean[] = [];
  const client = {
    async query(sql: string) {
      calls.push(sql);
      if (sql === 'slow') throw error;
      if (sql === 'ROLLBACK' && rollbackFails) throw new Error('connection lost');
      return { rows: [] };
    },
    release(discard: boolean) { releases.push(discard); },
  };
  const pool = {
    options: { statement_timeout: 100 }, totalCount: 1, idleCount: 0, waitingCount: 0,
    async connect() { return client; }, query: client.query,
  } as unknown as pg.Pool;
  return { ...createIngestDatabase(pool), calls, releases };
}

test('server cancellation rolls back, frees the client, reports a timeout and preserves the error', async (t) => {
  const logs = t.mock.method(console, 'error', () => {});
  const error = Object.assign(new Error('statement timeout'), { code: '57014' });
  const db = fixture(error);
  await assert.rejects(db.withTransaction((query) => query('slow')), (actual) => actual === error);
  assert.deepEqual(db.calls, ['BEGIN', 'slow', 'ROLLBACK']);
  assert.deepEqual(db.releases, [false]);
  assert.equal(logs.mock.calls[0].arguments[0], '[ingest-db] query timed out');
  assert.deepEqual(logs.mock.calls[0].arguments[1], { timeoutMs: 100 });
  assert.ok((await dbQueriesTotal.get()).values.some(
    (value) => value.labels.pool === 'ingest' && value.labels.outcome === 'timed_out' && value.value >= 1,
  ));
});

test('a client read timeout destroys the connection without waiting for queued rollback', async (t) => {
  t.mock.method(console, 'error', () => {});
  const db = fixture(new Error('Query read timeout'));
  await assert.rejects(db.withTransaction((query) => query('slow')), /Query read timeout/);
  assert.deepEqual(db.calls, ['BEGIN', 'slow']);
  assert.deepEqual(db.releases, [true]);
});

test('rollback failure destroys the connection and retains the original failure', async () => {
  const error = new Error('original failure');
  const db = fixture(error, true);
  await assert.rejects(db.withTransaction((query) => query('slow')), (actual) => actual === error);
  assert.deepEqual(db.releases, [true]);
});

test('successful work commits and returns the client to the pool', async () => {
  const db = fixture(new Error('unused'));
  assert.equal(await db.withTransaction(async (query) => { await query('fast'); return 42; }), 42);
  assert.deepEqual(db.calls, ['BEGIN', 'fast', 'COMMIT']);
  assert.deepEqual(db.releases, [false]);
});

async function ingestDuration(metric: typeof dbPoolWaitDuration, outcome: string) {
  const values = (await metric.get()).values.filter(
    row => row.labels['pool'] === 'ingest' && row.labels['outcome'] === outcome,
  );
  return {
    count: values.find(row => row.metricName.endsWith('_count'))?.value ?? 0,
    sum: values.find(row => row.metricName.endsWith('_sum'))?.value ?? 0,
  };
}

test('a contended ingest checkout measures only acquisition and preserves the transaction lease', async t => {
  let clock = 0n;
  t.mock.method(process.hrtime, 'bigint', () => clock);
  let admit!: (client: pg.PoolClient) => void;
  const waiting = new Promise<pg.PoolClient>(resolve => { admit = resolve; });
  const calls: string[] = [], releases: boolean[] = [];
  const client = {
    async query(sql: string) {
      calls.push(sql);
      if (sql === 'COMMIT') clock += 200_000_000n;
      return { rows: [] };
    },
    release(discard: boolean) { releases.push(discard); },
  } as unknown as pg.PoolClient;
  const pool = {
    options: {}, totalCount: 1, idleCount: 0, waitingCount: 1, connect: () => waiting,
  } as unknown as pg.Pool;
  const before = await ingestDuration(dbPoolWaitDuration, 'success');
  const executionBefore = await ingestDuration(dbQueryDuration, 'success');
  const work = createIngestDatabase(pool).withTransaction(async query => {
    const acquired = await ingestDuration(dbPoolWaitDuration, 'success');
    assert.equal(acquired.count - before.count, 1, 'checkout is observed before the work callback');
    assert.ok(Math.abs(acquired.sum - before.sum - .075) < 1e-9);
    clock += 200_000_000n;
    await query('owned work');
    return 42;
  });
  assert.deepEqual(calls, []);
  assert.deepEqual(await ingestDuration(dbPoolWaitDuration, 'success'), before);
  clock = 75_000_000n;
  admit(client);
  assert.equal(await work, 42);
  assert.deepEqual(calls, ['BEGIN', 'owned work', 'COMMIT']);
  assert.deepEqual(releases, [false]);
  const after = await ingestDuration(dbPoolWaitDuration, 'success');
  assert.equal(after.count - before.count, 1);
  assert.ok(Math.abs(after.sum - before.sum - .075) < 1e-9, 'work and COMMIT do not extend pool wait');
  const executionAfter = await ingestDuration(dbQueryDuration, 'success');
  assert.equal(executionAfter.count - executionBefore.count, 3);
  assert.ok(Math.abs(executionAfter.sum - executionBefore.sum - .2) < 1e-9, 'COMMIT remains SQL execution time');
});

test('failed ingest acquisition records its bounded failure label without SQL, callback or lease release', async t => {
  let clock = 0n;
  t.mock.method(process.hrtime, 'bigint', () => clock);
  let refuse!: (error: Error) => void;
  const waiting = new Promise<pg.PoolClient>((_resolve, reject) => { refuse = reject; });
  const pool = {
    options: {}, totalCount: 0, idleCount: 0, waitingCount: 1, connect: () => waiting,
  } as unknown as pg.Pool;
  const before = await ingestDuration(dbPoolWaitDuration, 'failure');
  let called = false;
  const work = createIngestDatabase(pool).withTransaction(async () => { called = true; });
  const failure = new Error('owned connection unavailable');
  const rejected = assert.rejects(work, error => error === failure);
  clock = 25_000_000n;
  refuse(failure);
  await rejected;
  assert.equal(called, false);
  const after = await ingestDuration(dbPoolWaitDuration, 'failure');
  assert.equal(after.count - before.count, 1);
  assert.ok(Math.abs(after.sum - before.sum - .025) < 1e-9);
});

test('singleton preparation uses fixed names and fresh values on the same transaction client', async () => {
  const calls: unknown[] = [];
  const client = { query: async (...args: unknown[]) => { calls.push(args); return { rows: [] }; }, release() {} };
  const pool = { options: {}, totalCount: 1, idleCount: 0, waitingCount: 0,
    connect: async () => client } as unknown as pg.Pool;
  const db = createIngestDatabase(pool);
  await db.withTransaction(async query => {
    await query('SELECT $1::int', [1], undefined, 'packet_batch_single_v1');
    await query('SELECT $1::int', [2], undefined, 'packet_batch_single_v1');
    await query('SELECT $1::text', ['new'], undefined, 'packet_batch_single_idempotent_v1');
  });
  assert.deepEqual(calls, [['BEGIN', undefined],
    [{ name: 'packet_batch_single_v1', text: 'SELECT $1::int', values: [1] }],
    [{ name: 'packet_batch_single_v1', text: 'SELECT $1::int', values: [2] }],
    [{ name: 'packet_batch_single_idempotent_v1', text: 'SELECT $1::text', values: ['new'] }],
    ['COMMIT', undefined]]);
});

test('unregistered statement names roll back without allocating a prepared statement', async () => {
  const db = fixture(new Error('unused'));
  await assert.rejects(db.withTransaction(query => query('SELECT 1', [], undefined,
    'unbounded-node-identity' as FixedPreparedStatementName)), /INVALID_FIXED_PREPARED_STATEMENT/);
  assert.deepEqual(db.calls, ['BEGIN', 'ROLLBACK']);
  assert.deepEqual(db.releases, [false]);
});
