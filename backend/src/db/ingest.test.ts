import assert from 'node:assert/strict';
import test from 'node:test';
import type pg from 'pg';
import { createIngestDatabase, ingestPoolOptions } from './ingest.js';
import { dbQueriesTotal } from '../metrics.js';

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
