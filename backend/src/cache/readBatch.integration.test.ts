import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { createReadBatcher } from './readBatch.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];

test('a later same-key caller gets a new SQL snapshot while an older read is held', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'read_batch_snapshot');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 2,
    connectionTimeoutMillis: 2_000, statement_timeout: 5_000, query_timeout: 6_000 });
  fixture.addCleanup(() => pool.end());
  type Rows = Array<{ id: number; payload: { nested: { value: string } }; observed_at: Date }>;
  const batch = createReadBatcher<Rows>(structuredClone);
  fixture.addCleanup(() => batch.close());
  await pool.query('CREATE TABLE batch_rows(id integer PRIMARY KEY,payload jsonb,observed_at timestamptz)');
  await pool.query('INSERT INTO batch_rows VALUES(1,$1,$2)',
    [{ nested: { value: 'before' } }, '2026-01-01T00:00:00Z']);
  const sql = 'SELECT id,payload,observed_at FROM batch_rows ORDER BY id';
  let ready!: () => void, failReady!: (error: unknown) => void, release!: () => void;
  const snapshotReady = new Promise<void>((resolve, reject) => { ready = resolve; failReady = reject; });
  const releaseFirst = new Promise<void>(resolve => { release = resolve; });
  let calls = 0;
  const first = batch.run('unchanged-generation-and-key', async () => {
    calls++;
    let client: pg.PoolClient | undefined;
    try {
      client = await pool.connect();
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      const rows = (await client.query<Rows[number]>(sql)).rows;
      ready();
      await releaseFirst;
      await client.query('COMMIT');
      return rows;
    } catch (error) {
      failReady(error);
      await client?.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client?.release(); }
  });
  const joined = batch.run('unchanged-generation-and-key', async () => {
    throw new Error('a pre-dispatch join must not issue its own SQL');
  });
  const settled = Promise.allSettled([first, joined]);
  // Release the held transaction even if a later assertion or query fails.
  fixture.addCleanup(async () => { release(); await settled; });
  await snapshotReady;
  assert.equal(calls, 1);
  await pool.query('UPDATE batch_rows SET payload=$1', [{ nested: { value: 'after external commit' } }]);
  const later = await batch.run('unchanged-generation-and-key', async () => {
    calls++;
    return (await pool.query<Rows[number]>(sql)).rows;
  });
  assert.equal(calls, 2, 'the old promise is still unresolved but cannot admit another caller');
  assert.equal(later[0]!.payload.nested.value, 'after external commit');
  release();
  const [old, oldJoined] = await Promise.all([first, joined]);
  assert.equal(old[0]!.payload.nested.value, 'before');
  assert.equal(oldJoined[0]!.payload.nested.value, 'before');
  old[0]!.payload.nested.value = 'caller mutation';
  old[0]!.observed_at.setUTCFullYear(2030);
  assert.equal(oldJoined[0]!.payload.nested.value, 'before');
  assert.equal(oldJoined[0]!.observed_at.getUTCFullYear(), 2026);
  assert.equal(later[0]!.payload.nested.value, 'after external commit');
  assert.equal(pool.waitingCount, 0);
  assert.equal(pool.totalCount, pool.idleCount);
});
