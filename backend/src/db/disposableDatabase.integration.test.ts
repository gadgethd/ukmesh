import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import { mockRuntimeQuery } from './fixtures/mockRuntimeQuery.js';
import { query, analyticsQuery } from './runtime.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
const native = { skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000 };

test('disposable fixtures reject remote endpoints before creating resources', async t => {
  await assert.rejects(createDisposableDatabase(t, 'postgresql://example.invalid/postgres', 'fixture'), /loopback/);
  await assert.rejects(createDisposableDatabase(t, 'postgresql://127.0.0.1/postgres', 'not-an-identifier'), /prefix/);
});

test('disposable fixtures preserve real pools during default-runtime mocks and remove their database', native, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'disposable_fixture');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 1 });
  fixture.addCleanup(() => pool.end());
  const mocked = mockRuntimeQuery(t, async () => ({ rows: [{ database: 'default-runtime-mock' }] }));
  assert.deepEqual((await query('SELECT current_database()')).rows, [{ database: 'default-runtime-mock' }]);
  assert.deepEqual((await analyticsQuery('SELECT current_database()')).rows, [{ database: 'default-runtime-mock' }]);
  assert.deepEqual((await pool.query('SELECT current_database() AS database')).rows, [{ database: fixture.name }]);
  mocked.mock.restore();
  await Promise.all([fixture.dispose(), fixture.dispose()]);
  const admin = new pg.Client({ connectionString: endpoint });
  await admin.connect();
  try {
    assert.equal((await admin.query('SELECT 1 FROM pg_database WHERE datname=$1', [fixture.name])).rowCount, 0);
  } finally { await admin.end(); }
});

test('fixture teardown releases reserved clients before pools even after setup or assertion failure', native, async t => {
  for (const failure of ['setup', 'assertion']) {
    const fixture = await createDisposableDatabase(t, endpoint!, 'failed_fixture');
    const pool = new pg.Pool({ connectionString: fixture.url, max: 1 });
    const released: string[] = [];
    fixture.addCleanup(async () => { await pool.end(); released.push('pool'); });
    const reserved = await pool.connect();
    fixture.addCleanup(() => { reserved.release(); released.push('client'); });
    await assert.rejects(async () => {
      try {
        if (failure === 'assertion') assert.equal((await reserved.query('SELECT 1 AS value')).rows[0].value, 2);
        throw new Error('fixture setup failed');
      } finally { await fixture.dispose(); }
    });
    assert.deepEqual(released, ['client', 'pool']);
    const admin = new pg.Client({ connectionString: endpoint });
    await admin.connect();
    try {
      assert.equal((await admin.query('SELECT 1 FROM pg_database WHERE datname=$1', [fixture.name])).rowCount, 0);
    } finally { await admin.end(); }
  }
});

test('one failed resource closer does not leave another owned database behind', native, async t => {
  let after: (() => unknown) | undefined;
  const hooks = { after(callback: () => unknown) { assert.equal(after, undefined); after = callback; } };
  let disposed = false;
  t.after(async () => { if (!disposed) await after?.(); });
  const first = await createDisposableDatabase(hooks, endpoint!, 'failed_closer');
  const second = await createDisposableDatabase(hooks, endpoint!, 'other_fixture');
  for (const fixture of [first, second]) {
    const pool = new pg.Pool({ connectionString: fixture.url, max: 1 });
    fixture.addCleanup(() => pool.end());
    await pool.query('SELECT 1');
  }
  first.addCleanup(() => { throw new Error('closer failed'); });
  await assert.rejects(async () => { await after!(); }, AggregateError);
  disposed = true;
  const admin = new pg.Client({ connectionString: endpoint });
  await admin.connect();
  try {
    assert.equal((await admin.query('SELECT 1 FROM pg_database WHERE datname=ANY($1)', [[first.name, second.name]])).rowCount, 0);
  } finally { await admin.end(); }
});
