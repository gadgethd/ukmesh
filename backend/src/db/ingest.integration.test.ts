import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, mkdtemp } from 'node:fs/promises';
import { createServer } from 'node:net';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import pg from 'pg';
import { createIngestDatabase, ingestPoolOptions } from './ingest.js';
import { dbQueriesTotal } from '../metrics.js';

const moduleUrl = process.env['TEST_INGEST_POSTGRES_MODULE'];
const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];

test('ingest cancels hung SQL on PostgreSQL, rolls back mutations, releases locks and serves the next query', {
  skip: moduleUrl || databaseUrl ? false : 'configure an isolated TEST_INGEST_POSTGRES_MODULE or loopback TEST_INGEST_DATABASE_URL',
  timeout: 30_000,
}, async (t) => {
  let postgres: any;
  let pool: pg.Pool | undefined;
  let observer: pg.Pool | undefined;
  t.after(async () => {
    try { await Promise.all([pool?.end(), observer?.end()]); }
    finally { await postgres?.stop(); }
  });
  let connection: pg.PoolConfig;
  if (moduleUrl) {
    const { default: EmbeddedPostgres } = await import(moduleUrl);
    const root = fileURLToPath(new URL('../../../.ukmesh-tools/', import.meta.url));
    await mkdir(root, { recursive: true });
    const databaseDir = await mkdtemp(`${root}ingest-postgres-`);
    const socket = createServer();
    await new Promise<void>((resolve, reject) => {
      socket.once('error', reject);
      socket.listen(0, '127.0.0.1', resolve);
    });
    const address = socket.address();
    assert.ok(address && typeof address !== 'string');
    const port = address.port;
    await new Promise<void>((resolve, reject) => socket.close((error) => error ? reject(error) : resolve()));
    const password = randomUUID();
    postgres = new EmbeddedPostgres({
      databaseDir, port, user: 'ingest_fixture', password,
      authMethod: 'scram-sha-256', persistent: false, createPostgresUser: false,
      postgresFlags: ['-h', '127.0.0.1', '-k', databaseDir, '-c', 'shared_buffers=16MB'],
      onLog: () => {}, onError: () => {},
    });
    await postgres.initialise();
    await postgres.start();
    connection = { host: '127.0.0.1', port, user: 'ingest_fixture', password, database: 'postgres' };
  } else {
    assert.ok(['localhost', '127.0.0.1', '::1', '[::1]'].includes(new URL(databaseUrl!).hostname), 'only an isolated loopback test database is permitted');
    connection = { connectionString: databaseUrl };
  }
  pool = new pg.Pool({ ...ingestPoolOptions({ MQTT_INGEST_STATEMENT_TIMEOUT_MS: '100' }), ...connection, max: 1 });
  observer = new pg.Pool({ ...connection, max: 1, statement_timeout: 2_000, connectionTimeoutMillis: 2_000 });
  const logs = t.mock.method(console, 'error', () => {});
  const db = createIngestDatabase(pool);
  await db.query('SELECT 1');
  assert.equal((await db.query('SHOW statement_timeout')).rows[0].statement_timeout, '100ms');
  const before = (await dbQueriesTotal.get()).values.find(
    (value) => value.labels.pool === 'ingest' && value.labels.outcome === 'timed_out',
  )?.value ?? 0;

  const started = Date.now();
  await assert.rejects(db.query('SELECT pg_sleep(20)'), { code: '57014' });
  assert.ok(Date.now() - started < 2_000, 'server cancellation must precede the client fallback deadline');
  // Pool.query removes errored clients; transaction cancellation below instead
  // proves safe reuse after an explicit rollback on the checked-out client.
  const pid = Number((await db.query('SELECT pg_backend_pid() AS pid')).rows[0].pid);

  await db.query('CREATE TEMP TABLE ingest_timeout_probe (value integer)');
  await assert.rejects(db.withTransaction(async (query) => {
    await query('INSERT INTO ingest_timeout_probe VALUES (1)');
    await query('SELECT pg_sleep(20)');
  }), { code: '57014' });
  assert.equal((await db.query('SELECT * FROM ingest_timeout_probe')).rows.length, 0, 'timed-out transactions must not commit earlier writes');

  const lockKey = pid;
  await observer.query('SELECT pg_advisory_lock($1)', [lockKey]);
  try {
    await assert.rejects(db.withTransaction((query) => query('SELECT pg_advisory_xact_lock($1)', [lockKey])), { code: '57014' });
  } finally {
    await observer.query('SELECT pg_advisory_unlock($1)', [lockKey]);
  }
  const state = await observer.query('SELECT state, wait_event_type FROM pg_stat_activity WHERE pid = $1', [pid]);
  assert.equal(state.rows[0].state, 'idle');
  assert.notEqual(state.rows[0].wait_event_type, 'Lock');
  assert.equal(Number((await db.query('SELECT pg_backend_pid() AS pid')).rows[0].pid), pid, 'worker must recover on the same connection');
  const after = (await dbQueriesTotal.get()).values.find(
    (value) => value.labels.pool === 'ingest' && value.labels.outcome === 'timed_out',
  )!.value;
  assert.equal(after - before, 3);
  assert.equal(logs.mock.calls.length, 3);
});
