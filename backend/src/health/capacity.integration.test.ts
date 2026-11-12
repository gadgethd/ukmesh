import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createHealthRepository, CORE_CAPACITY_TARGETS } from './repository.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
const local = endpoint && ['localhost', '127.0.0.1'].includes(new URL(endpoint).hostname);

test('capacity SQL observes the policy compression age plus exactly one day and strict retention boundaries', {
  skip: local ? false : 'owned loopback PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `health_capacity_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}" TEMPLATE template0`);
  const url = new URL(endpoint!); url.pathname = '/' + name;
  const client = new pg.Client({ connectionString: url.href });
  t.after(async () => {
    await client.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });
  await client.connect();
  await client.query(`BEGIN;
    CREATE SCHEMA timescaledb_information;
    CREATE SCHEMA owned_chunks;
    CREATE TABLE owned_chunks.data (value integer);
    CREATE TABLE timescaledb_information.chunks (
      hypertable_schema text, hypertable_name text, chunk_schema text, chunk_name text,
      range_end timestamptz, is_compressed boolean
    )`);
  for (const target of CORE_CAPACITY_TARGETS) {
    await client.query(`INSERT INTO timescaledb_information.chunks
      SELECT $1, $2, 'owned_chunks', 'data', NOW() - $3::interval - INTERVAL '1 day' + delta, FALSE
      FROM unnest(ARRAY[INTERVAL '1 microsecond', INTERVAL '0 seconds',
        INTERVAL '-1 microsecond', INTERVAL '-1 day']) AS delta`,
    [target.schema_name, target.table_name, target.compress_after]);
    await client.query(`INSERT INTO timescaledb_information.chunks
      SELECT $1, $2, 'owned_chunks', 'data', NOW() - $3::interval + delta, TRUE
      FROM unnest(ARRAY[INTERVAL '0 seconds', INTERVAL '-1 microsecond']) AS delta`,
    [target.schema_name, target.table_name, target.retention]);
  }
  await client.query(`INSERT INTO timescaledb_information.chunks VALUES
    ('core', 'outside_observation_subset', 'owned_chunks', 'data', NOW() - INTERVAL '1 year', FALSE)`);
  const result = await createHealthRepository((sql, params) => client.query(sql, params)).loadCoreCapacity();
  assert.deepEqual(result.rows.map(row => row.table_name).sort(), CORE_CAPACITY_TARGETS.map(target => target.table_name).sort());
  for (const row of result.rows) {
    assert.equal(row.chunk_count, '6', row.table_name);
    assert.equal(row.expired_chunks, '1', row.table_name);
    assert.equal(row.overdue_uncompressed_chunks, '2', row.table_name);
  }
  await client.query('ROLLBACK');
});
