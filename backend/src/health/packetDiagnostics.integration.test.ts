import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { INGEST_HEALTH_SQL, PATH_HASH_HEALTH_SQL } from './packetDiagnostics.js';

// Always shadow packets with a session-local table, and roll it back on exit.
const connectionString = process.env['TEST_DATABASE_URL'];
test('health packet diagnostics preserve mixed paths, test isolation, ties, and empty data', {
  skip: !connectionString,
}, async () => {
  const client = new pg.Client({ connectionString });
  await client.connect();
  try {
    await client.query('BEGIN');
    await client.query(`CREATE TEMP TABLE packets (
      time timestamptz, rx_node_id text, network text, topic text, path_hashes text[]
    ) ON COMMIT DROP`);
    const paths = async () => (await client.query(PATH_HASH_HEALTH_SQL)).rows[0];
    const ingest = async () => (await client.query(INGEST_HEALTH_SQL)).rows[0];
    assert.deepEqual(await paths(), {
      one_byte: '0', two_byte: '0', three_byte: '0',
      latest_multibyte_at: null, multibyte_packets_24h: '0',
    });
    assert.deepEqual(await ingest(), {
      stale_nodes: '0', active_nodes: '0', max_stale_minutes: null,
      stale_threshold_minutes: '15', global_last_packet_at: null,
    });

    await client.query(`INSERT INTO packets VALUES
      (NOW() - INTERVAL '1 minute', 'active', 'ukmesh', 'meshcore/x', ARRAY['AA', 'BBBB', 'CCCCCC', NULL, '', 'DDD']),
      (NOW() - INTERVAL '20 minutes', 'stale', NULL, 'meshcore/x', ARRAY['BB', 'BB']),
      (NOW() - INTERVAL '25 hours', 'no-path', 'ukmesh', 'meshcore/x', NULL),
      (NOW() - INTERVAL '24 hours', 'boundary', 'ukmesh', 'meshcore/x', ARRAY['CCCC']),
      (NOW() - INTERVAL '4 days', 'old', 'ukmesh', 'meshcore/x', ARRAY['EEEE']),
      (NOW() - INTERVAL '2 minutes', 'test-latest', 'ukmesh', 'meshcore/x', ARRAY[]::text[]),
      (NOW() - INTERVAL '1 minute', 'test-latest', 'test', 'meshcore-test/x', ARRAY['FFFF']),
      (NOW() - INTERVAL '2 minutes', 'tied', 'ukmesh', 'meshcore/x', NULL),
      (NOW() - INTERVAL '2 minutes', 'tied', 'test', 'meshcore-test/x', NULL),
      (NOW() - INTERVAL '3 minutes', 'public-latest', 'test', 'meshcore-test/x', NULL),
      (NOW() - INTERVAL '2 minutes', 'public-latest', 'ukmesh', 'meshcore/x', ARRAY[NULL]::text[]),
      (NOW() - INTERVAL '1 minute', 'test-topic', 'ukmesh', 'meshcore-test/x', ARRAY['12345678']),
      (NOW() - INTERVAL '1 minute', 'null-topic', 'ukmesh', NULL, NULL),
      (NOW() - INTERVAL '1 minute', '', 'ukmesh', 'meshcore/x', ARRAY[]::text[]),
      (NOW() - INTERVAL '1 minute', NULL, 'ukmesh', 'meshcore/x', ARRAY[]::text[])`);
    const expectedTime = (await client.query(`SELECT (NOW() - INTERVAL '1 minute')::text AS time`)).rows[0].time;
    assert.deepEqual(await paths(), {
      one_byte: '3', two_byte: '1', three_byte: '1',
      latest_multibyte_at: expectedTime, multibyte_packets_24h: '2',
    });
    assert.deepEqual(await ingest(), {
      stale_nodes: '3', active_nodes: '5', max_stale_minutes: '1500',
      stale_threshold_minutes: '15', global_last_packet_at: expectedTime,
    });
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    await client.end();
  }
});
