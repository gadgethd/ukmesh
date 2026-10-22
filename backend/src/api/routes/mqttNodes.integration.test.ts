import assert from 'node:assert/strict';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import pg from 'pg';
import { toPublicMqttNodeStatusDTO } from '../publicStatus.js';
import { registerMiscRoutes } from './misc.js';

const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];
const local = databaseUrl && ['127.0.0.1', 'localhost'].includes(new URL(databaseUrl).hostname);
const json = (value: unknown) => JSON.parse(JSON.stringify(value));
const key = (character: string) => character.repeat(64);

test('MQTT node SQL preserves the original scoped contract and reads changes immediately', {
  skip: local ? false : 'loopback TEST_INGEST_DATABASE_URL fixture required',
}, async (t) => {
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  t.after(async () => { await client.query('ROLLBACK'); await client.end(); });
  await client.query('BEGIN');
  // Everything, including the function's fixed search_path, belongs to a new
  // transaction-local fixture schema. Never use a production database URL.
  const schema = `mqtt_nodes_${process.pid}_${Date.now()}`;
  await client.query(`CREATE SCHEMA ${schema}; SET LOCAL search_path = ${schema}, public`);
  await client.query(`
    CREATE TABLE node_identity_aliases (source_node_id text PRIMARY KEY, canonical_node_id text);
    CREATE TABLE node_identity_nodes (node_id text PRIMARY KEY, name text);
    CREATE TABLE node_status_samples (
      time timestamptz, node_id text, network text, battery_mv integer, uptime_secs bigint,
      tx_air_secs double precision, rx_air_secs double precision,
      channel_utilization double precision, air_util_tx double precision, stats jsonb
    );
    CREATE TABLE packets (time timestamptz, rx_node_id text, network text, packet_hash text);
  `);
  const migration = await readFile(new URL('../../db/migrations/036_node_identity_merge.sql', import.meta.url), 'utf8');
  const functionStart = migration.indexOf('CREATE OR REPLACE FUNCTION meshcore_canonical_node_id');
  const functionEnd = migration.indexOf('$$;', functionStart) + 3;
  await client.query(migration.slice(functionStart, functionEnd).replace('SET search_path = public', `SET search_path = ${schema}`));
  const viewStart = migration.indexOf('CREATE VIEW node_identity_status_samples AS');
  await client.query(migration.slice(viewStart, migration.indexOf(';', viewStart) + 1));
  await client.query(`
    INSERT INTO node_identity_aliases VALUES ($1, $3), ($2, $3);
  `, [key('F'), key('E'), key('A')]);
  await client.query(`INSERT INTO node_identity_nodes VALUES
    ($1, 'Canonical observer'), ($2, NULL), ($3, 'Hidden 🚫'),
    ($4, 'Stale status'), ($5, 'No packets'), ($6, 'Test only')`,
  [key('A'), key('B'), key('C'), key('D'), key('1'), key('2')]);
  const status = async (node: string, network: string, minutes: number, uptime: number | null, battery: number | null) => {
    await client.query(`INSERT INTO node_status_samples VALUES
      (NOW() - $1 * INTERVAL '1 minute', $2, $3, $4, $5, 3, 4, 5, 6, '{"owner_secret":"hidden"}')`,
    [minutes, node, network, battery, uptime]);
  };
  await status(key('A'), 'ukmesh', 1, 10, 3500);
  await status(` ${key('F').toLowerCase()} `, 'northeast', 3, 7200, 3900);
  await status(key('E'), 'teesside', 4, 7200, 3800);
  await status(key('B'), 'ukmesh', 2, null, null);
  await status(key('C'), 'ukmesh', 2, 10, 3700);
  await status(key('D'), 'ukmesh', 16, 10, 3700);
  await status(key('1'), 'ukmesh', 2, 10, 3700);
  await status(key('A'), 'test', 1, 50, 3600);
  await status(key('2'), 'test', 1, 50, 3600);
  const packets = async (node: string | null, network: string, count = 1, hours = 1) => {
    // Repeated hashes are separate observations and must each be counted.
    await client.query(`INSERT INTO packets
      SELECT NOW() - $1 * INTERVAL '1 hour', $2, $3, 'same-hash'
      FROM generate_series(1, $4::integer)`, [hours, node, network, count]);
  };
  await packets(key('A'), 'ukmesh', 2);
  await packets(key('A').toLowerCase(), 'ukmesh');
  await packets(` ${key('F').toLowerCase()} `, 'northeast', 2);
  await packets(key('E'), 'teesside', 2);
  await packets(key('B'), 'teesside');
  await packets(key('C'), 'ukmesh');
  await packets(key('D'), 'ukmesh');
  await packets(key('A'), 'test', 20);
  await packets(key('2'), 'test');
  await packets(key('A'), 'ukmesh', 50, 25);
  await packets(null, 'ukmesh', 50);
  await packets('', 'ukmesh');
  await packets('   ', 'ukmesh');

  const beforeSql = await readFile(new URL('./fixtures/mqtt-nodes-before.sql', import.meta.url), 'utf8');
  let actualSql = '';
  let actualParams: unknown[] = [];
  const router = Router();
  const passThrough: RequestHandler = (_req, _res, next) => next();
  registerMiscRoutes(router, {
    query: async (sql, params) => {
      actualSql = sql;
      actualParams = params ?? [];
      return client.query(sql, params);
    },
    getRecentPackets: async () => [], getRecentPacketEvents: async () => [],
    getPacketDetail: async () => null, getChannelMessageHistory: async () => [],
    getRecentMessageTags: async () => [], getPublicVisibilityGeneration: async () => 1,
    packetDetailLimiter: passThrough as Parameters<typeof registerMiscRoutes>[1]['packetDetailLimiter'],
  });
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  t.after(() => server.close());
  await once(server, 'listening');
  const { port } = server.address() as AddressInfo;
  const read = async (scope = '') => {
    const response = await fetch(`http://127.0.0.1:${port}/mqtt-nodes${scope}`);
    assert.equal(response.status, 200);
    const rows = await response.json();
    const before = await client.query(beforeSql, actualParams);
    assert.deepEqual(rows, json(before.rows.filter((row) => Number(row.packets_24h) > 0).map(toPublicMqttNodeStatusDTO)));
    return rows as Array<Record<string, unknown>>;
  };
  await t.test('anonymous scope merges receiver aliases and all three production labels', async () => {
    const rows = await read();
    assert.deepEqual(rows.map((row) => [row.node_id, row.packets_24h]), [[key('A'), '7'], [key('B'), '1']]);
    assert.equal(rows[0]!.battery_mv, 3900, 'uptime takes precedence, then newest time breaks the tie');
    assert.equal(rows[0]!.uptime_secs, '7200', 'existing PostgreSQL bigint telemetry serialization is retained');
    assert.equal(rows[1]!.name, null);
    assert.equal(rows[1]!.battery_mv, null);
    assert.equal(rows[1]!.uptime_secs, null);
    assert.equal('stats' in rows[0]!, false);
    const types = await client.query(`SELECT DISTINCT pg_typeof(packets_24h)::text AS type FROM (${actualSql}) result`, actualParams);
    assert.deepEqual(types.rows, [{ type: 'bigint' }]);
  });
  await t.test('legacy labels use production scope and test traffic stays isolated', async () => {
    await read('?network=northeast');
    await read('?network=teesside');
    const rows = await read('?network=test');
    assert.deepEqual(rows.map((row) => [row.node_id, row.packets_24h]), [[key('2'), '1'], [key('A'), '20']]);
    assert.equal(rows[1]!.battery_mv, 3600);
  });
  await t.test('ingest, alias reassignment and visibility changes are reflected on the next read', async () => {
    await packets(key('F'), 'ukmesh');
    assert.equal((await read())[0]!.packets_24h, '8');
    await client.query('UPDATE node_identity_aliases SET canonical_node_id = $1 WHERE source_node_id = $2', [key('B'), key('F')]);
    assert.deepEqual((await read()).map((row) => [row.node_id, row.packets_24h]), [[key('A'), '5'], [key('B'), '4']]);
    await client.query("UPDATE node_identity_nodes SET name = 'Now private 🚫' WHERE node_id = $1", [key('A')]);
    assert.deepEqual((await read()).map((row) => row.node_id), [key('B')]);
  });
  await t.test('no recent status preserves the empty array response', async () => {
    await client.query('DELETE FROM node_status_samples');
    assert.deepEqual(await read(), []);
  });
});
