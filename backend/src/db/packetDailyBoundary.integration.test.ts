import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { randomUUID } from 'node:crypto';
import { runMigrations } from './migrations.js';
import { persistEntriesCore, type PacketBatchInput, type TransactionRunner } from './packetBatch.js';

const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];
test('one batch keeps the maximum for every network and UTC day, including replay', {
  skip: databaseUrl ? false : 'TEST_INGEST_DATABASE_URL is not configured',
}, async (t) => {
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  const database = `meshcore_daily_${process.pid}_${Date.now()}`;
  const url = new URL(databaseUrl!); url.pathname = `/${database}`;
  await admin.query(`CREATE DATABASE "${database}"`);
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE "${database}" WITH (FORCE)`);
    await admin.end();
  });
  await pool.query(fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  // Public reads must also work when the optional tagger has never started.
  const db = await import('./index.js');
  try {
    assert.deepEqual(await db.getRecentPackets(5, 'ukmesh', undefined, 'slim', undefined, (sql, params) => pool.query(sql, params)), []);
    assert.deepEqual(await db.getRecentMessages(5, 'ukmesh', undefined, undefined, (sql, params) => pool.query(sql, params)), []);
  } finally { await db.closeDb(); }
  const transaction: TransactionRunner = async (work) => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work((sql, params) => client.query(sql, params));
      await client.query('COMMIT');
      return result;
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  };
  const input = (network: string, time: string, hopCount: number): PacketBatchInput => ({
    network, time: new Date(time), hopCount, packetHash: randomUUID(),
    rxNodeId: 'A'.repeat(64), srcNodeId: null,
    topic: `meshcore/LHR/${'A'.repeat(64)}/packets`, topicPrefix: 'meshcore', iata: 'LHR',
    packetType: 3, routeType: 1, rssi: -90, snr: 5, payloadJson: '{}', companionSender: null,
    rawHex: '0D00DEADBEEF', advertCount: null, pathHashes: null,
    pathHashSizeBytes: null, transportCodes: null, regionScope: null,
  });
  const entries = [
    input('ukmesh', '2026-10-30T23:59:59.999Z', 3),
    input('ukmesh', '2026-10-31T00:00:00.001Z', 5),
    input('ukmesh', '2026-10-30T23:59:59.998Z', 7),
    input('teesside', '2026-10-30T23:59:59.999Z', 2),
    input('teesside', '2026-10-31T00:00:00.001Z', 4),
  ].map((input) => ({ input, observationId: randomUUID() }));
  const query = (sql: string, params?: unknown[]) => pool.query(sql, params);
  await persistEntriesCore(entries, query, false, transaction);
  const daily = () => pool.query(`SELECT network, day::text, max_hop_count
    FROM packet_daily_stats ORDER BY network, day`);
  const expected = [
    { network: 'teesside', day: '2026-10-30', max_hop_count: 2 },
    { network: 'teesside', day: '2026-10-31', max_hop_count: 4 },
    { network: 'ukmesh', day: '2026-10-30', max_hop_count: 7 },
    { network: 'ukmesh', day: '2026-10-31', max_hop_count: 5 },
  ];
  assert.deepEqual((await daily()).rows, expected);
  await persistEntriesCore(entries, query, true, transaction);
  assert.deepEqual((await daily()).rows, expected);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS n FROM packets')).rows[0].n, entries.length);
  await t.test('message pages aggregate every sighting and include captured overlap outside the page', async () => {
    const now = Date.now();
    for (let i = 0; i < 6; i += 1) {
      await pool.query(`INSERT INTO packets (time, packet_hash, rx_node_id, topic, network, packet_type, payload)
        VALUES ($1, $2, $3, 'meshcore/LHR/fixture/packets', 'ukmesh', 5, $4::jsonb)`,
        [new Date(now - i * 1000), i < 3 ? 'page-leader' : `older-${i}`, String.fromCharCode(65+i).repeat(64),
          JSON.stringify({ direction: i === 1 ? 'tx' : 'rx' })]);
    }
    const read = () => db.getRecentMessages(2, 'ukmesh', undefined, undefined, query);
    const page = await read();
    assert.deepEqual(page.map((row) => row.packet_hash), ['page-leader', 'older-3']);
    assert.equal(page[0].rx_count, 2);
    assert.equal(page[0].tx_count, 1);
    assert.equal(page[0].observer_node_ids.length, 3);
    const oldMode = process.env['MQTT_INGEST_MODE'];
    process.env['MQTT_INGEST_MODE'] = 'ram';
    try {
      const overlap = await db.getRecentMessages(2, 'ukmesh', undefined, undefined, query, ['older-5']);
      assert.deepEqual(overlap.map((row) => row.packet_hash), ['page-leader', 'older-3', 'older-5']);
      assert.equal(overlap[0]._observation_ids.length, 3);
    } finally {
      if (oldMode === undefined) delete process.env['MQTT_INGEST_MODE']; else process.env['MQTT_INGEST_MODE'] = oldMode;
    }
  });
});
