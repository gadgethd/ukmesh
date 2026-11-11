import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import pg from 'pg';
import { Redis } from 'ioredis';
import { WebSocket } from 'ws';
import { runMigrations } from '../db/migrations.js';

const databaseURL = process.env['TEST_REGIONS_DATABASE_URL'];
const redisURL = process.env['TEST_REGIONS_REDIS_URL'];
const local = (url?: string) => url && ['127.0.0.1', 'localhost'].includes(new URL(url).hostname);
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test('real regional sockets isolate snapshots, caches, live packets and node membership', {
  skip: local(databaseURL) && local(redisURL) ? false : 'disposable loopback PG/Redis fixtures required', timeout: 60_000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: databaseURL, max: 1 });
  const name = `meshcore_region_ws_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(databaseURL!); url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });
  await pool.query(fs.readFileSync(new URL('../db/schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  const a = 'A'.repeat(64), b = 'B'.repeat(64), c = 'C'.repeat(64);
  await pool.query(`INSERT INTO nodes(node_id, network, name, role, lat, lon)
    VALUES ($1, 'ukmesh', 'Europe relay', 2, 54, -1), ($2, 'ukmesh', 'eng-ne relay', 2, 55, -2)`, [a, b]);
  await pool.query(`INSERT INTO region_scope_stats(region) VALUES ('Europe'), ('eng-ne')`);
  await pool.query(`INSERT INTO region_scope_members(region, node_id, packets) VALUES ('Europe', $1, 4), ('eng-ne', $2, 4)`, [a, b]);
  await pool.query(`INSERT INTO packets(packet_hash, rx_node_id, topic, network, packet_type, region_scope, path_hashes, path_hash_size_bytes)
    VALUES ('Europe', $1, 'meshcore/ZZZ/fixture/packets', 'ukmesh', 5, '#Europe', ARRAY[]::text[], 1),
           ('eng-ne', $2, 'meshcore/ZZZ/fixture/packets', 'ukmesh', 5, '#eng-ne', ARRAY[]::text[], 1)`, [a, b]);
  process.env['DATABASE_URL'] = url.toString();
  process.env['REDIS_URL'] = redisURL;
  delete process.env['REDIS_PASSWORD'];
  process.env['WS_INITIAL_STATE_ENABLED'] = '1';
  process.env['WARMUP_NETWORKS'] = '';
  process.env['MQTT_INGEST_MODE'] = 'durable';
  process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = 'false';
  const { initWebSocketServer, closeWebSocketServer } = await import('./server.js');
  const { closeDb } = await import('../db/index.js');
  const { processNextPrivacyRematerialization } = await import('../workers/privacy-rematerialization.js');
  while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* fixture only */ }
  const pub = new Redis(redisURL!);
  const server = http.createServer();
  const wss = initWebSocketServer(server);
  const clients: WebSocket[] = [];
  try {
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const address = server.address(); assert.ok(address && typeof address !== 'string');
    const base = `ws://127.0.0.1:${address.port}/ws?network=ukmesh`;
    const denied = new WebSocket(`${base}&region=Europe`);
    denied.on('error', () => {});
    const [, response] = await once(denied, 'unexpected-response');
    assert.equal(response.statusCode, 400);
    denied.terminate();
    process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = 'true';
    const connect = async (region?: string) => {
      const socket = new WebSocket(`${base}${region ? `&region=${region}` : ''}`);
      clients.push(socket);
      const seen: Array<{ type: string; data: Record<string, unknown> }> = [];
      socket.on('message', (raw) => {
        for (const line of raw.toString().split('\n')) seen.push(JSON.parse(line));
      });
      const [raw] = await once(socket, 'message');
      const initial = JSON.parse(raw.toString());
      assert.equal(initial.type, 'initial_state');
      return { seen, initial: initial.data };
    };
    const all = await connect(), europe = await connect('Europe'), eng = await connect('eng-ne');
    const unknown = await connect('unknown'), cachedEurope = await connect('Europe');
    assert.equal(all.initial.nodes.length, 2);
    for (const scoped of [europe, cachedEurope]) {
      assert.deepEqual(scoped.initial.nodes.map((node: { node_id: string }) => node.node_id), [a]);
      assert.deepEqual(scoped.initial.packets.map((packet: { packet_hash: string }) => packet.packet_hash), ['Europe']);
      assert.deepEqual(scoped.initial.messages.map((packet: { packet_hash: string }) => packet.packet_hash), ['Europe']);
    }
    assert.deepEqual(eng.initial.nodes.map((node: { node_id: string }) => node.node_id), [b]);
    assert.deepEqual(unknown.initial.nodes, []);
    assert.deepEqual(unknown.initial.packets, []);
    for (let i = 0; i < 100; i++) {
      if (Number((await pub.call('PUBSUB', 'NUMSUB', 'meshcore:live') as string[])[1]) === 1) break;
      await delay(10);
    }
    for (const [hash, regionScope] of [['europe-live', '#Europe'], ['eng-ne-live', '#eng-ne'], ['unscoped-live', undefined]]) {
      await pub.publish('meshcore:live', JSON.stringify({ type: 'packet', ts: Date.now(), data: {
        id: hash, packetHash: hash, topic: 'meshcore/ZZZ/fixture/packets', network: 'ukmesh',
        rxNodeId: c.toLowerCase(), packetType: 5, regionScope,
      } }));
    }
    const hashes = (client: typeof all) => client.seen.filter((message) => message.type === 'packet').map((message) => message.data['packetHash']);
    for (let i = 0; i < 100 && hashes(all).length < 3; i++) await delay(10);
    assert.equal(hashes(all).length, 3);
    assert.deepEqual(hashes(europe), ['europe-live']);
    assert.deepEqual(hashes(eng), ['eng-ne-live']);
    assert.deepEqual(hashes(unknown), []);
    for (const node_id of [a, b, c]) {
      await pub.publish('meshcore:live', JSON.stringify({ type: 'node_upsert', ts: Date.now(), data: {
        node_id: node_id.toLowerCase(), network: 'ukmesh', name: 'Updated public node',
      } }));
    }
    const upserts = (client: typeof all) => client.seen.filter((message) => message.type === 'node_upsert').map((message) => message.data['node_id']);
    for (let i = 0; i < 100 && upserts(all).length < 3; i++) await delay(10);
    assert.deepEqual(upserts(europe), [a.toLowerCase()]);
    assert.deepEqual(upserts(eng), [b.toLowerCase()]);
  } finally {
    clients.forEach((client) => client.terminate());
    await closeWebSocketServer(wss);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await pub.quit();
    await closeDb();
  }
});
