import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import pg from 'pg';
import { Redis } from 'ioredis';
import { WebSocket } from 'ws';
import { runMigrations } from '../db/migrations.js';

const databaseURL = process.env['TEST_INGEST_DATABASE_URL'];
const redisURL = process.env['TEST_REDIS_URL'];
const local = (url?: string) => url && ['127.0.0.1', 'localhost'].includes(new URL(url).hostname);
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test('F01 ready WebSocket batches and cached initial state obey current authority', {
  skip: local(databaseURL) && local(redisURL) ? false : 'loopback PG/Redis fixtures required', timeout: 40_000,
}, async () => {
  const admin = new pg.Pool({ connectionString: databaseURL, max: 1 });
  const name = `meshcore_ws_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(databaseURL!); url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  await pool.query(fs.readFileSync(new URL('../db/schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  const observer = 'AA'.repeat(32), source = 'CC'.repeat(32);
  await pool.query(`INSERT INTO nodes(node_id, public_key, network, name) VALUES
    ($1, $1, 'teesside', 'Observer'), ($2, $2, 'teesside', 'Relay')`, [observer, 'BB'.repeat(32)]);
  process.env['DATABASE_URL'] = url.toString();
  process.env['REDIS_URL'] = redisURL;
  delete process.env['REDIS_PASSWORD'];
  process.env['WS_INITIAL_STATE_ENABLED'] = '1';
  process.env['WARMUP_NETWORKS'] = '';
  process.env['MQTT_INGEST_MODE'] = 'ram';
  const { initWebSocketServer, closeWebSocketServer } = await import('./server.js');
  const { closeDb } = await import('../db/index.js');
  const { configureRamStore } = await import('../ram/liveStore.js');
  const pub = new Redis(redisURL!);
  await pub.ping();
  configureRamStore(pub);
  const server = http.createServer();
  const wss = initWebSocketServer(server);
  const clients: WebSocket[] = [];
  try {
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const address = server.address(); assert.ok(address && typeof address !== 'string');
    const socketURL = `ws://127.0.0.1:${address.port}/ws?network=ukmesh`;
    const client = new WebSocket(socketURL); clients.push(client);
    const seen: string[] = [];
    client.on('message', (raw) => {
      for (const line of raw.toString().split('\n')) {
        const message = JSON.parse(line);
        if (message.type === 'packet') seen.push(message.data.packetHash);
      }
    });
    const [initial] = await once(client, 'message');
    assert.equal(JSON.parse(initial.toString()).type, 'initial_state');
    for (let attempt = 0; attempt < 100; attempt++) {
      if (Number((await pub.call('PUBSUB', 'NUMSUB', 'meshcore:live') as string[])[1]) === 1) break;
      await delay(10);
    }
    const packet = (hash: string, extra = {}) => JSON.stringify({ type: 'packet', ts: Date.now(),
      data: { id: hash, packetHash: hash, topic: `meshcore/ZZZ/${observer}/packets`,
        packetType: 5, network: 'northeast', rxNodeId: observer, srcNodeId: source,
        visibilityOk: true, isPrivate: false, ...extra } });
    for (let attempt = 0; attempt < 100 && !seen.includes('ready'); attempt++) {
      await pub.publish('meshcore:live', packet('ready')); await delay(50);
    }
    assert.ok(seen.includes('ready'), 'prove the subscribed reader can receive before changing policy');
    // Frozen vo=1 and a stale WS private-name index cannot grant permission.
    await pool.query(`INSERT INTO private_node_prefixes(node_id, network, prefix_size_bytes, prefix)
      VALUES ($1, 'teesside', 1, 'AA'), ($2, 'teesside', 2, 'BBBB')`, [observer, 'BB'.repeat(32)]);
    await pool.query('UPDATE public_visibility_state SET generation = generation + 1 WHERE singleton');
    await pool.query(`UPDATE packet_visibility_materialization_state SET visibility_generation =
      (SELECT generation FROM public_visibility_state WHERE singleton) WHERE singleton`);
    await pub.publish('meshcore:live', packet('revoked-observer'));
    await pub.publish('meshcore:live', packet('revoked-path', { rxNodeId: source, path: ['BBBB'], pathHashSizeBytes: 2 }));
    await delay(150);
    assert.equal(seen.some((hash) => hash.startsWith('revoked-')), false, 'privacy changes must fence queued/live data');
    const burst = Array.from({ length: 800 }, (_, i) => `burst-${i}`);
    await Promise.all(burst.map((hash) => pub.publish('meshcore:live', packet(hash, { rxNodeId: source }))));
    for (let attempt = 0; attempt < 200 && seen.filter((h) => h.startsWith('burst-')).length < 800; attempt++) await delay(10);
    assert.deepEqual(new Set(seen.filter((h) => h.startsWith('burst-'))), new Set(burst), 'a ready, bounded reader receives the full batch');
    // This DB is owned by the test. Missing authority suppresses broadcasts
    // and cached initial data, even though the cached epoch was once public.
    await pool.query('ALTER TABLE public_visibility_state RENAME TO public_visibility_unavailable');
    await pub.publish('meshcore:live', packet('unavailable', { rxNodeId: source }));
    await delay(150);
    assert.equal(seen.includes('unavailable'), false);
    const cachedClient = new WebSocket(socketURL); clients.push(cachedClient);
    const [code] = await once(cachedClient, 'close');
    assert.equal(code, 1013, 'unavailable authority cannot reuse cached initial state');
  } finally {
    clients.forEach((client) => client.terminate());
    await closeWebSocketServer(wss);
    configureRamStore(null);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await pub.quit(); await closeDb(); await pool.end();
    await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`); await admin.end();
  }
});
