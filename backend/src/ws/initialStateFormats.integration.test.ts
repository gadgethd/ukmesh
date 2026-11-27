import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import pg from 'pg';
import { WebSocket } from 'ws';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from '../db/fixtures/baseSchema.js';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { convergePacketVisibility } from '../privacy/convergence.js';
import { decodeInitialStateColumns, INITIAL_STATE_COLUMNS_FORMAT, INITIAL_STATE_FORMAT_PARAMETER } from '../contracts/initialStateColumns.generated.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];
const redis = process.env['TEST_REDIS_URL'];
const localRedis = redis && ['127.0.0.1', 'localhost'].includes(new URL(redis).hostname);

test('real bootstrap formats retain rows, byte limits and cached privacy authority', {
  skip: endpoint && localRedis ? false : 'owned loopback PostgreSQL and Redis required', timeout: 120_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'ws_columns');
  const writer = new pg.Pool({ connectionString: fixture.url, max: 2, options: POST_MOVE_SEARCH_PATH });
  fixture.addCleanup(() => writer.end());
  await bootstrapBaseSchemaFixture(writer);
  const id = (n: number) => n.toString(16).padStart(64, '0');
  await writer.query(`INSERT INTO core.nodes(node_id, public_key, network, name, role, lat, lon)
    VALUES ($1, $1, 'ukmesh', 'Format fixture', 2, 54, -1)`, [id(1)]);
  const converge = async () => {
    const generation = String((await writer.query('SELECT generation::text FROM ops.public_visibility_state WHERE singleton')).rows[0].generation);
    await convergePacketVisibility(writer, { apply: true, expectedGeneration: generation });
  };
  await converge();
  process.env['DATABASE_URL'] = fixture.url;
  process.env['REDIS_URL'] = redis;
  delete process.env['REDIS_PASSWORD'];
  process.env['WS_INITIAL_STATE_ENABLED'] = '1';
  process.env['WS_INITIAL_STATE_MAX_BYTES'] = '16384';
  process.env['WS_INITIAL_STATE_CONCURRENCY'] = '1';
  process.env['WS_INITIAL_STATE_QUEUE_MAX'] = '0';
  process.env['MQTT_INGEST_MODE'] = 'durable';
  process.env['WARMUP_NETWORKS'] = '';
  const { initWebSocketServer, closeWebSocketServer } = await import('./server.js');
  const { closeDb } = await import('../db/runtime.js');
  fixture.addCleanup(closeDb);
  const server = http.createServer(), wss = initWebSocketServer(server);
  const clients: WebSocket[] = [];
  fixture.addCleanup(async () => {
    clients.forEach(client => client.terminate());
    await closeWebSocketServer(wss);
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const url = (format?: string) => {
    const result = new URL(`ws://127.0.0.1:${address.port}/ws?network=ukmesh`);
    if (format) result.searchParams.set(INITIAL_STATE_FORMAT_PARAMETER, format);
    return result.href;
  };
  const connect = (format?: string) => { const client = new WebSocket(url(format)); clients.push(client); return client; };
  const state = async (format?: string) => {
    const client = connect(format);
    const [raw] = await once(client, 'message');
    const text = raw.toString(); client.close();
    return { text, value: JSON.parse(text) };
  };
  await t.test('concurrent formats share the one cold snapshot slot without queuing additional builds', async () => {
    const [legacy, unknown, columns] = await Promise.all([
      state(), state('future'), state(INITIAL_STATE_COLUMNS_FORMAT),
    ]);
    assert.equal(legacy.value.format, undefined);
    assert.deepEqual(unknown.value.data, legacy.value.data);
    assert.deepEqual(decodeInitialStateColumns(columns.value).data, legacy.value.data);
    assert.equal(legacy.value.data.nodes.length, 1);
    assert.ok(Buffer.byteLength(columns.text) <= 16384);
  });
  await writer.query(`INSERT INTO core.nodes(node_id, public_key, network, name, role, lat, lon)
    SELECT lpad(to_hex(n),64,'0'), lpad(to_hex(n),64,'0'), 'ukmesh', 'Format fixture', 2, 54, -1
    FROM generate_series(2,64) n`);
  await writer.query('UPDATE ops.public_visibility_state SET generation = generation + 1 WHERE singleton');
  await converge();
  await t.test('complete compact snapshot fits while oversized legacy remains closed', async () => {
    const legacy = connect(); const [code] = await once(legacy, 'close');
    assert.equal(code, 1013);
    const columns = await state(INITIAL_STATE_COLUMNS_FORMAT);
    assert.ok(Buffer.byteLength(columns.text) <= 16384);
    const restored = decodeInitialStateColumns(columns.value);
    assert.equal(restored.data.nodes.length, 64);
    assert.deepEqual(new Set(restored.data.nodes.map(node => node['node_id'])),
      new Set(Array.from({ length: 64 }, (_, n) => id(n + 1).toUpperCase())));
    assert.ok(Buffer.byteLength(JSON.stringify(restored)) > 16384);
  });
  await t.test('missing authority refuses cached compact state', async () => {
    await writer.query('ALTER TABLE ops.public_visibility_state RENAME TO public_visibility_unavailable');
    try {
      const client = connect(INITIAL_STATE_COLUMNS_FORMAT);
      const [code] = await once(client, 'close'); assert.equal(code, 1013);
    } finally { await writer.query('ALTER TABLE ops.public_visibility_unavailable RENAME TO public_visibility_state'); }
  });
  await t.test('compact delivery itself still obeys the exact same byte cap', async () => {
    await writer.query("UPDATE core.nodes SET name = repeat('x', 1000)");
    await writer.query('UPDATE ops.public_visibility_state SET generation = generation + 1 WHERE singleton');
    await converge();
    const client = connect(INITIAL_STATE_COLUMNS_FORMAT);
    const [code] = await once(client, 'close'); assert.equal(code, 1013);
  });
});
