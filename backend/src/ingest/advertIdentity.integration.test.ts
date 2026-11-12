import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import test from 'node:test';
import pg from 'pg';
import { bootstrapBaseSchemaFixture } from '../db/fixtures/baseSchema.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
const run = promisify(execFile);

test('verified advert identity and canonical counts survive duplicate observers and processor recreation', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `advert_identity_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(endpoint!); url.pathname = '/' + name;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 1 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });
  await bootstrapBaseSchemaFixture(pool);
  // Execute the production processor with its actual process composition in a
  // fresh process. The parent owns only bootstrap and final database inspection.
  const source = `
    import assert from 'node:assert/strict';
    import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
    import { createPacketProcessor } from './src/ingest/packetProcessor.ts';
    import { createIngestEvents } from './src/ingest/events.ts';
    import { decodePacketCompat, verifyAdvertSignature } from './src/mqtt/decodePacket.ts';
    import { initializePacketPersistence } from './src/db/packetPersistence.ts';
    import { closePacketBatch } from './src/db/packetBatch.ts';
    import { upsertNode } from './src/repositories/nodeState.ts';
    import { query, closeDb } from './src/db/runtime.ts';
    const raw = '11007E7662676F7F0850A8A355BAAFBFC1EB7B4174C340442D7D7161C9474A2C94006CE7CF682E58408DD8FCC51906ECA98EBF94A037886BDADE7ECD09FD92B839491DF3809C9454F5286D1D3370AC31A34593D569E9A042A3B41FD331DFFB7E18599CE1E60992A076D50238C5B8F85757375354522F50756765744D65736820436F75676172';
    const keys = MeshCoreDecoder.createKeyStore({ channelSecrets: [] });
    const decoded = decodePacketCompat(raw, keys);
    assert.equal(await verifyAdvertSignature(decoded.decoded), true);
    const sourceKey = decoded.decoded.payload.decoded.publicKey.toUpperCase();
    const observer = 'A'.repeat(64), other = 'B'.repeat(64);
    const events = createIngestEvents();
    const processors = [];
    const create = () => {
      const processor = createPacketProcessor({ clientId: 'owned-advert', events,
        markMqttPersistenceHealthy() {}, committedFollowUps: { schedule() {}, start() {}, async close() {} } });
      processors.push(processor); return processor;
    };
    const send = (processor, raw, observerId = observer, prefix = 'meshcore') => processor.process({
      topic: prefix + '/LHR/' + observerId + '/packets',
      payload: JSON.stringify({ raw, packet_type: '4', direction: 'rx' }),
    });
    const count = async () => Number((await query('SELECT advert_count FROM core.nodes WHERE node_id=$1', [sourceKey])).rows[0].advert_count);
    initializePacketPersistence();
    try {
      const first = create();
      await send(first, raw);
      await send(first, raw, other);
      await send(first, raw);
      assert.equal(await count(), 1, 'multi-observer duplicates count one canonical advert');
      await first.close();
      const restarted = create();
      await send(restarted, raw);
      assert.equal(await count(), 1, 'counting survives recreation without an in-memory hash registry');
      const tampered = raw.slice(0, -2) + '00';
      assert.equal(await verifyAdvertSignature(decodePacketCompat(tampered, keys).decoded), false);
      await send(restarted, tampered);
      await send(restarted, raw, observer, 'meshcore-test');
      assert.equal(await count(), 1, 'invalid signatures and test traffic cannot increment source adverts');
      const invalid = await query('SELECT src_node_id FROM mesh.packets WHERE packet_hash=$1', [decodePacketCompat(tampered, keys).canonicalPacketId]);
      assert.ok(invalid.rows.length > 0);
      assert.ok(invalid.rows.every(row => row.src_node_id === null), 'invalid signatures cannot establish identity');
      await upsertNode(sourceKey, { network: 'ukmesh', advertHash: 'FF'.repeat(32) });
      await upsertNode(sourceKey, { network: 'ukmesh', advertHash: 'FF'.repeat(32) });
      await upsertNode(sourceKey, { network: 'ukmesh' });
      assert.equal(await count(), 2, 'positionless UPSERT counts once and plain observer touches do not count');
    } finally {
      await Promise.all(processors.map(processor => processor.close()));
      events.close();
      await closePacketBatch();
      await closeDb();
    }
  `;
  await run(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', source], {
    env: { ...process.env, DATABASE_URL: url.toString(), MESHCORE_HTTP_DATABASE_URL: url.toString(),
      MESHCORE_INGEST_DATABASE_URL: url.toString(), MESHCORE_WORKER_DATABASE_URL: url.toString(),
      MQTT_INGEST_MODE: 'durable', PATH_BETA_PRERESOLVE_ENABLED: '0' }, timeout: 20_000,
  });
  const counts = await pool.query('SELECT advert_count FROM core.nodes WHERE advert_count > 0');
  assert.deepEqual(counts.rows, [{ advert_count: 2 }]);
  assert.equal(Number((await pool.query('SELECT count(*) FROM core.node_counted_adverts')).rows[0].count), 2);
});
