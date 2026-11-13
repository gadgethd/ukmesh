import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { createDatabaseRuntime } from './databaseRuntime.js';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from './fixtures/baseSchema.js';
import { persistEntriesCore } from './packetBatch.js';
import { buildPacketBatchInput } from './packetInput.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
test('prepared packet batches preserve fresh values, privacy generations, chunk routing and idempotent replay', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 120_000,
}, async t => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `prepared_batch_${process.pid}_${Date.now()}`, url = new URL(endpoint!); url.pathname = '/' + name;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 2, options: POST_MOVE_SEARCH_PATH });
  const runtime = createDatabaseRuntime({ DATABASE_URL: url.href, DATABASE_POOL_MAX: '1' });
  let reserved: pg.PoolClient | undefined;
  t.after(async () => { reserved?.release(); await runtime.close(); await pool.end(); await admin.query(`DROP DATABASE ${name} WITH (FORCE)`); await admin.end(); });
  await bootstrapBaseSchemaFixture(pool);
  for (const prefix of ['AA', 'BB', 'CC']) await pool.query(`INSERT INTO core.nodes(node_id,name,lat,lon,role,network)
    VALUES($1,'Invented prepared-batch relay',52,-1,2,'ukmesh')`, [prefix.repeat(32)]);
  while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* certify fixture */ }
  // Prepared plans belong to a connection. Hold the spare slot so concurrent
  // prefix refreshes cannot switch the writer to another session mid-proof.
  reserved = await runtime.ingestPool.connect();
  const entries = Array.from({ length: 18 }, (_, i) => ({ observationId: randomUUID(), input: { ...buildPacketBatchInput({
    packetHash: (i + 1).toString(16).padStart(64, '0'),
    rxNodeId: 'AA'.repeat(32), topic: `meshcore/LHR/${'AA'.repeat(32)}/packets`,
    network: i % 2 ? 'teesside' : 'ukmesh', packetType: 3, routeType: 1,
    pathHashes: ['BBBB', 'CCCC'], pathHashSizeBytes: 2, hopCount: 2, rawHex: '00', payload: { direction: 'rx' },
  }), time: new Date(Date.now() - i * 86_400_000) } }));
  for (const [index, entry] of entries.entries()) {
    if (index === 9) {
      await pool.query("UPDATE core.nodes SET name='Private 🚫 fixture relay' WHERE node_id=$1", ['BB'.repeat(32)]);
      while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* new generation */ }
    }
    const result = await persistEntriesCore([entry], runtime.ingestQuery, false, runtime.withIngestTransaction);
    assert.equal(result[0]?.is_private, index >= 9, 'a reused statement must read the current privacy generation');
  }
  for (const entry of entries) await persistEntriesCore([entry], runtime.ingestQuery, true, runtime.withIngestTransaction);
  const rows = await pool.query(`SELECT packet_hash,network,time,observation_id::text FROM mesh.packets ORDER BY packet_hash`);
  assert.deepEqual(rows.rows, entries.map(({ observationId, input }) => ({
    packet_hash: input.packetHash, network: input.network, time: input.time, observation_id: observationId,
  })));
  assert.equal(Number((await pool.query('SELECT count(*) FROM mesh.ingest_observation_ledger')).rows[0].count), entries.length);
  const prepared = (await runtime.ingestQuery(`SELECT name, (generic_plans+custom_plans)::int AS executions
    FROM pg_prepared_statements ORDER BY name`)).rows;
  assert.deepEqual(prepared.map(row => row.name), ['packet_batch_single_idempotent_v1', 'packet_batch_single_v1']);
  assert.ok(prepared.every(row => row.executions > 5), 'both fixed forms are reused beyond plan warmup on this connection');
});
