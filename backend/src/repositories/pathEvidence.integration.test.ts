import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { bootstrapBaseSchemaFixture } from '../db/fixtures/baseSchema.js';
import { assertLocalDatabaseUrl } from '../tools/sampleData.js';
import { reactivateHistoricPathNodes } from './pathEvidence.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];
test('best-effort historic reactivation skips busy relays and resumes after the writer unlocks', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 120_000,
}, async t => {
  assertLocalDatabaseUrl(endpoint!);
  t.mock.method(console, 'log', () => undefined);
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `path_hint_${process.pid}_${Date.now()}`, url = new URL(endpoint!); url.pathname = '/' + name;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 1 });
  const writer = new pg.Client({ connectionString: url.href });
  t.after(async () => {
    await writer.end(); await pool.end();
    await admin.query(`DROP DATABASE ${name} WITH (FORCE)`); await admin.end();
  });
  await bootstrapBaseSchemaFixture(pool);
  const busy = 'AABB' + '22'.repeat(30), free = 'CCDD' + '33'.repeat(30);
  await pool.query(`INSERT INTO core.nodes (node_id,name,lat,lon,role,network)
    VALUES ($1,'Invented busy relay',51.51,-0.12,2,'ukmesh'),
           ($2,'Invented free relay',51.52,-0.08,2,'ukmesh')`, [busy,free]);
  await pool.query("SET statement_timeout = '1000ms'");
  await writer.connect(); await writer.query('BEGIN');
  await writer.query('SELECT node_id FROM core.nodes WHERE node_id=$1 FOR UPDATE', [busy]);
  const seenAt = new Date(Date.now() + 60_000);
  const first = await reactivateHistoricPathNodes(pool.query.bind(pool), {
    pathHashes: ['AABB','CCDD'], sizeBytes: 2, seenAt, routeType: 0, network: 'ukmesh',
  });
  assert.deepEqual(first.map(node => node.node_id), [free]);
  assert.equal((await pool.query('SELECT last_path_evidence_at FROM core.nodes WHERE node_id=$1', [busy])).rows[0].last_path_evidence_at, null);
  await writer.query('ROLLBACK');
  const second = await reactivateHistoricPathNodes(pool.query.bind(pool), {
    pathHashes: ['CCDD','AABB'], sizeBytes: 2, seenAt: new Date(seenAt.getTime()+1), routeType: 1, network: 'ukmesh',
  });
  assert.deepEqual(second.map(node => node.node_id), [busy,free]);
  assert.ok(second.every(node => new Date(node.last_seen!).getTime() === seenAt.getTime()+1));
});
