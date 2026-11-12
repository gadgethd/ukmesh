import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import pg from 'pg';
import { bootstrapBaseSchemaFixture } from '../../db/fixtures/baseSchema.js';
import { PUBLIC_REVALIDATE } from '../../http/publicJson.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { readPublicResponseGeneration } from '../../privacy/responseGeneration.js';
import { generateSampleDataset } from '../../tools/generateSampleDataset.js';
import { loadSampleDataset } from '../../tools/loadSampleDataset.js';
import { prepareSampleDataset } from '../../tools/prepareSampleDataset.js';
import { assertLocalDatabaseUrl } from '../../tools/sampleData.js';
import { registerActivityTimelineRoutes } from './activityTimeline.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];
test('real timeline SQL preserves alias weights, duplicate receptions and null observers, and fences privacy races', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 180_000,
}, async t => {
  assertLocalDatabaseUrl(endpoint!);
  t.mock.timers.enable({ apis: ['Date'], now: Date.now() });
  t.mock.method(console, 'log', () => undefined); t.mock.method(console, 'error', () => undefined);
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `timeline_delivery_${process.pid}_${Date.now()}`, url = new URL(endpoint!); url.pathname = '/' + name;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 3 });
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-timeline-delivery-'));
  t.after(async () => {
    await pool.end(); await admin.query(`DROP DATABASE ${name} WITH (FORCE)`); await admin.end();
    await rm(directory, { recursive: true, force: true });
  });
  await bootstrapBaseSchemaFixture(pool);
  await generateSampleDataset(['--data-dir', directory, '--count', '25']);
  await loadSampleDataset(['--data-dir', directory, '--database-url', url.href, '--reset', '--fast']);
  const receiver = '0102' + '11'.repeat(30), alias = 'AABB' + '22'.repeat(30), peer = 'CCDD' + '33'.repeat(30);
  await pool.query(`INSERT INTO core.node_identity_aliases
    (source_node_id,canonical_node_id,confidence,reason,source_kind)
    VALUES ($1,$2,'high','invented timeline fixture','manual')`, [alias, receiver]);
  await pool.query(`WITH numbered AS (
    SELECT time,packet_hash,row_number() OVER (ORDER BY packet_hash) AS n FROM mesh.packets
  ) UPDATE mesh.packets p SET
    time=date_bin(INTERVAL '15 minutes',NOW(),TIMESTAMPTZ '2000-01-01')-INTERVAL '1 minute',
    rx_node_id=CASE WHEN n.n<=12 THEN $1 WHEN n.n<=24 THEN $2 ELSE NULL END,
    src_node_id=CASE WHEN n.n<=5 THEN $3 WHEN n.n=6 THEN $2 ELSE NULL END
    FROM numbered n WHERE p.time=n.time AND p.packet_hash=n.packet_hash`, [receiver,alias,peer]);
  await pool.query(`INSERT INTO mesh.packets
    (time,packet_hash,rx_node_id,src_node_id,topic,topic_prefix,network,packet_type,route_type,
     hop_count,path_hashes,path_hash_size_bytes,visibility_ok,is_private)
    SELECT time+INTERVAL '1 microsecond',packet_hash,$1,NULL,topic,topic_prefix,network,packet_type,
      route_type,hop_count,path_hashes,path_hash_size_bytes,TRUE,FALSE FROM mesh.packets LIMIT 1`, [alias]);
  await pool.query(`INSERT INTO mesh.packets
    (time,packet_hash,rx_node_id,src_node_id,topic,topic_prefix,network,packet_type,route_type,
     hop_count,path_hashes,path_hash_size_bytes,visibility_ok,is_private)
    SELECT time-INTERVAL '15 minutes',$1,NULL,$2,topic,topic_prefix,network,packet_type,
      route_type,hop_count,path_hashes,path_hash_size_bytes,TRUE,FALSE FROM mesh.packets LIMIT 1`, ['F'.repeat(64),peer]);
  await prepareSampleDataset(url.href);
  let hold = false, captured!: () => void, release!: () => void;
  const reached = new Promise<void>(resolve => { captured = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  t.after(() => release());
  const query = async <T extends pg.QueryResultRow>(sql: string, params?: unknown[]) => {
    const result = await pool.query<T>(sql, params);
    if (hold && sql.includes('WITH scoped AS MATERIALIZED')) { captured(); await gate; }
    return result;
  };
  const router = Router(), limiter: RequestHandler = (_req, _res, next) => next();
  const shutdownCache = registerActivityTimelineRoutes(router, {
    query, networkFilters, getPublicVisibilityGeneration: () => readPublicResponseGeneration(query),
    limiter: limiter as Parameters<typeof registerActivityTimelineRoutes>[1]['limiter'],
  });
  t.after(shutdownCache);
  const app = express(); app.use(router);
  const server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  const target = `http://127.0.0.1:${(server.address() as AddressInfo).port}/activity/timeline?network=ukmesh`;
  const first = await fetch(target); assert.equal(first.status, 200);
  assert.equal(first.headers.get('cache-control'), PUBLIC_REVALIDATE);
  const body = await first.json();
  assert.equal(body.buckets.length, 2);
  assert.deepEqual(body.buckets.map((bucket: { packetCount: number; observerCount: number; activeNodeIds: string[] }) => ({
    packetCount: bucket.packetCount, observerCount: bucket.observerCount, activeNodeIds: bucket.activeNodeIds,
  })), [
    { packetCount: 1, observerCount: 0, activeNodeIds: [peer] },
    { packetCount: 25, observerCount: 1, activeNodeIds: [receiver,peer] },
  ]);
  const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': first.headers.get('etag')! } };
  assert.equal((await fetch(target, conditional)).status, 304);
  t.mock.timers.setTime(Date.now() + 91_000); // Expire the complete snapshot before holding a cold load.
  hold = true; const racing = fetch(target, conditional); await reached;
  const writer = new pg.Client({ connectionString: url.href }); await writer.connect();
  try { await writer.query("UPDATE core.nodes SET name='Invented private peer 🚫' WHERE node_id=$1", [peer]); }
  finally { await writer.end(); }
  hold = false; release();
  const withheld = await racing; assert.equal(withheld.status, 500);
  assert.equal(withheld.headers.get('cache-control'), 'no-store');
  assert.ok(!JSON.stringify(await withheld.json()).includes(peer));
  const pending = await fetch(target, conditional); assert.equal(pending.status, 500);
  assert.equal(pending.headers.get('cache-control'), 'no-store');
  await prepareSampleDataset(url.href);
  const repaired = await fetch(target, conditional); assert.equal(repaired.status, 200);
  assert.equal(repaired.headers.get('cache-control'), PUBLIC_REVALIDATE);
  assert.deepEqual((await repaired.json()).buckets, []);
});
