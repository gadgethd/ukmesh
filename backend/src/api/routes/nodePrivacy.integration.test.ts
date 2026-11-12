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
import { createNodeRepository } from '../../repositories/nodes.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { readPublicResponseGeneration } from '../../privacy/responseGeneration.js';
import { generateSampleDataset } from '../../tools/generateSampleDataset.js';
import { loadSampleDataset } from '../../tools/loadSampleDataset.js';
import { prepareSampleDataset } from '../../tools/prepareSampleDataset.js';
import { assertLocalDatabaseUrl } from '../../tools/sampleData.js';
import { PUBLIC_REVALIDATE } from '../../http/publicJson.js';
import { registerNodeRoutes } from './nodes.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];
test('real node HTTP reads revoke cached links and reject in-flight map data across privacy convergence', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 180_000,
}, async t => {
  assertLocalDatabaseUrl(endpoint!);
  t.mock.method(console, 'log', () => undefined); t.mock.method(console, 'error', () => undefined);
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `node_delivery_${process.pid}_${Date.now()}`, url = new URL(endpoint!); url.pathname = '/' + name;
  await admin.query(`CREATE DATABASE ${name}`);
  const pool = new pg.Pool({ connectionString: url.href, max: 3 });
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-node-delivery-'));
  t.after(async () => {
    await pool.end(); await admin.query(`DROP DATABASE ${name} WITH (FORCE)`); await admin.end();
    await rm(directory, { recursive: true, force: true });
  });
  await bootstrapBaseSchemaFixture(pool);
  await generateSampleDataset(['--data-dir', directory, '--count', '25']);
  await loadSampleDataset(['--data-dir', directory, '--database-url', url.href, '--reset', '--fast']);
  await prepareSampleDataset(url.href);
  let hold = false;
  let captured!: () => void, release!: () => void;
  const reached = new Promise<void>(resolve => { captured = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  t.after(() => release());
  const query = async <T extends pg.QueryResultRow>(sql: string, params?: unknown[]) => {
    const result = await pool.query<T>(sql, params);
    if (hold && sql.includes('core.node_map_suppressions')) { captured(); await gate; }
    return result;
  };
  const router = Router(), limiter: RequestHandler = (_req, _res, next) => next();
  registerNodeRoutes(router, {
    nodeRepository: createNodeRepository(query), networkFilters,
    getNodes: async () => [], getNodeHistory: async () => [], getNodeAdverts: async () => [],
    getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    getPublicVisibilityGeneration: () => readPublicResponseGeneration(query),
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: limiter as Parameters<typeof registerNodeRoutes>[1]['nodesLimiter'],
  });
  const app = express(); app.use(router);
  const server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const map = `${base}/nodes/map?network=ukmesh&snapshot=${encodeURIComponent(new Date().toISOString())}`;
  const first = await fetch(map); assert.equal(first.status, 200);
  assert.equal(first.headers.get('cache-control'), PUBLIC_REVALIDATE);
  const rows = await first.json() as { nodes: Array<{ node_id: string }> };
  assert.equal(rows.nodes.length, 3);
  const nodeId = 'AABB' + '22'.repeat(30), peerId = 'CCDD' + '33'.repeat(30);
  const links = `${base}/nodes/${nodeId}/links?network=ukmesh`;
  const priorLinks = await fetch(links); assert.equal(priorLinks.status, 200);
  assert.equal((await priorLinks.json())[0].peer_id, peerId);
  const conditional = (etag: string) => ({ cache: 'force-cache' as const, headers: { 'If-None-Match': etag } });
  assert.equal((await fetch(map, conditional(first.headers.get('etag')!))).status, 304);
  assert.equal((await fetch(links, conditional(priorLinks.headers.get('etag')!))).status, 304);
  hold = true;
  const racing = fetch(map, conditional(first.headers.get('etag')!));
  await reached;
  // A separate SQL connection commits the real node trigger while the HTTP
  // response holds already-selected public metadata.
  const writer = new pg.Client({ connectionString: url.href }); await writer.connect();
  try { await writer.query("UPDATE core.nodes SET name='Invented private 🚫' WHERE node_id=$1", [peerId]); }
  finally { await writer.end(); }
  hold = false; release();
  const rejected = await racing;
  assert.ok(rejected.status >= 500); assert.equal(rejected.headers.get('cache-control'), 'no-store');
  assert.ok(!JSON.stringify(await rejected.json()).includes(peerId));
  const pendingLinks = await fetch(links, conditional(priorLinks.headers.get('etag')!));
  assert.ok(pendingLinks.status >= 500); assert.equal(pendingLinks.headers.get('cache-control'), 'no-store');
  await prepareSampleDataset(url.href);
  const repairedMap = await fetch(map, conditional(first.headers.get('etag')!));
  assert.equal(repairedMap.status, 200);
  assert.ok(!(await repairedMap.json()).nodes.some((row: { node_id: string }) => row.node_id === peerId));
  const repairedLinks = await fetch(links, conditional(priorLinks.headers.get('etag')!));
  assert.equal(repairedLinks.status, 200);
  assert.equal(repairedLinks.headers.get('cache-control'), PUBLIC_REVALIDATE);
  assert.deepEqual(await repairedLinks.json(), [], 'a current generation cannot replay the old link-cache entry');
});
