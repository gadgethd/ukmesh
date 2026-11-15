import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { PUBLIC_REVALIDATE } from '../../http/publicJson.js';
import { encodePublicMapCursor } from '../../nodes/publicMap.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { createNodeRepository } from '../../repositories/nodes.js';
import { registerNodeRoutes } from './nodes.js';

test('actual map HTTP encoding preserves the legacy body, page cursor, headers and ETag', async t => {
  const rows = [
    { node_id: 'A'.repeat(64), name: 'Invented "node" 🛰️ \\ \n<>&', lat: 54, lon: -1, role: 2, firmware_version: null },
    { node_id: 'B'.repeat(64), name: 'Next public node', lat: 55, lon: -2, role: 2, firmware_version: 'v1' },
  ];
  const snapshot = new Date().toISOString();
  const history = [{ time: new Date('2026-01-01T00:00:00.000Z'), data: { text: 'Public 🛰️ <>&', values: [1, null] } }];
  const expected = { nodes: [rows[0]], page: { snapshot, nextCursor: encodePublicMapCursor(rows[0]!.node_id),
    complete: false, returned: 1, rowLimit: 1 } };
  const repository = createNodeRepository(async () => ({ rows: [] }));
  repository.listPublicMapRows = async () => rows;
  let reads = 0;
  const app = express(), router = Router();
  const limiter: RequestHandler = (_req, _res, next) => next();
  const close = registerNodeRoutes(router, {
    nodeRepository: repository, getNodes: async () => rows, getNodeHistory: async () => history,
    getNodeAdverts: async () => [], getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: async () => { reads++; return 1; },
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: limiter as Parameters<typeof registerNodeRoutes>[1]['nodesLimiter'],
  });
  app.get('/legacy-body', (_req, res) => { res.json(expected); });
  app.get('/legacy-history', (_req, res) => { res.json(history); });
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  t.after(async () => {
    close(); server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const url = base + `/nodes/map?network=ukmesh&limit=1&snapshot=${encodeURIComponent(snapshot)}`;
  for (const spaces of [undefined, 2]) {
    app.set('json spaces', spaces);
    reads = 0;
    const control = await fetch(base + '/legacy-body'), actual = await fetch(url);
    assert.equal(actual.status, 200);
    assert.equal(await actual.text(), await control.text(), 'wire bytes match the prior object envelope');
    for (const header of ['content-type', 'content-length', 'etag']) {
      assert.equal(actual.headers.get(header), control.headers.get(header), header);
    }
    assert.equal(actual.headers.get('cache-control'), PUBLIC_REVALIDATE);
    assert.equal(actual.headers.get('x-map-snapshot'), snapshot);
    assert.equal(actual.headers.get('x-map-complete'), 'false');
    assert.equal(reads, 2);
    reads = 0;
    const cached = await fetch(url, { cache: 'force-cache', headers: { 'If-None-Match': actual.headers.get('etag')! } });
    assert.equal(cached.status, 304);
    assert.equal(await cached.text(), '');
    assert.equal(reads, 2);
    reads = 0;
    const historyControl = await fetch(base + '/legacy-history');
    const historyUrl = base + `/nodes/${rows[0]!.node_id}/history?network=ukmesh`;
    const historyActual = await fetch(historyUrl);
    assert.equal(historyActual.status, 200);
    assert.equal(await historyActual.text(), await historyControl.text(), 'Date and JSON history wire bytes stay equivalent');
    for (const header of ['content-type', 'content-length', 'etag']) {
      assert.equal(historyActual.headers.get(header), historyControl.headers.get(header), header);
    }
    assert.equal(reads, 2);
    reads = 0;
    const historyCached = await fetch(historyUrl, {
      cache: 'force-cache', headers: { 'If-None-Match': historyActual.headers.get('etag')! },
    });
    assert.equal(historyCached.status, 304); assert.equal(await historyCached.text(), '');
    assert.equal(reads, 2);
  }
});
