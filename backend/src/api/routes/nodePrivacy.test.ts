import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { createNodeRepository } from '../../repositories/nodes.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { PUBLIC_REVALIDATE } from '../../http/publicJson.js';
import { registerNodeRoutes } from './nodes.js';

async function fixture(t: test.TestContext) {
  let generation = 1, reads = 0, loads = 0;
  let mode = 'ready';
  const row = { node_id: 'A'.repeat(64), name: 'Invented public node', lat: 54, lon: -1, role: 2 };
  const data = async () => { loads++; return generation === 1 ? [row] : []; };
  const repository = createNodeRepository(async () => ({ rows: [] }));
  repository.listPublicMapRows = data;
  repository.listAllNodeIds = async () => [];
  repository.listInferredPackets = async () => [];
  repository.listNodeLinks = async () => { loads++; return generation === 1 ? [{
    peer_id: 'B'.repeat(64), peer_name: 'Invented peer', observed_count: 2,
    itm_path_loss_db: null, count_this_to_peer: 1, count_peer_to_this: 1,
    last_observed: null, recent_count_72h: 2,
  }] : []; };
  const limiter: RequestHandler = (_req, _res, next) => next();
  const app = express(), router = Router();
  registerNodeRoutes(router, {
    nodeRepository: repository, getNodes: data, getNodeHistory: data,
    getNodeAdverts: data, getTopAdvertingRepeaters: data, requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: async () => {
      reads++;
      if (mode === 'initial-failure' || mode === 'final-failure' && reads === 2) throw new Error('unavailable');
      if (mode === 'final-change' && reads === 2) generation++;
      return generation;
    },
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: limiter as Parameters<typeof registerNodeRoutes>[1]['nodesLimiter'],
  });
  app.use(router);
  const server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  return {
    base: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    begin(value: string) { mode = value; reads = 0; },
    get reads() { return reads; }, get loads() { return loads; },
  };
}

test('public node routes validate cached 200/304 delivery and withhold on authority failure or mid-request changes', async t => {
  t.mock.method(console, 'error', () => undefined);
  const id = 'A'.repeat(64), snapshot = encodeURIComponent(new Date().toISOString());
  for (const route of ['/nodes', `/nodes/map?snapshot=${snapshot}`, `/nodes/map?snapshot=${snapshot}&q=Invented`,
    `/nodes/${id}/links`, `/nodes/${id}/history`, `/nodes/${id}/adverts`, '/nodes/top-adverts', '/inferred-nodes']) {
    const f = await fixture(t);
    const url = f.base + route + (route.includes('?') ? '&' : '?') + 'network=ukmesh';
    const first = await fetch(url); assert.equal(first.status, 200, route);
    const control = route.includes('q=') || route === '/nodes/top-adverts' ? 'no-store' : PUBLIC_REVALIDATE;
    assert.equal(first.headers.get('cache-control'), control, route);
    const etag = first.headers.get('etag'); assert.ok(etag);
    await first.arrayBuffer();
    const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': etag } };
    f.begin('ready');
    const unchanged = await fetch(url, conditional);
    assert.equal(unchanged.status, 304, route);
    assert.equal(unchanged.headers.get('cache-control'), control, route);
    assert.equal(f.reads, 2, `a cached response still checks current authority twice: ${route}`);
    for (const mode of ['initial-failure', 'final-failure', 'final-change']) {
      f.begin(mode); const before = f.loads;
      const rejected = await fetch(url, conditional);
      assert.ok(rejected.status >= 500, `${route} ${mode}: ${rejected.status}`);
      assert.equal(rejected.headers.get('cache-control'), 'no-store', `${route} ${mode}`);
      assert.ok(!JSON.stringify(await rejected.json()).includes('Invented'), `${route} ${mode}`);
      if (mode === 'initial-failure') assert.equal(f.loads, before);
    }
  }
});
