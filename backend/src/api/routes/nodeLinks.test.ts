import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { networkFilters } from '../utils/networkFilters.js';
import { createNodeRepository, type NodeLinkRow } from '../../repositories/nodes.js';
import { registerNodeRoutes } from './nodes.js';

test('links endpoint serializes additive fields and retains them on a cache hit', async () => {
  const nodeId = 'A'.repeat(64);
  const rows: NodeLinkRow[] = [{
    peer_id: 'B'.repeat(64), peer_name: 'Peer', observed_count: 2000,
    itm_path_loss_db: 112.1, count_this_to_peer: 700, count_peer_to_this: 1300,
    last_observed: '2026-09-07T12:00:00.123Z', recent_count_72h: 0,
  }, {
    peer_id: 'C'.repeat(64), peer_name: null, observed_count: 20,
    itm_path_loss_db: null, count_this_to_peer: 10, count_peer_to_this: 10,
    last_observed: null, recent_count_72h: 12,
  }];
  let calls = 0;
  const repository = createNodeRepository(async () => ({ rows: [] }));
  repository.listNodeLinks = async (id, scope) => {
    calls += 1;
    assert.equal(id.toUpperCase(), nodeId);
    assert.deepEqual(scope.params, [['ukmesh', 'northeast', 'teesside']]);
    return rows;
  };
  const app = express();
  const router = Router();
  registerNodeRoutes(router, {
    nodeRepository: repository,
    getNodes: async () => [], getNodeHistory: async () => [], getNodeAdverts: async () => [],
    getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: async () => 1,
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60000,
    nodesLimiter: rateLimit({ windowMs: 60000, limit: 100 }),
  });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    for (const id of [nodeId.toLowerCase(), nodeId]) {
      const response = await fetch(`${base}/api/nodes/${id}/links?network=ukmesh`);
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), rows);
    }
    assert.equal(calls, 1);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});

test('nodes endpoint opts into map visibility while inferred-path matching stays unfiltered', async () => {
  const calls: unknown[][] = [];
  const app = express();
  const router = Router();
  registerNodeRoutes(router, {
    nodeRepository: createNodeRepository(async () => ({ rows: [] })),
    getNodes: async (...args) => { calls.push(args); return []; },
    getNodeHistory: async () => [], getNodeAdverts: async () => [],
    getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: async () => 1,
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60000,
    nodesLimiter: rateLimit({ windowMs: 60000, limit: 100 }),
  });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    assert.equal((await fetch(`${base}/api/nodes?network=ukmesh&fields=slim`)).status, 200);
    assert.equal((await fetch(`${base}/api/inferred-nodes?network=ukmesh`)).status, 200);
    assert.deepEqual(calls, [
      ['ukmesh', undefined, 'slim', undefined, undefined, { mapVisible: true }],
      ['ukmesh', undefined, 'slim'],
    ]);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});
