import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { networkFilters } from '../utils/networkFilters.js';
import { createNodeRepository } from '../../repositories/nodes.js';
import { registerNodeRoutes } from './nodes.js';

async function fixture(t: test.TestContext, state: {
  generation: () => Promise<number>;
  load: () => Promise<unknown[]>;
}) {
  const app = express();
  const router = Router();
  registerNodeRoutes(router, {
    nodeRepository: createNodeRepository(async () => ({ rows: [] })),
    getNodes: async () => [], getNodeHistory: async () => [], getNodeAdverts: async () => [],
    getTopAdvertingRepeaters: state.load, requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: state.generation,
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: rateLimit({ windowMs: 60_000, limit: 1_000 }),
  });
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise<void>((resolve) => server.close(() => resolve())); });
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/nodes/top-adverts?network=ukmesh`;
}

test('top adverts expire on a privacy change and cannot be held by a public HTTP cache', async (t) => {
  let generation = 1;
  let calls = 0;
  const url = await fixture(t, { generation: async () => generation, load: async () => {
    calls += 1;
    return generation === 1 ? [{ node_id: 'public-until-revoked', adverts_in_window: '12' }] : [];
  } });
  const before = await fetch(url);
  assert.deepEqual(await before.json(), [{ node_id: 'public-until-revoked', adverts_in_window: 12 }]);
  generation = 2;
  const after = await fetch(url);
  assert.deepEqual(await after.json(), []);
  assert.equal(after.headers.get('cache-control'), 'no-store');
  assert.equal(calls, 2);
});

test('concurrent top-advert reads share one query and reject a mid-query privacy change', async (t) => {
  let generation = 41;
  let arrivals = 0;
  let calls = 0;
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  const url = await fixture(t, { generation: async () => { arrivals += 1; return generation; }, load: async () => {
    calls += 1;
    await held;
    return [{ node_id: 'now-private', adverts_in_window: '2' }];
  } });
  t.after(() => release());
  const responses = Array.from({ length: 12 }, () => fetch(`${url}&hours=23`));
  const deadline = Date.now() + 5_000;
  while (arrivals < 12 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(arrivals, 12, 'all requests must join the held generation before it changes');
  generation = 42;
  release();
  const results = await Promise.all(responses);
  assert.equal(calls, 1);
  for (const response of results) {
    assert.equal(response.status, 503);
    assert.doesNotMatch(await response.text(), /now-private/);
  }
});
