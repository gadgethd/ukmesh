import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import type { QueryResultRow } from 'pg';
import { PUBLIC_REVALIDATE } from '../../http/publicJson.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { registerActivityTimelineRoutes } from './activityTimeline.js';

test('timeline fences ordinary and conditional delivery before and after its SQL load', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-12T12:00:00Z') });
  t.mock.method(console, 'error', () => undefined);
  let reads = 0, loads = 0, mode = 'ready';
  const id = 'A'.repeat(64), router = Router();
  const limiter: RequestHandler = (_req, _res, next) => next();
  const shutdownCache = registerActivityTimelineRoutes(router, {
    limiter: limiter as Parameters<typeof registerActivityTimelineRoutes>[1]['limiter'], networkFilters,
    query: async <T extends QueryResultRow>(_sql: string, params?: unknown[]) => {
      loads++;
      assert.deepEqual(params?.slice(-2), ['360 minutes', '15 minutes']);
      return { rows: [{ bucket: '2026-11-12 11:45:00+00', packet_count: '25', observer_count: '1',
        active_node_ids: [id] }] as unknown as T[] };
    },
    getPublicVisibilityGeneration: async () => {
      reads++;
      if (mode === 'initial-failure' || mode === 'final-failure' && reads === 2) throw new Error('unavailable');
      return mode === 'final-change' && reads === 2 ? 2 : mode === 'invalid' ? 0 : 1;
    },
  });
  t.after(shutdownCache);
  const app = express(); app.use(router);
  const server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/activity/timeline?network=ukmesh`;
  const first = await fetch(url); assert.equal(first.status, 200);
  assert.equal(first.headers.get('cache-control'), PUBLIC_REVALIDATE);
  assert.equal(reads, 3); // Fill publication plus both delivery fences.
  assert.deepEqual((await first.json()).buckets[0], {
    time: '2026-11-12 11:45:00+00', packetCount: 25, observerCount: 1, activeNodeIds: [id],
  });
  const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': first.headers.get('etag')! } };
  reads = 0;
  const unchanged = await fetch(url, conditional); assert.equal(unchanged.status, 304);
  assert.equal(unchanged.headers.get('cache-control'), PUBLIC_REVALIDATE); assert.equal(reads, 2);
  for (const failure of ['initial-failure', 'final-failure', 'final-change', 'invalid']) {
    mode = failure; reads = 0; const before = loads;
    const response = await fetch(url, conditional);
    assert.equal(response.status, 500, failure);
    assert.equal(response.headers.get('cache-control'), 'no-store', failure);
    assert.ok(!JSON.stringify(await response.json()).includes(id), failure);
    if (failure === 'initial-failure' || failure === 'invalid') assert.equal(loads, before);
  }
});

test('timeline HTTP cache isolates network, window, bucket and authority generation', async t => {
  let generation = 1, loads = 0;
  const router = Router(), limiter: RequestHandler = (_req, _res, next) => next();
  const stop = registerActivityTimelineRoutes(router, {
    limiter: limiter as Parameters<typeof registerActivityTimelineRoutes>[1]['limiter'], networkFilters,
    getPublicVisibilityGeneration: async () => generation,
    query: async <T extends QueryResultRow>() => ({ rows: [{ bucket: '2026-11-12 12:00:00+00',
      packet_count: String(++loads), observer_count: '1', active_node_ids: ['A'.repeat(64)] }] as unknown as T[] }),
  });
  t.after(stop);
  const app = express(); app.use(router); const server = app.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/activity/timeline`;
  const get = async (query: string) => {
    const response = await fetch(url + query); assert.equal(response.status, 200);
    return (await response.json()).buckets[0].packetCount;
  };
  assert.equal(await get('?network=ukmesh'), 1);
  assert.equal(await get('?network=ukmesh'), 1);
  assert.equal(await get('?network=teesside'), 1); // Unified production alias.
  assert.equal(await get('?network=test'), 2);
  assert.equal(await get('?network=ukmesh&minutes=60'), 3);
  assert.equal(await get('?network=ukmesh&bucket=5'), 4);
  generation = 2;
  assert.equal(await get('?network=ukmesh'), 5);
  assert.equal(loads, 5);
});
