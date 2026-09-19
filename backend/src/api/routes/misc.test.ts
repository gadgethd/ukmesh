import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { registerMiscRoutes } from './misc.js';

const passThroughLimiter: RequestHandler = (_req, _res, next) => next();

test('channel feed history route forwards its bounded channel and scope', async () => {
  let call: unknown[] | undefined;
  const router = Router();
  registerMiscRoutes(router, {
    query: async () => ({ rows: [] }),
    getRecentPackets: async () => [],
    getRecentPacketEvents: async () => [],
    getPacketDetail: async () => null,
    getChannelMessageHistory: async (...args) => {
      call = args;
      return [{ packet_hash: 'history-row' }];
    },
    getRecentMessageTags: async () => [],
    getPublicVisibilityGeneration: async () => 1,
    packetDetailLimiter: passThroughLimiter as Parameters<typeof registerMiscRoutes>[1]['packetDetailLimiter'],
  });
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/feed/messages?channel=Bot&limit=50&network=ukmesh`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), [{ packet_hash: 'history-row' }]);
    assert.deepEqual(call, ['Bot', 50, 'ukmesh', undefined]);
    assert.equal(response.headers.get('cache-control'), 'no-store');
  } finally {
    server.close();
  }
});

test('MQTT nodes retain production aliases, test isolation and the active-packet filter', async () => {
  const calls: unknown[][] = [];
  // Capture the real route's SQL parameters without connecting to any database.
  const scopedRouter = Router();
  registerMiscRoutes(scopedRouter, {
    query: async <T>(_sql: string, params?: unknown[]) => {
      calls.push(params ?? []);
      return { rows: [{ node_id: 'active', packets_24h: '42' }, { node_id: 'idle', packets_24h: '0' }] as T[] };
    },
    getRecentPackets: async () => [], getRecentPacketEvents: async () => [],
    getPacketDetail: async () => null, getChannelMessageHistory: async () => [], getRecentMessageTags: async () => [],
    getPublicVisibilityGeneration: async () => 1,
    packetDetailLimiter: passThroughLimiter as Parameters<typeof registerMiscRoutes>[1]['packetDetailLimiter'],
  });
  const app = express();
  app.use(scopedRouter);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const { port } = server.address() as AddressInfo;
    for (const network of ['northeast', 'ukmesh', 'test']) {
      const response = await fetch(`http://127.0.0.1:${port}/mqtt-nodes?network=${network}`);
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), [{ node_id: 'active', packets_24h: '42' }]);
    }
    const production = ['ukmesh', 'northeast', 'teesside'];
    assert.deepEqual(calls, [[production, production], [production, production], [['test'], ['test']]]);
  } finally { server.close(); }
});
