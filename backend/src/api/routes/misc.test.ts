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

test('recent packets forwards an opt-in slim profile without changing the full default', async () => {
  const profiles: Array<'full' | 'slim' | undefined> = [];
  const router = Router();
  registerMiscRoutes(router, {
    query: async () => ({ rows: [] }),
    getRecentPackets: async (_limit, _network, _observer, fields) => {
      profiles.push(fields);
      return [{ packet_hash: 'packet' }];
    },
    getRecentPacketEvents: async () => [],
    getPacketDetail: async () => null,
    getChannelMessageHistory: async () => [],
    getPublicVisibilityGeneration: async () => 1,
    packetDetailLimiter: passThroughLimiter as Parameters<typeof registerMiscRoutes>[1]['packetDetailLimiter'],
  });
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const { port } = server.address() as AddressInfo;
    const full = await fetch(`http://127.0.0.1:${port}/packets/recent?limit=1`);
    const slim = await fetch(`http://127.0.0.1:${port}/packets/recent?limit=1&fields=slim`);
    assert.equal(full.headers.get('x-response-profile'), 'full');
    assert.equal(slim.headers.get('x-response-profile'), 'slim');
    assert.deepEqual(profiles, ['full', 'slim']);
  } finally {
    server.close();
  }
});
