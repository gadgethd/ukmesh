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

test('public MQTT node status omits owner-only and unknown telemetry fields', async () => {
  const storedRow = {
    node_id: 'b'.repeat(64),
    name: 'Public MQTT node',
    last_seen: '2026-09-23T12:00:00.000Z',
    battery_mv: 3900,
    uptime_secs: 4200,
    channel_utilization: 8,
    air_util_tx: 4,
    rx_air_secs: 18,
    tx_air_secs: 9,
    packets_24h: '12',
    hardware_model: 'private model',
    firmware_version: 'private firmware',
    stats: {
      wifi_ssid: 'private wifi',
      reset_reason: 'private reset detail',
      fs_free_bytes: 1234,
      mqtt: { broker_uri: 'mqtts://private', broker_username: 'private-user' },
      future_owner_field: 'private future value',
    },
    future_status_field: 'private future value',
  };
  const router = Router();
  registerMiscRoutes(router, {
    query: async <T extends import('pg').QueryResultRow = import('pg').QueryResultRow>(text) => {
      assert.doesNotMatch(text, /nss\.stats/);
      return { rows: [storedRow as unknown as T] };
    },
    getRecentPackets: async () => [],
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
    const response = await fetch(`http://127.0.0.1:${port}/mqtt-nodes?network=ukmesh`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), [{
      node_id: storedRow.node_id,
      name: storedRow.name,
      last_seen: storedRow.last_seen,
      battery_mv: storedRow.battery_mv,
      uptime_secs: storedRow.uptime_secs,
      channel_utilization: storedRow.channel_utilization,
      air_util_tx: storedRow.air_util_tx,
      rx_air_secs: storedRow.rx_air_secs,
      tx_air_secs: storedRow.tx_air_secs,
      packets_24h: storedRow.packets_24h,
    }]);
  } finally {
    server.close();
  }
});
