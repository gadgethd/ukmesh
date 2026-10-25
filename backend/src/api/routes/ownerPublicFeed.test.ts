import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import type { QueryResultRow } from 'pg';
import { registerOwnerRoutes } from './owner.js';

const nodeId = 'A'.repeat(64);
const passThroughLimiter: RequestHandler = (_req, _res, next) => next();

function ownerRouteDeps(
  query: (sql: string, params?: unknown[]) => Promise<{ rows: QueryResultRow[] }>,
  reconcileOwnerAuthorization?: () => Promise<void>,
) {
  return {
    ownerCookieName: 'meshcore_owner_session',
    ownerLiveCacheTtlMs: 1_000,
    ownerLiveCache: new Map(),
    ownerDashboardCacheTtlMs: 1_000,
    ownerLastHopCacheTtlMs: 1_000,
    ownerSessionTtlMs: 60_000,
    mqttUsernameMaxLen: 128,
    mqttPasswordMaxLen: 128,
    ownerLoginLimiter: passThroughLimiter,
    hasControlChars: () => false,
    verifyMqttCredentials: async () => true,
    resolveOwnerNodeIds: async () => [nodeId],
    autoLinkOwnerNodeIds: async () => [nodeId],
    buildOwnerDashboard: async () => ({ nodes: [] }),
    encryptOwnerSession: () => 'session',
    isSecureRequest: () => false,
    getOwnerSession: () => ({ v: 3 as const, mqttUsername: 'node1', exp: Date.now() + 60_000, gen: 0 }),
    requireOwnerSession: async () => [nodeId],
    getOwnerCredentialGeneration: async () => 0,
    invalidateOwnerNodeIdCache: () => undefined,
    reconcileOwnerAuthorization,
    query,
    withTransaction: async (work: (runQuery: typeof query) => Promise<void>) => work(query),
  };
}

async function withOwnerApi(
  query: (sql: string, params?: unknown[]) => Promise<{ rows: QueryResultRow[] }>,
  check: (baseUrl: string) => Promise<void>,
  reconcileOwnerAuthorization?: () => Promise<void>,
) {
  const router = Router();
  registerOwnerRoutes(router, ownerRouteDeps(query, reconcileOwnerAuthorization));
  const app = express();
  app.use(express.json());
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    await check(`http://127.0.0.1:${(server.address() as AddressInfo).port}`);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}

test('public feed settings are account-level and default off', async () => {
  await withOwnerApi(async (sql) => {
    if (sql.includes('COALESCE(settings.enabled')) {
      return { rows: [{ enabled: false, last_forwarded_at: null }] };
    }
    return { rows: [] };
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/owner/public-feed`);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await response.json(), {
      featureEnabled: true,
      configured: false,
      brokerUrl: null,
      enabled: false,
      nodeCount: 1,
      eligible: false,
      recentPacketCount: 0,
      topicFilters: [],
      lastForwardedAt: null,
    });
  });
});

test('public feed rejects enabling until the operator supplies a broker configuration', async () => {
  await withOwnerApi(async (sql) => {
    if (sql.includes('COALESCE(settings.enabled')) {
      return { rows: [{ enabled: false, recent_packet_count: 1, last_forwarded_at: null }] };
    }
    return { rows: [] };
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/owner/public-feed`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: 'meshcore_owner_csrf=test-token',
        'x-csrf-token': 'test-token',
      },
      body: JSON.stringify({ enabled: true }),
    });
    assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), { error: 'Public feed is not configured by the operator' });
  });
});

test('public feed rejects enabling when the owner has no eligible packet activity', async () => {
  await withOwnerApi(async (sql) => {
    if (sql.includes('COALESCE(settings.enabled')) {
      return { rows: [{ enabled: false, recent_packet_count: 0, last_forwarded_at: null }] };
    }
    return { rows: [] };
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/owner/public-feed`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: 'meshcore_owner_csrf=test-token',
        'x-csrf-token': 'test-token',
      },
      body: JSON.stringify({ enabled: true }),
    });
    assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), {
      error: 'Publish at least one eligible packet in the last 24 hours before enabling the public feed',
    });
  });
});

test('public feed persists the account toggle and current owned-node mapping', async () => {
  const previous = { ...process.env };
  Object.assign(process.env, {
    PUBLIC_FEED_PUBLISH_ENABLED: 'true',
    PUBLIC_FEED_BROKER_URL: 'mqtts://broker.example.test:8883',
    PUBLIC_FEED_BROKER_USERNAME: 'publisher',
    PUBLIC_FEED_BROKER_PASSWORD: 'unit-test-secret',
  });
  let enabled = false;
  const writes: string[] = [];
  let aclReconciliations = 0;
  try {
    await withOwnerApi(async (sql) => {
      if (sql.includes('COALESCE(settings.enabled')) {
        return { rows: [{ enabled, recent_packet_count: 1, last_forwarded_at: null }] };
      }
      if (sql.startsWith('INSERT INTO owner_public_feed_settings')
        || sql.startsWith('DELETE FROM owner_public_feed_nodes')
        || sql.startsWith('INSERT INTO owner_public_feed_nodes')) {
        writes.push(sql);
        if (sql.startsWith('INSERT INTO owner_public_feed_settings')) enabled = true;
      }
      return { rows: [] };
    }, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/api/owner/public-feed`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          cookie: 'meshcore_owner_csrf=test-token',
          'x-csrf-token': 'test-token',
        },
        body: JSON.stringify({ enabled: true }),
      });
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), {
        featureEnabled: true,
        configured: true,
        brokerUrl: 'mqtts://broker.example.test:8883',
        enabled: true,
        nodeCount: 1,
        eligible: true,
        recentPacketCount: 1,
        topicFilters: ['public/+/+/packets'],
        lastForwardedAt: null,
      });
      assert.equal(writes.length, 3);
    }, async () => { aclReconciliations += 1; });
    assert.equal(aclReconciliations, 1);
  } finally {
    for (const key of Object.keys(process.env)) {
      if (!(key in previous)) delete process.env[key];
    }
    Object.assign(process.env, previous);
  }
});
