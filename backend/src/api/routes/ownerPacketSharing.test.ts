import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import type { QueryResultRow } from 'pg';
import { registerOwnerRoutes } from './owner.js';

const nodeId = 'A'.repeat(64);
const destination = {
  destination_id: 'remote-site',
  display_name: 'Remote site',
  website_url: 'https://remote.example.test',
  description: 'A remote packet site',
  broker_url: 'mqtts://remote.example.test:8883',
  topic_template: 'replicated/{network}/{observerId}/packets',
  broker_username: null,
  broker_password_ciphertext: null,
  enabled: true,
};
const passThroughLimiter: RequestHandler = (_req, _res, next) => next();

function ownerRouteDeps(
  query: (sql: string, params?: unknown[]) => Promise<{ rows: QueryResultRow[] }>,
  mqttUsername = 'node1',
  requireOwnerSession: (req: import('express').Request, res: import('express').Response) => Promise<string[] | null> = async () => [nodeId],
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
    getOwnerSession: () => ({ v: 3 as const, mqttUsername, exp: Date.now() + 60_000, gen: 0 }),
    requireOwnerSession,
    getOwnerCredentialGeneration: async () => 0,
    invalidateOwnerNodeIdCache: () => undefined,
    query,
    withTransaction: async (work: (runQuery: typeof query) => Promise<void>) => work(query),
  };
}

async function withOwnerApi(
  query: (sql: string, params?: unknown[]) => Promise<{ rows: QueryResultRow[] }>,
  check: (baseUrl: string, writes: string[]) => Promise<void>,
  selectedDestinationRows: QueryResultRow[] = [{ destination_id: 'remote-site' }],
  requireOwnerSession?: (req: import('express').Request, res: import('express').Response) => Promise<string[] | null>,
) {
  const writes: string[] = [];
  const router = Router();
  registerOwnerRoutes(router, ownerRouteDeps(async (sql, params) => {
    if (sql.startsWith('DELETE FROM owner_packet_share_rules')
      || sql.startsWith('UPDATE owner_packet_share_deliveries')
      || sql.startsWith('INSERT INTO owner_packet_share_rules')) {
      writes.push(`${sql}\n${JSON.stringify(params ?? [])}`);
      return sql.startsWith('INSERT INTO owner_packet_share_rules')
        ? { rows: [{ destination_id: 'remote-site' }] }
        : { rows: [] };
    }
    if (sql.includes('FROM owner_packet_share_destinations')) {
      return { rows: [destination] };
    }
    if (sql.includes('FROM owner_packet_share_rules')) {
      return { rows: selectedDestinationRows };
    }
    return query(sql, params);
  }, 'node1', requireOwnerSession));
  const app = express();
  app.use(express.json());
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    await check(`http://127.0.0.1:${(server.address() as AddressInfo).port}`, writes);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}

test('owner packet-sharing settings start with every destination off', async () => {
  await withOwnerApi(async () => ({ rows: [] }), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/owner/packet-sharing?nodeId=${nodeId}`);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.enabled, false);
    assert.equal(body.destinations[0]?.selected, false);
  }, []);
});

test('owner packet-sharing settings expose destinations and persist a selected destination', async () => {
  const lastForwardedAt = '2026-09-16T22:45:00.000Z';
  await withOwnerApi(async (sql) => (
    sql.includes('FROM owner_packet_share_deliveries')
      ? { rows: [{ destination_id: 'remote-site', last_forwarded_at: lastForwardedAt }] }
      : { rows: [] }
  ), async (baseUrl, writes) => {
    const getResponse = await fetch(`${baseUrl}/api/owner/packet-sharing?nodeId=${nodeId}`);
    assert.equal(getResponse.status, 200);
    assert.equal(getResponse.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await getResponse.json(), {
      nodeId,
      featureEnabled: true,
      enabled: true,
      destinations: [{
        id: 'remote-site',
        name: 'Remote site',
        websiteUrl: 'https://remote.example.test',
        description: 'A remote packet site',
        configured: true,
        selected: true,
        lastForwardedAt,
      }],
    });

    const postResponse = await fetch(`${baseUrl}/api/owner/packet-sharing`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: 'meshcore_owner_csrf=test-token',
        'x-csrf-token': 'test-token',
      },
      body: JSON.stringify({ nodeId: nodeId.toLowerCase(), enabled: true, destinationIds: ['remote-site'] }),
    });
    assert.equal(postResponse.status, 200);
    assert.deepEqual(await postResponse.json(), { nodeId, enabled: true, destinationIds: ['remote-site'] });
    assert.equal(writes.length, 3);

    const disableResponse = await fetch(`${baseUrl}/api/owner/packet-sharing`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: 'meshcore_owner_csrf=test-token',
        'x-csrf-token': 'test-token',
      },
      body: JSON.stringify({ nodeId, enabled: false, destinationIds: [] }),
    });
    assert.equal(disableResponse.status, 200);
    assert.deepEqual(await disableResponse.json(), { nodeId, enabled: false, destinationIds: [] });
    assert.equal(writes.length, 5);
  });
});

test('owner packet-sharing rejects a destination that is not in the operator registry', async () => {
  await withOwnerApi(async () => ({ rows: [] }), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/owner/packet-sharing`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: 'meshcore_owner_csrf=test-token',
        'x-csrf-token': 'test-token',
      },
      body: JSON.stringify({ nodeId, enabled: true, destinationIds: ['unknown-site'] }),
    });
    assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), { error: 'One or more selected destinations are not configured' });
  });
});

test('owner packet-sharing settings are available to every authenticated owner account', async () => {
  const router = Router();
  registerOwnerRoutes(router, ownerRouteDeps(async () => ({ rows: [] }), 'node2'));
  const app = express();
  app.use(express.json());
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const response = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/api/owner/packet-sharing`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {
      nodeId,
      featureEnabled: true,
      enabled: false,
      destinations: [],
    });
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test('owner packet-sharing rejects nodes outside the authenticated session', async () => {
  await withOwnerApi(async () => ({ rows: [] }), async (baseUrl, writes) => {
    const response = await fetch(`${baseUrl}/api/owner/packet-sharing?nodeId=${'B'.repeat(64)}`);
    assert.equal(response.status, 403);
    assert.deepEqual(await response.json(), { error: 'Node is not owned by this session' });
    assert.equal(writes.length, 0);
  });
});

test('owner packet-sharing requires an authenticated session', async () => {
  await withOwnerApi(async () => ({ rows: [] }), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/owner/packet-sharing`);
    assert.equal(response.status, 401);
    assert.deepEqual(await response.json(), { error: 'Unauthorized' });
  }, [{ destination_id: 'remote-site' }], async (_req, res) => {
    res.status(401).json({ error: 'Unauthorized' });
    return null;
  });
});

test('owner packet-sharing rejects malformed settings without database writes', async () => {
  await withOwnerApi(async () => ({ rows: [] }), async (baseUrl, writes) => {
    const response = await fetch(`${baseUrl}/api/owner/packet-sharing`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: 'meshcore_owner_csrf=test-token',
        'x-csrf-token': 'test-token',
      },
      body: JSON.stringify({ nodeId, enabled: true, destinationIds: ['remote-site'], unexpected: true }),
    });
    assert.equal(response.status, 400);
    assert.deepEqual(await response.json(), { error: 'Invalid packet sharing settings' });
    assert.equal(writes.length, 0);
  });
});
