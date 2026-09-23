import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import type { QueryResultRow } from 'pg';
import { registerOwnerRoutes } from './owner.js';
import { getOwnerSession, encryptOwnerSession } from '../../owner/ownerSession.js';

const NODE_ID = 'A1'.repeat(32);
const passThroughLimiter: RequestHandler = (_req, _res, next) => next();
const TEST_COOKIE_SECRET = 'test-owner-cookie-secret-at-least-32-bytes';

async function startOwnerRoutes(options: {
  currentGeneration?: () => Promise<number>;
  verifyCredentials?: (username: string, password: string) => Promise<boolean>;
} = {}): Promise<{ baseUrl: string; close: () => Promise<void>; restore: () => void }> {
  const previousSecret = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = TEST_COOKIE_SECRET;
  const router = Router();
  registerOwnerRoutes(router, {
    ownerCookieName: 'meshcore_owner_session',
    ownerLiveCacheTtlMs: 1_000,
    ownerLiveCache: new Map(),
    ownerDashboardCacheTtlMs: 1_000,
    ownerLastHopCacheTtlMs: 1_000,
    ownerSessionTtlMs: 60_000,
    mqttUsernameMaxLen: 128,
    mqttPasswordMaxLen: 128,
    ownerLoginLimiter: passThroughLimiter as Parameters<typeof registerOwnerRoutes>[1]['ownerLoginLimiter'],
    hasControlChars: (value) => /[\u0000-\u001F\u007F]/.test(value),
    verifyMqttCredentials: options.verifyCredentials ?? (async () => true),
    resolveOwnerNodeIds: async () => [NODE_ID],
    autoLinkOwnerNodeIds: async () => [NODE_ID],
    buildOwnerDashboard: async (nodeIds) => ({ nodes: nodeIds.map((nodeId) => ({ node_id: nodeId })) }),
    encryptOwnerSession,
    isSecureRequest: () => false,
    getOwnerSession: (req) => getOwnerSession(req, 'meshcore_owner_session'),
    requireOwnerSession: async () => [NODE_ID],
    getOwnerCredentialGeneration: options.currentGeneration ?? (async () => 2),
    invalidateOwnerNodeIdCache: () => undefined,
    query: async <T extends QueryResultRow = QueryResultRow>() => ({ rows: [] as T[] }),
  });
  const app = express();
  app.use(express.json());
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address() as AddressInfo;

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: async () => {
      server.close();
      await once(server, 'close');
    },
    restore: () => {
      if (previousSecret === undefined) delete process.env['OWNER_COOKIE_SECRET'];
      else process.env['OWNER_COOKIE_SECRET'] = previousSecret;
    },
  };
}

test('owner session route rejects cookies from an older credential generation', async () => {
  const ownerServer = await startOwnerRoutes();
  const token = encryptOwnerSession({
    v: 3,
    mqttUsername: 'owner',
    exp: Date.now() + 60_000,
    gen: 1,
  });

  try {
    const response = await fetch(`${ownerServer.baseUrl}/owner/session`, {
      headers: { cookie: `meshcore_owner_session=${token}` },
    });

    assert.equal(response.status, 401);
    assert.equal((await response.json() as { error: string }).error, 'Credentials have been rotated — please log in again');
  } finally {
    await ownerServer.close();
    ownerServer.restore();
  }
});
