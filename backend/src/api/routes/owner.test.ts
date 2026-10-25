import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import type { QueryResultRow } from 'pg';
import { registerOwnerRoutes } from './owner.js';
import { getOwnerSession, encryptOwnerSession } from '../../owner/ownerSession.js';
import { mqttOnboardingDiagnostics } from '../../mqtt/onboardingDiagnostics.js';

const NODE_ID = 'A1'.repeat(32);
const passThroughLimiter: RequestHandler = (_req, _res, next) => next();
const TEST_COOKIE_SECRET = 'test-owner-cookie-secret-at-least-32-bytes';

async function startOwnerRoutes(options: {
  currentGeneration?: () => Promise<number>;
  verifyCredentials?: (username: string, password: string) => Promise<boolean>;
  nodeIds?: string[];
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
    resolveOwnerNodeIds: async () => options.nodeIds ?? [NODE_ID],
    autoLinkOwnerNodeIds: async () => options.nodeIds ?? [NODE_ID],
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

async function login(baseUrl: string, mqttUsername: string) {
  const csrfResponse = await fetch(`${baseUrl}/owner/csrf`);
  const { csrfToken } = await csrfResponse.json() as { csrfToken: string };
  const csrfCookie = csrfResponse.headers.get('set-cookie')!.split(';', 1)[0]!;
  return fetch(`${baseUrl}/owner/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', cookie: csrfCookie, 'x-csrf-token': csrfToken },
    body: JSON.stringify({ mqttUsername, mqttPassword: 'fixture-password' }),
  });
}

test('credential-only owner sees the exact default-topic fix without receiving a session', async () => {
  mqttOnboardingDiagnostics.record('unlinked-owner', 'meshcore', Date.now());
  const server = await startOwnerRoutes({ nodeIds: [] });
  try {
    const response = await login(server.baseUrl, 'unlinked-owner');
    assert.equal(response.status, 403);
    const body = await response.json() as { error: string };
    assert.match(body.error, /broker denied/);
    assert.match(body.error, /meshcore\/\{IATA\}\/\{PUBLIC_KEY\}\/packets/);
    assert.doesNotMatch(response.headers.get('set-cookie') ?? '', /meshcore_owner_session=/);
    const other = await login(server.baseUrl, 'other-owner');
    assert.match((await other.json() as { error: string }).error, /MQTT credentials accepted/);
    assert.doesNotMatch((await (await login(server.baseUrl, 'other-owner')).text()), /broker denied/);
  } finally { await server.close(); server.restore(); }
});

test('invalid credentials cannot read an account diagnostic', async () => {
  mqttOnboardingDiagnostics.record('protected-owner', 'meshcore', Date.now());
  const server = await startOwnerRoutes({ verifyCredentials: async () => false });
  try {
    const response = await login(server.baseUrl, 'protected-owner');
    assert.equal(response.status, 403);
    assert.deepEqual(await response.json(), { error: 'Invalid MQTT credentials' });
  } finally { await server.close(); server.restore(); }
});

test('existing owners see only their own diagnostic on login and valid session refresh', async () => {
  mqttOnboardingDiagnostics.record('linked-owner', 'meshcore/status', Date.now());
  const server = await startOwnerRoutes();
  try {
    const response = await login(server.baseUrl, 'linked-owner');
    assert.equal(response.status, 200);
    const body = await response.json() as { dashboard: { nodes: unknown[]; mqttDiagnostic: { code: string } } };
    assert.equal(body.dashboard.nodes.length, 1);
    assert.equal(body.dashboard.mqttDiagnostic.code, 'MQTT_DEFAULT_TOPIC_ROOT');
    for (const [username, hasDiagnostic] of [['linked-owner', true], ['different-owner', false]] as const) {
      const token = encryptOwnerSession({ v: 3, mqttUsername: username, exp: Date.now() + 60_000, gen: 2 });
      const session = await fetch(`${server.baseUrl}/owner/session`, { headers: { cookie: `meshcore_owner_session=${token}` } });
      assert.equal(session.status, 200);
      const data = await session.json() as { dashboard: { mqttDiagnostic: unknown } };
      assert.equal(Boolean(data.dashboard.mqttDiagnostic), hasDiagnostic);
    }
  } finally { await server.close(); server.restore(); }
});

test('owner login verifies the exact password string, including surrounding whitespace', async () => {
  let receivedCredentials: [string, string] | undefined;
  const ownerServer = await startOwnerRoutes({
    verifyCredentials: async (username, password) => {
      receivedCredentials = [username, password];
      return true;
    },
  });

  try {
    const csrfResponse = await fetch(`${ownerServer.baseUrl}/owner/csrf`);
    const { csrfToken } = await csrfResponse.json() as { csrfToken: string };
    const csrfCookie = csrfResponse.headers.get('set-cookie')?.split(';', 1)[0];
    assert.ok(csrfCookie);

    const response = await fetch(`${ownerServer.baseUrl}/owner/login`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        cookie: csrfCookie,
        'x-csrf-token': csrfToken,
      },
      body: JSON.stringify({ mqttUsername: ' owner ', mqttPassword: ' password ' }),
    });

    assert.equal(response.status, 200);
    assert.deepEqual(receivedCredentials, ['owner', ' password ']);
  } finally {
    await ownerServer.close();
    ownerServer.restore();
  }
});
