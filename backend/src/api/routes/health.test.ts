import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router } from 'express';
import type { HealthSnapshotRead } from '../../health/snapshot.js';
import { registerHealthRoutes, type HealthRouteDeps } from './healthRoutes.js';

type HealthDetail = Parameters<typeof import('../../health/status.js').toPublicHealthOverview>[0];

function healthDetail(status: 'healthy' | 'degraded' | 'critical'): HealthDetail {
  return {
    status,
    problems: status === 'healthy'
      ? []
      : [{ code: 'ingest_stale', severity: 'warning', message: 'operator-only detail' }],
    maintenance: { active: false, message: 'operator-only maintenance note' },
    system: { generated_at: '2026-09-12T00:00:00.000Z', disk: { used_pct: 42 } },
    ingest: { packet_age_minutes: 5 },
    workers: [{ worker_name: 'link-worker', status: 'running' }],
  } as unknown as HealthDetail;
}

function readySnapshot(status: 'healthy' | 'degraded' | 'critical'): HealthSnapshotRead<HealthDetail> {
  return { ready: true, generatedAt: Date.now(), data: healthDetail(status) };
}

async function withHealthServer(
  deps: Pick<HealthRouteDeps, 'readSnapshot' | 'isLocalClient' | 'hasOperatorAuthorization'>,
  callback: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const router = Router();
  registerHealthRoutes(router, deps);
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const { port } = server.address() as AddressInfo;
    await callback(`http://127.0.0.1:${port}`);
  } finally {
    server.close();
  }
}

test('anonymous health callers receive only a bounded status word', async () => {
  await withHealthServer({
    readSnapshot: () => readySnapshot('degraded'),
    isLocalClient: () => false,
    hasOperatorAuthorization: () => false,
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 200);
    const body = await response.json() as Record<string, unknown>;
    assert.deepEqual(body, { status: 'degraded' });
    assert.doesNotMatch(JSON.stringify(body), /ingest_stale|link-worker|operator-only/);
  });
});

test('anonymous health callers see ok only when every component is healthy', async () => {
  await withHealthServer({
    readSnapshot: () => readySnapshot('healthy'),
    isLocalClient: () => false,
    hasOperatorAuthorization: () => false,
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { status: 'ok' });
  });
});

test('local callers still receive the full health overview', async () => {
  await withHealthServer({
    readSnapshot: () => readySnapshot('degraded'),
    isLocalClient: () => true,
    hasOperatorAuthorization: () => false,
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 200);
    const body = await response.json() as {
      status: string;
      incidents: Array<{ code: string }>;
      components: unknown;
      maintenance: unknown;
    };
    assert.equal(body.status, 'degraded');
    assert.deepEqual(body.incidents, [{ code: 'ingest_stale', severity: 'warning' }]);
    assert.ok(body.components);
    assert.ok(body.maintenance);
  });
});

test('operator-authenticated callers receive the full health overview', async () => {
  await withHealthServer({
    readSnapshot: () => readySnapshot('healthy'),
    isLocalClient: () => false,
    hasOperatorAuthorization: () => true,
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 200);
    const body = await response.json() as { status: string; incidents: unknown[] };
    assert.equal(body.status, 'healthy');
    assert.deepEqual(body.incidents, []);
  });
});

test('anonymous callers never receive initialisation error strings', async () => {
  const notReady: HealthSnapshotRead<HealthDetail> = {
    ready: false,
    generatedAt: Date.now(),
    lastError: 'password authentication failed for user "internal"',
  };
  await withHealthServer({
    readSnapshot: () => notReady,
    isLocalClient: () => false,
    hasOperatorAuthorization: () => false,
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { status: 'degraded' });
  });

  await withHealthServer({
    readSnapshot: () => notReady,
    isLocalClient: () => true,
    hasOperatorAuthorization: () => false,
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 503);
    const body = await response.json() as { status: string; lastError: string | null };
    assert.equal(body.status, 'initializing');
    assert.equal(body.lastError, notReady.lastError);
  });
});
