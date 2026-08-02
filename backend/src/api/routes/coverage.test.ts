import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { registerCoverageRoutes } from './coverage.js';

const passThroughLimiter: RequestHandler = (_req, _res, next) => next();

const emptyNetworkFilters = (): NetworkFilters => ({
  params: [],
  packets: '',
  packetsAlias: () => '',
  nodes: '',
  nodesAlias: () => '',
});

async function withCoverageServer(
  query: Parameters<typeof registerCoverageRoutes>[1]['query'],
  callback: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const router = Router();
  registerCoverageRoutes(router, {
    coverageLimiter: passThroughLimiter as Parameters<
      typeof registerCoverageRoutes
    >[1]['coverageLimiter'],
    networkFilters: emptyNetworkFilters,
    query,
    coverageModelVersion: 7,
  });
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

test('coverage list exposes only computed rows from the active model', async (t) => {
  const previousViewshed = process.env['VIEWSHED_ENABLED'];
  process.env['VIEWSHED_ENABLED'] = '1';
  t.after(() => {
    if (previousViewshed === undefined) delete process.env['VIEWSHED_ENABLED'];
    else process.env['VIEWSHED_ENABLED'] = previousViewshed;
  });

  const calls: Array<{ sql: string; params: unknown[] }> = [];
  await withCoverageServer(async (sql, params = []) => {
    calls.push({ sql, params });
    return { rows: [] };
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/coverage?bbox=-2,50,1,55`);
    assert.equal(response.status, 200);
  });

  assert.equal(calls.length, 1);
  assert.match(calls[0]!.sql, /nc\.model_version = \$7/);
  assert.match(calls[0]!.sql, /nc\.calculation_status = 'computed'/);
  assert.match(calls[0]!.sql, /\$1::double precision/);
  assert.match(calls[0]!.sql, /\$4::double precision/);
  assert.equal(calls[0]!.params.at(-1), 7);
});

test('single-node coverage cannot label stale or terminal geometry ready', async (t) => {
  const previousViewshed = process.env['VIEWSHED_ENABLED'];
  process.env['VIEWSHED_ENABLED'] = '1';
  t.after(() => {
    if (previousViewshed === undefined) delete process.env['VIEWSHED_ENABLED'];
    else process.env['VIEWSHED_ENABLED'] = previousViewshed;
  });

  const nodeId = 'A'.repeat(64);
  const calls: Array<{ sql: string; params: unknown[] }> = [];
  await withCoverageServer(async (sql, params = []) => {
    calls.push({ sql, params });
    if (calls.length === 1) return { rows: [] };
    return { rows: [{ lat: null, lon: null }] };
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/coverage/${nodeId}`);
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { status: 'unavailable' });
  });

  assert.equal(calls.length, 2);
  assert.match(calls[0]!.sql, /nc\.model_version = \$2/);
  assert.match(calls[0]!.sql, /nc\.calculation_status = 'computed'/);
  assert.deepEqual(calls[0]!.params, [nodeId, 7]);
});
