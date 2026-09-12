import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { registerPathingRoutes } from './pathing.js';

const passThroughLimiter: RequestHandler = (_req, _res, next) => next();

async function withPathingServer(
  run: Parameters<typeof registerPathingRoutes>[1]['resolvePool']['run'],
  callback: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const router = Router();
  registerPathingRoutes(router, {
    pathBetaLimiter: passThroughLimiter as Parameters<typeof registerPathingRoutes>[1]['pathBetaLimiter'],
    pathLearningLimiter: passThroughLimiter as Parameters<typeof registerPathingRoutes>[1]['pathLearningLimiter'],
    getResolveCache: () => undefined,
    setResolveCache: () => undefined,
    getHeldPath: () => undefined,
    setHeldPath: () => undefined,
    resolvePool: { run },
    getPublicVisibilityGeneration: async () => 1,
    getMultibytePathSegments: async () => ({ maxCount: 0, segments: [] }),
    query: async () => ({ rows: [] }),
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

test('lazy path requests execute in the bounded resolver worker pool', async () => {
  const jobs: unknown[] = [];
  await withPathingServer(async (job) => {
    jobs.push(job);
    return { packetHash: 'ABCD', observerCount: 1, paths: [] };
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/path-lazy/resolve?hash=ABCD`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {
      packetHash: 'ABCD',
      observerCount: 1,
      paths: [],
    });
  });
  assert.deepEqual(jobs, [{ type: 'resolveLazy', packetHash: 'ABCD', network: 'ukmesh' }]);
});

test('lazy path saturation is exposed as a retryable response', async () => {
  await withPathingServer(async () => {
    throw new Error('PATH_RESOLVE_TIMEOUT');
  }, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/path-lazy/resolve?hash=ABCD`);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), {
      error: 'Path resolver is busy',
      retryable: true,
    });
  });
});

test('path learning export is hidden from proxied public callers', async () => {
  await withPathingServer(async () => null, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/path-learning`, {
      headers: { 'x-forwarded-for': '203.0.113.10' },
    });
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { error: 'Not found' });
  });
});

test('path learning export requires the operator token for local callers', async (t) => {
  const previousToken = process.env['OPERATOR_SITE_TOKEN'];
  delete process.env['OPERATOR_SITE_TOKEN'];
  t.after(() => {
    if (previousToken === undefined) delete process.env['OPERATOR_SITE_TOKEN'];
    else process.env['OPERATOR_SITE_TOKEN'] = previousToken;
  });

  await withPathingServer(async () => null, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/path-learning`);
    assert.equal(response.status, 401);
    assert.deepEqual(await response.json(), { error: 'Operator authentication required' });
  });
});

test('path learning export stays available to authenticated local operators', async (t) => {
  const previousToken = process.env['OPERATOR_SITE_TOKEN'];
  process.env['OPERATOR_SITE_TOKEN'] = 'test-operator-token-with-at-least-32-chars';
  t.after(() => {
    if (previousToken === undefined) delete process.env['OPERATOR_SITE_TOKEN'];
    else process.env['OPERATOR_SITE_TOKEN'] = previousToken;
  });

  await withPathingServer(async () => null, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/path-learning`, {
      headers: { 'x-operator-token': process.env['OPERATOR_SITE_TOKEN'] ?? '' },
    });
    assert.equal(response.status, 200);
    const body = await response.json() as {
      network: string;
      prefixPriors: unknown[];
      transitionPriors: unknown[];
      edgePriors: unknown[];
      motifPriors: unknown[];
    };
    assert.equal(body.network, 'ukmesh');
    assert.deepEqual(body.prefixPriors, []);
    assert.deepEqual(body.transitionPriors, []);
    assert.deepEqual(body.edgePriors, []);
    assert.deepEqual(body.motifPriors, []);
  });
});
