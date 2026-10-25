import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router } from 'express';
import type { QueryResultRow } from 'pg';
import type { QueryFn } from '../../repositories/productFeatures.js';
import { registerProductFeatureRoutes } from './productFeatures.js';

const nodeA = 'A'.repeat(64);
const nodeB = 'B'.repeat(64);

async function withHistoryApi(
  visibleIds: string[],
  points: QueryResultRow[],
  check: (baseUrl: string, historyParams: unknown[][]) => Promise<void>,
) {
  const historyParams: unknown[][] = [];
  const query: QueryFn = async <T extends QueryResultRow>(sql: string, params?: unknown[]) => {
    if (sql.includes('FROM node_identity_nodes')) {
      return { rows: visibleIds.map((node_id) => ({ node_id })) as unknown as T[] };
    }
    historyParams.push(params ?? []);
    return { rows: points as T[] };
  };
  const app = express();
  const router = Router();
  registerProductFeatureRoutes(router, query);
  app.use('/api', router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    await check(`http://127.0.0.1:${(server.address() as AddressInfo).port}`, historyParams);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}

test('link history preserves its response contract and forwards the public packet scope', async () => {
  const points = [{ time: '2026-09-07T12:00:00.123Z', snr: -7, rssi: -99, path_loss: 110, sample_count: 1 }];
  await withHistoryApi([nodeA, nodeB], points, async (baseUrl, params) => {
    const response = await fetch(`${baseUrl}/api/links/${nodeB.toLowerCase()}:${nodeA.toLowerCase()}/history`);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('cache-control'), 'public, max-age=60, stale-while-revalidate=300');
    assert.deepEqual(await response.json(), { linkId: `${nodeA}:${nodeB}`, network: 'ukmesh', hours: 72, points });
    assert.deepEqual(params, [[nodeA, nodeB, '72', ['ukmesh', 'northeast', 'teesside']]]);
  });
});

test('a fabricated pairing of visible nodes with no traffic returns HTTP 200 and empty points', async () => {
  await withHistoryApi([nodeA, nodeB], [], async (baseUrl, params) => {
    const response = await fetch(`${baseUrl}/api/links/${nodeA}:${nodeB}/history?network=test&hours=168`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { linkId: `${nodeA}:${nodeB}`, network: 'test', hours: 168, points: [] });
    assert.deepEqual(params, [[nodeA, nodeB, '168', ['test']]]);
  });
});

test('history never reads packets when either endpoint is outside visibility scope', async () => {
  await withHistoryApi([nodeA], [], async (baseUrl, params) => {
    const response = await fetch(`${baseUrl}/api/links/${nodeA}:${nodeB}/history?hours=72`);
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { error: 'Link not found' });
    assert.deepEqual(params, []);
  });
});

test('history rejects windows above 168 hours before querying packets', async () => {
  type Handler = (req: unknown, res: unknown) => Promise<void>;
  const handlers = new Map<string, Handler>();
  const router = {
    get: (path: string, handler: Handler) => handlers.set(path, handler),
    post: () => undefined,
  } as unknown as Router;
  registerProductFeatureRoutes(router, async () => {
    assert.fail('Invalid windows must not reach the database');
  });
  await assert.rejects(handlers.get('/links/:id/history')!({
    params: { id: `${nodeA}:${nodeB}` }, query: { hours: '169' },
  }, {}), /hours must be between 1 and 168/);
});
