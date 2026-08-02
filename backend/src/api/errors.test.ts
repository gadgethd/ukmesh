import assert from 'node:assert/strict';
import http from 'node:http';
import test from 'node:test';
import express, { Router } from 'express';
import {
  apiErrorMiddleware,
  requestContextMiddleware,
  wrapAsyncHandlers,
} from './errors.js';

test('rejected Express 4 async handlers return a bounded response with a request ID', async () => {
  const app = express();
  const router = Router();
  router.get('/failure', async () => {
    throw new Error('database contains sensitive detail');
  });
  wrapAsyncHandlers(router);
  app.use(requestContextMiddleware);
  app.use(router);
  app.use(apiErrorMiddleware);
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address === 'object');
    const response = await fetch(`http://127.0.0.1:${address.port}/failure`);
    assert.equal(response.status, 500);
    assert.match(response.headers.get('x-request-id') ?? '', /^[0-9a-f-]{36}$/);
    const body = await response.json() as Record<string, unknown>;
    assert.equal(body['code'], 'INTERNAL_ERROR');
    assert.equal(JSON.stringify(body).includes('sensitive'), false);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
