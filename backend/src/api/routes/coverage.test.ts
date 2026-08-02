import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { registerCoverageRoutes } from './coverage.js';

const passThroughLimiter: RequestHandler = (_req, _res, next) => next();

test('legacy coverage contracts return 410 without reading rollback data', async () => {
  let queries = 0;
  const router = Router();
  registerCoverageRoutes(router, {
    coverageLimiter: passThroughLimiter,
    query: async () => { queries += 1; return { rows: [] }; },
  });
  const app = express();
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const { port } = server.address() as AddressInfo;
    for (const path of ['/coverage?bbox=-2,50,1,55', `/coverage/${'A'.repeat(64)}`]) {
      const response = await fetch(`http://127.0.0.1:${port}${path}`);
      assert.equal(response.status, 410);
      assert.equal((await response.json() as { replacement: string }).replacement, '/rf-coverage/meta.json');
    }
    assert.equal(queries, 0);
  } finally {
    server.close();
  }
});
