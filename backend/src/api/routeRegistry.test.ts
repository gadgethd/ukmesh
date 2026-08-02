import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Router } from 'express';
import {
  assertUniqueRouteRegistry,
  listRegisteredRoutes,
} from './routeRegistry.js';

test('route registry includes routes from mounted routers', () => {
  const parent = Router();
  const child = Router();
  child.get('/nodes/map', (_req, res) => res.end());
  child.post('/nodes/query', (_req, res) => res.end());
  parent.use(child);

  assert.deepEqual(
    listRegisteredRoutes(parent).sort(),
    ['GET /nodes/map', 'POST /nodes/query'],
  );
  assert.doesNotThrow(() => assertUniqueRouteRegistry(parent));
});

test('route registry fails closed for duplicate method and path pairs', () => {
  const router = Router();
  router.get('/nodes/map', (_req, res) => res.end());
  router.get('/nodes/map', (_req, res) => res.end());

  assert.throws(
    () => assertUniqueRouteRegistry(router),
    /duplicate API method\/path registration: GET \/nodes\/map/,
  );
});
