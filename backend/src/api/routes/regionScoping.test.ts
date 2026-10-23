import assert from 'node:assert/strict';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express, { Router, type RequestHandler } from 'express';
import { apiErrorMiddleware } from '../errors.js';
import { networkFilters } from '../utils/networkFilters.js';
import { createNodeRepository } from '../../repositories/nodes.js';
import { registerMiscRoutes } from './misc.js';
import { registerNodeRoutes } from './nodes.js';
import { registerPathingRoutes } from './pathing.js';

test('region API validates before reads, scopes each node/packet path and preserves absent-region calls', async (t) => {
  const previous = process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'];
  const calls: Array<{ path: string; args: unknown[] }> = [];
  const record = (path: string) => async (...args: unknown[]) => {
    calls.push({ path, args });
    return [{ node_id: 'A'.repeat(64), name: 'Fixture' }];
  };
  const limiter: RequestHandler = (_req, _res, next) => next();
  const repository = createNodeRepository(async () => ({ rows: [] }));
  repository.listPublicMapRows = async (_fields, filters) => {
    calls.push({ path: 'map', args: [filters] });
    return [];
  };
  repository.listNodeLinks = async (_id, filters) => {
    calls.push({ path: 'links', args: [filters] });
    return [];
  };
  const router = Router();
  registerNodeRoutes(router, {
    nodeRepository: repository, getNodes: record('nodes'),
    getNodeHistory: async () => [], getNodeAdverts: async () => [],
    getTopAdvertingRepeaters: async () => [], requireLocalOnly: () => false,
    networkFilters, getPublicVisibilityGeneration: async () => 1,
    inferredNodesCache: new Map(), inferredNodesInflight: new Map(), inferredNodesCacheTtlMs: 60_000,
    nodeLinksCache: new Map(), nodeLinksInflight: new Map(), nodeLinksCacheTtlMs: 60_000,
    nodesLimiter: limiter as Parameters<typeof registerNodeRoutes>[1]['nodesLimiter'],
  });
  registerMiscRoutes(router, {
    query: async () => ({ rows: [] }), getRecentPackets: record('packets'),
    getRecentPacketEvents: record('raw'), getPacketDetail: async () => null,
    getChannelMessageHistory: async () => [], getRecentMessageTags: async () => [],
    getPublicVisibilityGeneration: async () => 1,
    packetDetailLimiter: limiter as Parameters<typeof registerMiscRoutes>[1]['packetDetailLimiter'],
  });
  registerPathingRoutes(router, {
    pathBetaLimiter: limiter as Parameters<typeof registerPathingRoutes>[1]['pathBetaLimiter'],
    pathLearningLimiter: limiter as Parameters<typeof registerPathingRoutes>[1]['pathLearningLimiter'],
    getResolveCache: () => undefined, setResolveCache: () => {},
    getHeldPath: () => undefined, setHeldPath: () => {}, resolvePool: { run: async () => null },
    getPublicVisibilityGeneration: async () => 1, query: async () => ({ rows: [] }),
    getMultibytePathSegments: async (...args) => {
      calls.push({ path: 'history', args });
      return { maxCount: 0, segments: [] };
    },
  });
  const app = express();
  app.use(router, apiErrorMiddleware);
  const server = app.listen(0, '127.0.0.1');
  t.after(() => {
    server.closeAllConnections();
    server.close();
    if (previous === undefined) delete process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'];
    else process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = previous;
  });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const paths = ['/nodes', '/nodes/map', `/nodes/${'A'.repeat(64)}/links`, '/path-beta/multibyte-paths', '/packets/recent', '/packets/recent?raw=1'];
  process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = 'false';
  for (const path of paths) {
    const sep = path.includes('?') ? '&' : '?';
    const before = calls.length;
    const disabled = await fetch(`${base}${path}${sep}region=eng-ne`);
    assert.equal(disabled.status, 400);
    assert.equal((await disabled.json()).code, 'FEATURE_DISABLED');
    assert.equal(calls.length, before);
    const unscoped = await fetch(`${base}${path}`);
    assert.equal(unscoped.status, 200);
    if (path === '/nodes') assert.deepEqual(calls.at(-1)?.args, ['ukmesh', undefined, 'full']);
    if (path === '/packets/recent') assert.equal(calls.at(-1)?.args.length, 7);
  }
  process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'] = 'true';
  for (const path of paths) {
    const sep = path.includes('?') ? '&' : '?';
    for (const value of ['a%20b', '%23', 'a'.repeat(65), 'eng-ne&region=Europe', 'x%27%20OR%201=1']) {
      const before = calls.length;
      assert.equal((await fetch(`${base}${path}${sep}region=${value}`)).status, 400);
      assert.equal(calls.length, before);
    }
    assert.equal((await fetch(`${base}${path}${sep}region=%23Europe`)).status, 200);
    const call = calls.at(-1)!;
    if (call.path === 'map' || call.path === 'links') {
      const filters = call.args[0] as ReturnType<typeof networkFilters>;
      assert.deepEqual(filters.params, [['ukmesh', 'northeast', 'teesside'], 'Europe']);
      assert.match(filters.nodesAlias('n'), /WHERE m.region = \$2/);
    } else assert.equal(call.args.at(-1), 'Europe');
  }
});
