import assert from 'node:assert/strict';
import test from 'node:test';
import type { Router } from 'express';
import { registerActivityTimelineRoutes } from './activityTimeline.js';

test('activity timeline excludes packets involving private endpoints or relays', async () => {
  let routeHandler: ((req: any, res: any) => Promise<void>) | undefined;
  const router = {
    get(_path: string, ...handlers: Array<(req: any, res: any) => unknown>) {
      routeHandler = handlers.at(-1) as (req: any, res: any) => Promise<void>;
    },
  } as unknown as Router;

  let capturedSql = '';
  registerActivityTimelineRoutes(router, {
    query: async (text) => {
      capturedSql = text;
      return { rows: [] };
    },
    networkFilters: () => ({
      params: ['ukmesh'],
      packets: 'AND network = $1',
      packetsAlias: (alias: string) => `AND ${alias}.network = $1`,
      nodes: 'AND network = $1',
      nodesAlias: (alias: string) => `AND ${alias}.network = $1`,
    }),
    limiter: ((_req: unknown, _res: unknown, next: () => void) => next()) as any,
  });

  assert.ok(routeHandler);
  const response = {
    setHeader() {},
    json() {},
    status() { return this; },
  };
  await routeHandler({ query: {}, headers: {} }, response);

  assert.match(capturedSql, /private_node\.node_id IN \(p\.rx_node_id, p\.src_node_id\)/);
  assert.match(capturedSql, /COALESCE\(cardinality\(p\.path_hashes\), 0\) = 0/);
  assert.match(capturedSql, /p\.path_hash_size_bytes BETWEEN 1 AND 3/);
  assert.match(capturedSql, /malformed_path_hash IS NULL/);
  assert.match(capturedSql, /length\(malformed_path_hash\) <> p\.path_hash_size_bytes \* 2/);
  assert.match(capturedSql, /malformed_path_hash !~ '\^\[0-9A-Fa-f\]\+\$'/);
  assert.match(capturedSql, /UPPER\(private_node\.node_id\) LIKE UPPER\(path_hash\) \|\| '%'/);
});
