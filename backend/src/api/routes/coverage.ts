import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import { isViewshedEligibleCoordinate, queueViewshedJob } from '../../queue/publisher.js';
import { normalizeObserverQuery } from '../utils/observer.js';

type NetworkFilters = {
  params: string[];
  nodesAlias: (alias: string) => string;
};

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type CoverageRouteDeps = {
  coverageCache: Map<string, { ts: number; data: unknown }>;
  coverageCacheTtlMs: number;
  coverageLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  query: QueryFn;
};

export function registerCoverageRoutes(router: Router, deps: CoverageRouteDeps): void {
  const {
    coverageCache,
    coverageCacheTtlMs,
    coverageLimiter,
    networkFilters,
    query,
  } = deps;

  router.get('/coverage', coverageLimiter, async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const observer = normalizeObserverQuery(req.query['observer']);
      const coverageCacheKey = `${network ?? 'all'}:${observer ?? ''}`;

      const coverageCached = coverageCache.get(coverageCacheKey);
      if (coverageCached && Date.now() - coverageCached.ts < coverageCacheTtlMs) {
        res.json(coverageCached.data);
        return;
      }

      const filters = networkFilters(network, observer);
      const result = await query(
        `SELECT nc.node_id, nc.geom, nc.strength_geoms, nc.antenna_height_m, nc.radius_m, nc.calculated_at
         FROM node_coverage nc
         JOIN nodes n ON n.node_id = nc.node_id
         WHERE (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           AND (n.role IS NULL OR n.role = 2)
           ${filters.nodesAlias('n')}`,
        filters.params,
      );
      coverageCache.set(coverageCacheKey, { ts: Date.now(), data: result.rows });
      res.json(result.rows);
    } catch (err) {
      console.error('[api] GET /coverage', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/coverage/:nodeId', coverageLimiter, async (req, res) => {
    try {
      const nodeId = String(req.params.nodeId ?? '').trim().toUpperCase();
      if (!/^[0-9A-F]{64}$/.test(nodeId)) {
        res.status(400).json({ error: 'invalid node id' });
        return;
      }

      const existing = await query<{
        node_id: string;
        geom: unknown;
        strength_geoms: unknown;
        antenna_height_m: number | null;
        radius_m: number | null;
        calculated_at: string | null;
      }>(
        `SELECT nc.node_id, nc.geom, nc.strength_geoms, nc.antenna_height_m, nc.radius_m, nc.calculated_at::text AS calculated_at
         FROM node_coverage nc
         JOIN nodes n ON n.node_id = nc.node_id
         WHERE nc.node_id = $1
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         LIMIT 1`,
        [nodeId],
      );
      if (existing.rows[0]) {
        res.json({ status: 'ready', coverage: existing.rows[0] });
        return;
      }

      const nodeResult = await query<{ lat: number | null; lon: number | null }>(
        `SELECT lat, lon
         FROM nodes
         WHERE node_id = $1
         LIMIT 1`,
        [nodeId],
      );
      const node = nodeResult.rows[0];
      if (!node) {
        res.status(404).json({ error: 'node not found' });
        return;
      }

      if (typeof node.lat === 'number' && typeof node.lon === 'number' && isViewshedEligibleCoordinate(node.lat, node.lon)) {
        queueViewshedJob(nodeId, node.lat, node.lon);
        res.status(202).json({ status: 'queued' });
        return;
      }

      res.status(404).json({ status: 'unavailable' });
    } catch (err) {
      console.error('[api] GET /coverage/:nodeId', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
