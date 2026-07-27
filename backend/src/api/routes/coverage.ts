import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { isViewshedFeatureEnabled } from '../../features.js';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { isViewshedEligibleCoordinate, queueViewshedJob } from '../../queue/publisher.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import {
  COVERAGE_MAX_VIEWPORT_SPAN_DEGREES,
  boundCoveragePage,
  parseCoverageBounds,
  parseCoverageCursor,
  parseCoverageLimit,
} from './coveragePagination.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type CoverageRouteDeps = {
  coverageLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  query: QueryFn;
};

export function registerCoverageRoutes(router: Router, deps: CoverageRouteDeps): void {
  const {
    coverageLimiter,
    networkFilters,
    query,
  } = deps;

  router.get('/coverage', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'viewshed disabled' });
      return;
    }

    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const bounds = parseCoverageBounds(req.query['bbox']);
      if (!bounds) {
        res.status(400).json({
          error: `bbox is required as minLon,minLat,maxLon,maxLat with a maximum ${COVERAGE_MAX_VIEWPORT_SPAN_DEGREES}° span`,
        });
        return;
      }

      const cursor = parseCoverageCursor(req.query['cursor']);
      if (cursor === undefined) {
        res.status(400).json({ error: 'cursor must be a 64-character hexadecimal node id' });
        return;
      }
      const limit = parseCoverageLimit(req.query['limit']);

      const filters = networkFilters(network, observer);
      const params = [
        ...filters.params,
        bounds.minLon,
        bounds.minLat,
        bounds.maxLon,
        bounds.maxLat,
        cursor ?? '',
        limit + 1,
      ];
      const minLonParam = `$${filters.params.length + 1}`;
      const minLatParam = `$${filters.params.length + 2}`;
      const maxLonParam = `$${filters.params.length + 3}`;
      const maxLatParam = `$${filters.params.length + 4}`;
      const cursorParam = `$${filters.params.length + 5}`;
      const limitParam = `$${filters.params.length + 6}`;

      const result = await query<{ node_id: string }>(
        `SELECT nc.node_id, nc.geom, nc.strength_geoms, nc.antenna_height_m, nc.radius_m, nc.calculated_at
         FROM node_coverage nc
         JOIN nodes n ON n.node_id = nc.node_id
         WHERE (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           AND (n.role IS NULL OR n.role = 2)
           AND n.lat BETWEEN ${minLatParam} - 2 AND ${maxLatParam} + 2
           AND n.lon BETWEEN ${minLonParam} - 2 AND ${maxLonParam} + 2
           AND nc.node_id > ${cursorParam}
           ${filters.nodesAlias('n')}
         ORDER BY nc.node_id
         LIMIT ${limitParam}`,
        params,
      );
      const page = boundCoveragePage(result.rows, limit);
      res.setHeader('Cache-Control', 'public, max-age=30, stale-while-revalidate=60');
      res.json({
        items: page.items,
        page: {
          limit,
          hasMore: page.hasMore,
          nextCursor: page.nextCursor,
        },
      });
    } catch (err) {
      console.error('[api] GET /coverage', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/coverage/:nodeId', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'viewshed disabled' });
      return;
    }

    try {
      const nodeId = String(req.params.nodeId ?? '').trim().toUpperCase();
      if (!/^[0-9A-F]{64}$/.test(nodeId)) {
        res.status(400).json({ error: 'invalid node id' });
        return;
      }
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const nodeIdParam = `$${filters.params.length + 1}`;

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
         WHERE nc.node_id = ${nodeIdParam}
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           ${filters.nodesAlias('n')}
         LIMIT 1`,
        [...filters.params, nodeId],
      );
      if (existing.rows[0]) {
        res.json({ status: 'ready', coverage: existing.rows[0] });
        return;
      }

      const nodeResult = await query<{ lat: number | null; lon: number | null }>(
        `SELECT lat, lon
         FROM nodes
         WHERE node_id = ${nodeIdParam}
           AND (name IS NULL OR name NOT LIKE '%🚫%')
           ${filters.nodes}
         LIMIT 1`,
        [...filters.params, nodeId],
      );
      const node = nodeResult.rows[0];
      if (!node) {
        res.status(404).json({ error: 'node not found' });
        return;
      }

      if (typeof node.lat === 'number' && typeof node.lon === 'number' && isViewshedEligibleCoordinate(node.lat, node.lon)) {
        queueViewshedJob(nodeId, node.lat, node.lon, true);
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
