import type { Router } from 'express';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { createStatsRepository } from '../../stats/statsRepository.js';
import { createStatsService, StatsWorkOverloadedError } from '../../stats/statsService.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type MaskDecodedPathNodesFn = (
  rawNodes: Array<{
    ord: number;
    node_id: string | null;
    name: string | null;
    lat: number | null;
    lon: number | null;
    last_seen?: string | null;
  }> | null | undefined,
) => Array<{
  ord: number;
  node_id: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
}>;

type StatsRouteDeps = {
  statsCache: Map<string, { ts: number; data: unknown }>;
  statsCacheTtlMs: number;
  chartsCache: Map<string, { ts: number; data: unknown }>;
  chartsCacheTtlMs: number;
  chartsSnapshotStaleTtlMs: number;
  chartsInflight: Map<string, Promise<unknown>>;
  expensiveLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  statsChartsLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  networkFilters: (network?: string, observer?: string, opts?: { includePrivacy?: boolean }) => NetworkFilters;
  query: QueryFn;
  analyticsQuery: QueryFn;
  getPublicVisibilityGeneration: () => Promise<number>;
  maskDecodedPathNodes: MaskDecodedPathNodesFn;
};

export function registerStatsRoutes(router: Router, deps: StatsRouteDeps): void {
  const repository = createStatsRepository({
    networkFilters: deps.networkFilters,
    query: deps.query,
  });
  const chartRepository = createStatsRepository({
    networkFilters: deps.networkFilters,
    query: deps.analyticsQuery,
  });

  const service = createStatsService({
    statsCache: deps.statsCache,
    statsCacheTtlMs: deps.statsCacheTtlMs,
    chartsCache: deps.chartsCache,
    chartsCacheTtlMs: deps.chartsCacheTtlMs,
    chartsSnapshotStaleTtlMs: deps.chartsSnapshotStaleTtlMs,
    chartsInflight: deps.chartsInflight,
    repository: {
      ...repository,
      fetchChartsData: chartRepository.fetchChartsData,
      fetchChannelTraffic: chartRepository.fetchChannelTraffic,
    },
    getPublicVisibilityGeneration: deps.getPublicVisibilityGeneration,
    maskDecodedPathNodes: deps.maskDecodedPathNodes,
  });

  service.startChartsWarmup();

  router.get('/stats', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      res.json(await service.getStatsSummary(network, observer));
    } catch (err) {
      if (err instanceof StatsWorkOverloadedError) {
        res.status(503).json({ error: 'Statistics are busy', retryable: true });
        return;
      }
      console.error('[api] GET /stats', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/stats/charts', deps.statsChartsLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      res.json(await service.getCharts(network, observer));
    } catch (err) {
      if (err instanceof StatsWorkOverloadedError) {
        res.status(503).json({ error: 'Statistics are busy', retryable: true });
        return;
      }
      console.error('[api] GET /stats/charts', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/observer-activity', deps.expensiveLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      res.json(await service.getObserverActivity(network));
    } catch (err) {
      console.error('[api] GET /observer-activity', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
