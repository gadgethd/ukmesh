import type { Router } from 'express';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import { createStatsRepository } from '../../stats/statsRepository.js';
import { createStatsService } from '../../stats/statsService.js';
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
  chartsInflight: Map<string, Promise<unknown>>;
  crossNetworkCache: Map<string, { ts: number; data: unknown }>;
  crossNetworkCacheTtlMs: number;
  expensiveLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  statsChartsLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  query: QueryFn;
  maskDecodedPathNodes: MaskDecodedPathNodesFn;
};

export function registerStatsRoutes(router: Router, deps: StatsRouteDeps): void {
  const repository = createStatsRepository({
    networkFilters: deps.networkFilters,
    query: deps.query,
  });

  const service = createStatsService({
    statsCache: deps.statsCache,
    statsCacheTtlMs: deps.statsCacheTtlMs,
    chartsCache: deps.chartsCache,
    chartsCacheTtlMs: deps.chartsCacheTtlMs,
    chartsInflight: deps.chartsInflight,
    crossNetworkCache: deps.crossNetworkCache,
    crossNetworkCacheTtlMs: deps.crossNetworkCacheTtlMs,
    repository,
    maskDecodedPathNodes: deps.maskDecodedPathNodes,
  });

  service.startChartsWarmup();

  router.get('/stats', async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const observer = normalizeObserverQuery(req.query['observer']);
      res.json(await service.getStatsSummary(network, observer));
    } catch (err) {
      console.error('[api] GET /stats', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/stats/charts', deps.statsChartsLimiter, async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const observer = normalizeObserverQuery(req.query['observer']);
      res.json(await service.getCharts(network, observer));
    } catch (err) {
      console.error('[api] GET /stats/charts', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/observer-activity', deps.expensiveLimiter, async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      res.json(await service.getObserverActivity(network));
    } catch (err) {
      console.error('[api] GET /observer-activity', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/cross-network-connectivity', deps.expensiveLimiter, async (_req, res) => {
    try {
      res.json(await service.getCrossNetworkConnectivity());
    } catch (err) {
      console.error('[api] GET /cross-network-connectivity', (err as Error).message);
      res.status(500).end();
    }
  });
}
