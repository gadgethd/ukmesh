import type { Router } from 'express';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import { createPathingRepository } from '../../pathing/pathingRepository.js';
import { createPathingService } from '../../pathing/pathingService.js';
import { normalizeObserverQuery } from '../utils/observer.js';

type ResolvePoolFn = {
  run<T>(job: { type: 'resolve'; packetHash: string; network: string; observer?: string | null } | { type: 'resolveMulti'; packetHash: string; network: string }): Promise<T | null>;
};

type PathingRouteDeps = {
  pathBetaLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathHistoryLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathLearningLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathHistoryCache: Map<string, { ts: number; data: unknown }>;
  pathHistoryCacheTtlMs: number;
  getResolveCache: (key: string) => unknown;
  setResolveCache: (key: string, value: unknown) => void;
  resolvePool: ResolvePoolFn;
  getPathHistoryCache: (scope: string) => Promise<{
    window_start: string | null;
    updated_at: string | null;
    packet_count: number;
    resolved_packet_count: number;
    segment_counts: Array<{ count?: number }> | null;
  } | null>;
  query: <T extends import('pg').QueryResultRow = import('pg').QueryResultRow>(
    text: string,
    params?: unknown[],
  ) => Promise<{ rows: T[] }>;
};

export function registerPathingRoutes(router: Router, deps: PathingRouteDeps): void {
  const repository = createPathingRepository({
    getPathHistoryCache: deps.getPathHistoryCache,
    query: deps.query,
  });

  const service = createPathingService({
    pathHistoryCache: deps.pathHistoryCache,
    pathHistoryCacheTtlMs: deps.pathHistoryCacheTtlMs,
    getResolveCache: deps.getResolveCache,
    setResolveCache: deps.setResolveCache,
    resolvePool: deps.resolvePool,
    repository,
  });

  router.get('/path-beta/resolve', deps.pathBetaLimiter, async (req, res) => {
    try {
      const packetHash = String(req.query['hash'] ?? '').trim();
      if (!packetHash) {
        res.status(400).json({ error: 'Missing hash query parameter' });
        return;
      }
      if (!/^[0-9a-fA-F]{1,128}$/.test(packetHash)) {
        res.status(400).json({ error: 'Invalid hash format' });
        return;
      }
      const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
      const observer = normalizeObserverQuery(req.query['observer']);
      res.json(await service.resolvePacket(packetHash, network, observer));
    } catch (err) {
      if ((err as Error).message === 'PACKET_NOT_FOUND') {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      console.error('[api] GET /path-beta/resolve', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/resolve-multi', deps.pathBetaLimiter, async (req, res) => {
    try {
      const packetHash = String(req.query['hash'] ?? '').trim();
      if (!packetHash) {
        res.status(400).json({ error: 'Missing hash query parameter' });
        return;
      }
      if (!/^[0-9a-fA-F]{1,128}$/.test(packetHash)) {
        res.status(400).json({ error: 'Invalid hash format' });
        return;
      }
      const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
      res.json(await service.resolvePacketMulti(packetHash, network));
    } catch (err) {
      if ((err as Error).message === 'PACKET_NOT_FOUND') {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      console.error('[api] GET /path-beta/resolve-multi', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/history', deps.pathHistoryLimiter, async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const scope = requestedNetwork === 'all' ? 'all' : (requestedNetwork ?? 'teesside');
      res.json(await service.getPathHistory(scope));
    } catch (err) {
      console.error('[api] GET /path-beta/history', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-learning', deps.pathLearningLimiter, async (req, res) => {
    try {
      const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
      const limit = Math.min(12000, Math.max(1000, Number(req.query['limit'] ?? 6000)));
      res.json(await service.getPathLearning(network, limit));
    } catch (err) {
      console.error('[api] GET /path-learning', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
