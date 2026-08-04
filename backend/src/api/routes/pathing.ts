import type { Router } from 'express';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { createPathingRepository } from '../../pathing/pathingRepository.js';
import { createPathingService } from '../../pathing/pathingService.js';
import {
  slowModeRemainingMs,
  slowModeStatus,
  slowModeWindowMs,
} from '../../path-beta/slowMode.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import { parseBoundedInteger, parseHexIdentifier } from '../utils/input.js';
import type { HeldPathEntry } from '../../path-beta/resolveCache.js';

type ResolvePoolFn = {
  run<T>(job:
    | { type: 'resolve'; packetHash: string; network: string; observer?: string | null; heldPath?: HeldPathEntry }
    | { type: 'resolveMulti'; packetHash: string; network: string; heldPath?: HeldPathEntry }
    | { type: 'resolveLazy'; packetHash: string; network: string }
  ): Promise<T | null>;
};

type PathingRouteDeps = {
  pathBetaLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathHistoryLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathLearningLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathHistoryCache: Map<string, { ts: number; data: unknown }>;
  pathHistoryCacheTtlMs: number;
  getResolveCache: (key: string) => unknown;
  setResolveCache: (key: string, value: unknown) => void;
  getHeldPath: (packetHash: string, network: string) => HeldPathEntry | undefined;
  setHeldPath: (packetHash: string, network: string, value: HeldPathEntry) => void;
  resolvePool: ResolvePoolFn;
  getPublicVisibilityGeneration: () => Promise<number>;
  getPathHistoryCache: (scope: string, visibilityGeneration: number) => Promise<{
    window_start: string | null;
    updated_at: string | null;
    packet_count: number;
    resolved_packet_count: number;
    segment_counts: Array<{ count?: number }> | null;
    visibility_generation: number;
  } | null>;
  getMultibytePathSegments: (network?: string, observer?: string) => Promise<{
    maxCount: number;
    segments: Array<{
      positions: [[number, number], [number, number]];
      count: number;
    }>;
  }>;
  query: <T extends import('pg').QueryResultRow = import('pg').QueryResultRow>(
    text: string,
    params?: unknown[],
  ) => Promise<{ rows: T[] }>;
};

export function registerPathingRoutes(router: Router, deps: PathingRouteDeps): void {
  const repository = createPathingRepository({
    getPathHistoryCache: deps.getPathHistoryCache,
    getPublicVisibilityGeneration: deps.getPublicVisibilityGeneration,
    query: deps.query,
  });

  const service = createPathingService({
    pathHistoryCache: deps.pathHistoryCache,
    pathHistoryCacheTtlMs: deps.pathHistoryCacheTtlMs,
    getResolveCache: deps.getResolveCache,
    setResolveCache: deps.setResolveCache,
    getHeldPath: deps.getHeldPath,
    setHeldPath: deps.setHeldPath,
    resolvePool: deps.resolvePool,
    repository,
  });

  router.get('/path-beta/resolve', deps.pathBetaLimiter, async (req, res) => {
    const packetHash = parseHexIdentifier(req.query['hash'], {
      name: 'hash',
      maxLength: 128,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      res.json(await service.resolvePacket(packetHash, network, observer));
    } catch (err) {
      const message = (err as Error).message;
      if (message === 'PACKET_NOT_FOUND') {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      if (message === 'PATH_HISTORY_LIMIT') {
        res.status(422).json({ error: 'HISTORY_LIMIT', retryable: false });
        return;
      }
      if (message === 'PATH_RESOLVE_OVERLOADED' || message === 'PATH_RESOLVE_TIMEOUT') {
        res.status(503).json({ error: 'Path resolver is busy', retryable: true });
        return;
      }
      console.error('[api] GET /path-beta/resolve', message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/resolve-multi', deps.pathBetaLimiter, async (req, res) => {
    const packetHash = parseHexIdentifier(req.query['hash'], {
      name: 'hash',
      maxLength: 128,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      if (req.query['mode'] === 'slow') {
        const remainingMs = slowModeRemainingMs(packetHash, network);
        if (remainingMs > 0) {
          res.status(202).json({
            status: 'pending',
            remainingMs,
            windowMs: slowModeWindowMs(),
          });
          return;
        }
      }
      res.json(await service.resolvePacketMulti(packetHash, network));
    } catch (err) {
      const message = (err as Error).message;
      if (message === 'PACKET_NOT_FOUND') {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      if (message === 'PATH_HISTORY_LIMIT') {
        res.status(422).json({ error: 'HISTORY_LIMIT', retryable: false });
        return;
      }
      if (message === 'PATH_RESOLVE_OVERLOADED' || message === 'PATH_RESOLVE_TIMEOUT') {
        res.status(503).json({ error: 'Path resolver is busy', retryable: true });
        return;
      }
      console.error('[api] GET /path-beta/resolve-multi', message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/slow-mode', (_req, res) => {
    res.json(slowModeStatus());
  });

  router.get('/path-beta/history', deps.pathHistoryLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      res.json(await service.getPathHistory(network));
    } catch (err) {
      console.error('[api] GET /path-beta/history', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/multibyte-paths', deps.pathHistoryLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const { maxCount, segments } = await deps.getMultibytePathSegments(network, observer ?? undefined);
      res.json({
        ok: true,
        scope: network,
        maxCount,
        segments,
      });
    } catch (err) {
      console.error('[api] GET /path-beta/multibyte-paths', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-lazy/resolve', deps.pathBetaLimiter, async (req, res) => {
    const packetHash = parseHexIdentifier(req.query['hash'], {
      name: 'hash',
      maxLength: 128,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const result = await deps.resolvePool.run({
        type: 'resolveLazy',
        packetHash,
        network,
      });
      if (!result) {
        res.status(404).json({ error: 'No path data found for this packet' });
        return;
      }
      res.json(result);
    } catch (err) {
      if ((err as Error).message === 'PATH_HISTORY_LIMIT') {
        res.status(422).json({ error: 'HISTORY_LIMIT', retryable: false });
        return;
      }
      if ((err as Error).message === 'PATH_RESOLVE_OVERLOADED' || (err as Error).message === 'PATH_RESOLVE_TIMEOUT') {
        res.status(503).json({ error: 'Path resolver is busy', retryable: true });
        return;
      }
      console.error('[api] GET /path-lazy/resolve', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-learning', deps.pathLearningLimiter, async (req, res) => {
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 6000,
      min: 1000,
      max: 12000,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      res.json(await service.getPathLearning(network, limit));
    } catch (err) {
      console.error('[api] GET /path-learning', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
