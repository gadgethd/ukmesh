import type { Router } from 'express';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { createPathingRepository } from '../../pathing/pathingRepository.js';
import { createPathingService } from '../../pathing/pathingService.js';
import type { SlowResolutionScheduler } from '../../path-beta/slowMode.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import { parseBoundedInteger, parseHexIdentifier } from '../utils/input.js';
import type { createResolveCoordinator } from '../../path-beta/resolveCoordinator.js';
import { parseRegionFilter } from '../../regions/filter.js';

type PathingRouteDeps = {
  pathBetaLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathLearningLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  resolve: ReturnType<typeof createResolveCoordinator>['resolve'];
  slowMode: Pick<SlowResolutionScheduler, 'slowModeRemainingMs' | 'slowModeStatus' | 'slowModeWindowMs'>;
  getMultibytePathSegments: (network?: string, observer?: string, region?: string) => Promise<{
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
  const { slowModeRemainingMs, slowModeStatus, slowModeWindowMs } = deps.slowMode;
  const repository = createPathingRepository({ query: deps.query });

  const service = createPathingService({
    resolve: deps.resolve,
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
      if (['PATH_RESOLVE_OVERLOADED', 'PATH_RESOLVE_TIMEOUT', 'PATH_RESOLVE_STALE'].includes(message)) {
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
      if (['PATH_RESOLVE_OVERLOADED', 'PATH_RESOLVE_TIMEOUT', 'PATH_RESOLVE_STALE'].includes(message)) {
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

  router.get('/path-beta/multibyte-paths', deps.pathBetaLimiter, async (req, res) => {
    const region = parseRegionFilter(req.query['region']);
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const { maxCount, segments } = region
        ? await deps.getMultibytePathSegments(network, observer ?? undefined, region)
        : await deps.getMultibytePathSegments(network, observer ?? undefined);
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
      const result = await deps.resolve({ kind: 'lazy', packetHash, network });
      if (!result) {
        res.status(404).json({ error: 'No path data found for this packet' });
        return;
      }
      res.json(result.payload);
    } catch (err) {
      if ((err as Error).message === 'PATH_HISTORY_LIMIT') {
        res.status(422).json({ error: 'HISTORY_LIMIT', retryable: false });
        return;
      }
      if (['PATH_RESOLVE_OVERLOADED', 'PATH_RESOLVE_TIMEOUT', 'PATH_RESOLVE_STALE'].includes((err as Error).message)) {
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
