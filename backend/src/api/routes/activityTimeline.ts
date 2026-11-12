import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../../platform/query/networkFilters.js';
import { parseBoundedInteger } from '../utils/input.js';
import { publicJson } from '../../http/publicJson.js';
import { createTimelineRepository } from '../../timeline/repository.js';
import { createTimelineService } from '../../timeline/service.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;

type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  getPublicVisibilityGeneration: () => Promise<number>;
};

export function registerActivityTimelineRoutes(router: Router, deps: Deps): () => void {
  const service = createTimelineService(createTimelineRepository(deps), deps.getPublicVisibilityGeneration);
  router.get('/activity/timeline', deps.limiter, async (req, res) => {
    const minutes = parseBoundedInteger(req.query['minutes'], {
      name: 'minutes',
      defaultValue: 360,
      min: 60,
      max: 1_440,
    });
    const bucketMinutes = parseBoundedInteger(req.query['bucket'], {
      name: 'bucket',
      defaultValue: 15,
      min: 5,
      max: 60,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      await publicJson(res, deps.getPublicVisibilityGeneration, generation => service.get({ network, minutes, bucketMinutes }, generation));
    } catch (err) {
      console.error('[api] GET /activity/timeline', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
  return service.close;
}
