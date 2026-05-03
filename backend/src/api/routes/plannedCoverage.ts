import { randomBytes } from 'crypto';
import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { isViewshedEligibleCoordinate, queueViewshedJob } from '../../queue/publisher.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type PlannedCoverageRouteDeps = {
  coverageLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  query: QueryFn;
};

const PLAN_ID_RE = /^plan_[0-9a-f]{16}$/;

export function registerPlannedCoverageRoutes(router: Router, deps: PlannedCoverageRouteDeps): void {
  const { coverageLimiter, query } = deps;

  /** Queue a viewshed job for a hypothetical repeater location. Returns a plan_id to poll. */
  router.post('/coverage/planned', coverageLimiter, async (req, res) => {
    try {
      const body = req.body as Record<string, unknown>;
      const lat = body['lat'];
      const lon = body['lon'];
      if (typeof lat !== 'number' || typeof lon !== 'number') {
        res.status(400).json({ error: 'lat and lon are required numbers' });
        return;
      }
      if (!isViewshedEligibleCoordinate(lat, lon)) {
        res.status(400).json({ error: 'Location must be within the UK' });
        return;
      }
      const planId = `plan_${randomBytes(8).toString('hex')}`;
      queueViewshedJob(planId, lat, lon);
      res.json({ plan_id: planId });
    } catch (err) {
      console.error('[api] POST /coverage/planned', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  /** Poll for a planned coverage result. Returns {status:'ready',coverage:{...}} or {status:'pending'}. */
  router.get('/coverage/planned/:planId', coverageLimiter, async (req, res) => {
    try {
      const planId = String(req.params['planId'] ?? '').trim();
      if (!PLAN_ID_RE.test(planId)) {
        res.status(400).json({ error: 'invalid plan id' });
        return;
      }
      const result = await query<{
        node_id: string;
        geom: unknown;
        strength_geoms: unknown;
        antenna_height_m: number | null;
        radius_m: number | null;
        calculated_at: string | null;
      }>(
        `SELECT node_id, geom, strength_geoms, antenna_height_m, radius_m, calculated_at::text AS calculated_at
         FROM node_coverage
         WHERE node_id = $1
         LIMIT 1`,
        [planId],
      );
      if (result.rows[0]) {
        res.json({ status: 'ready', coverage: result.rows[0] });
      } else {
        res.json({ status: 'pending' });
      }
    } catch (err) {
      console.error('[api] GET /coverage/planned/:planId', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  /** Remove a planned repeater's coverage data. */
  router.delete('/coverage/planned/:planId', coverageLimiter, async (req, res) => {
    try {
      const planId = String(req.params['planId'] ?? '').trim();
      if (!PLAN_ID_RE.test(planId)) {
        res.status(400).json({ error: 'invalid plan id' });
        return;
      }
      await query('DELETE FROM node_coverage WHERE node_id = $1', [planId]);
      res.status(204).send();
    } catch (err) {
      console.error('[api] DELETE /coverage/planned/:planId', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
