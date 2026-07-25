import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { isViewshedFeatureEnabled } from '../../features.js';
import {
  getPlannedCoverageState,
  isViewshedEligibleCoordinate,
  queuePlannedViewshedJob,
  releasePlannedCoverage,
  resolvePlannedCoverageHandle,
} from '../../queue/publisher.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type PlannedCoverageRouteDeps = {
  coverageLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  query: QueryFn;
};

const PLAN_ID_RE = /^plan_[0-9a-f]{16}$/;

export function toPublicPlannedCoverage<T extends { node_id: string }>(
  planId: string,
  row: T,
): Omit<T, 'node_id'> & { node_id: string } {
  const { node_id: _internalJobId, ...coverage } = row;
  return { ...coverage, node_id: planId };
}

export function registerPlannedCoverageRoutes(router: Router, deps: PlannedCoverageRouteDeps): void {
  const { coverageLimiter, query } = deps;

  /** Queue a viewshed job for a hypothetical repeater location. Returns a plan_id to poll. */
  router.post('/coverage/planned', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'planned coverage disabled' });
      return;
    }

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
      const admission = await queuePlannedViewshedJob(lat, lon);
      if (!('planId' in admission)) {
        res.setHeader('Retry-After', String(admission.retryAfterSeconds));
        res.status(503).json({ error: 'planned coverage capacity is temporarily unavailable' });
        return;
      }
      res.status(202).json({ plan_id: admission.planId, status: admission.status });
    } catch (err) {
      console.error('[api] POST /coverage/planned', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  /** Poll for a planned coverage result. Returns {status:'ready',coverage:{...}} or {status:'pending'}. */
  router.get('/coverage/planned/:planId', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'planned coverage disabled' });
      return;
    }

    try {
      const planId = String(req.params['planId'] ?? '').trim();
      if (!PLAN_ID_RE.test(planId)) {
        res.status(400).json({ error: 'invalid plan id' });
        return;
      }
      const jobId = await resolvePlannedCoverageHandle(planId);
      if (!jobId) {
        res.status(404).json({ status: 'expired' });
        return;
      }
      const result = await query<{
        node_id: string;
        geom: unknown;
        strength_geoms: unknown;
        antenna_height_m: number | null;
        radius_m: number | null;
        predicted_links: unknown;
        calculated_at: string | null;
      }>(
        `SELECT node_id, geom, strength_geoms, antenna_height_m, radius_m, predicted_links,
                calculated_at::text AS calculated_at
         FROM node_coverage
         WHERE node_id = $1
           AND is_planned = TRUE
           AND expires_at > NOW()
         LIMIT 1`,
        [jobId],
      );
      if (result.rows[0]) {
        res.json({
          status: 'ready',
          coverage: toPublicPlannedCoverage(planId, result.rows[0]),
        });
      } else {
        const state = await getPlannedCoverageState(planId);
        if (state === 'queued' || state === 'leased') {
          res.json({ status: 'pending' });
        } else if (state === 'failed') {
          res.status(503).json({ status: 'failed' });
        } else {
          res.status(404).json({ status: 'expired' });
        }
      }
    } catch (err) {
      console.error('[api] GET /coverage/planned/:planId', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  /** Remove a planned repeater's coverage data. */
  router.delete('/coverage/planned/:planId', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'planned coverage disabled' });
      return;
    }

    try {
      const planId = String(req.params['planId'] ?? '').trim();
      if (!PLAN_ID_RE.test(planId)) {
        res.status(400).json({ error: 'invalid plan id' });
        return;
      }
      await releasePlannedCoverage(planId);
      res.status(204).send();
    } catch (err) {
      console.error('[api] DELETE /coverage/planned/:planId', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
