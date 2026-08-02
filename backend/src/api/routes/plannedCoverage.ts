import { createHash, randomBytes } from 'node:crypto';
import type { Router } from 'express';
import {
  isViewshedEligibleCoordinate,
  isViewshedWorkerHealthy,
  queuePlannedViewshedJob,
} from '../../queue/publisher.js';
import { isViewshedFeatureEnabled } from '../../features.js';
import {
  PlannedCoverageCapacityError,
  plannedCoverageAdmissionDecision,
  type PlannedCoverageRepository,
} from '../../repositories/plannedCoverage.js';

export { plannedCoverageAdmissionDecision };

export type PlannedCoverageRouteDeps = {
  coverageLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  plannedCoverageRepository: PlannedCoverageRepository;
};

const LEGACY_PLAN_ID_RE = /^plan_[0-9a-f]{16}$/;
const PLAN_HANDLE_RE = /^planv2_[0-9a-f]{64}$/;
const PLAN_TTL_MS = Math.min(
  7 * 24 * 60 * 60_000,
  Math.max(60 * 60_000, Number(process.env['PLANNED_COVERAGE_TTL_MS'] ?? 24 * 60 * 60_000) || 24 * 60 * 60_000),
);
const MAX_OUTSTANDING_JOBS = Math.min(
  1_000,
  Math.max(1, Number(process.env['PLANNED_COVERAGE_MAX_OUTSTANDING_JOBS'] ?? 100) || 100),
);
const MAX_OUTSTANDING_HANDLES = Math.min(
  100_000,
  Math.max(1, Number(process.env['PLANNED_COVERAGE_MAX_OUTSTANDING_HANDLES'] ?? 10_000) || 10_000),
);
const MAX_HANDLES_PER_JOB = Math.min(
  10_000,
  Math.max(1, Number(process.env['PLANNED_COVERAGE_MAX_HANDLES_PER_JOB'] ?? 256) || 256),
);

export function plannedCoverageHandleDigest(handle: string): {
  hash: string;
  algorithm: 'sha256' | 'md5';
} | null {
  if (PLAN_HANDLE_RE.test(handle)) {
    return { hash: createHash('sha256').update(handle).digest('hex'), algorithm: 'sha256' };
  }
  if (LEGACY_PLAN_ID_RE.test(handle)) {
    return { hash: createHash('md5').update(handle).digest('hex'), algorithm: 'md5' };
  }
  return null;
}

function locationFingerprint(lat: number, lon: number): string {
  return createHash('sha256')
    .update(`planned-coverage-v2\0${lat.toFixed(5)}\0${lon.toFixed(5)}`)
    .digest('hex');
}

export function registerPlannedCoverageRoutes(router: Router, deps: PlannedCoverageRouteDeps): void {
  const { coverageLimiter, plannedCoverageRepository } = deps;

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
      if (!await isViewshedWorkerHealthy()) {
        res.status(503).json({ error: 'planned coverage worker unavailable', retryable: true });
        return;
      }

      await plannedCoverageRepository.cleanupExpired();
      const handle = `planv2_${randomBytes(32).toString('hex')}`;
      const digest = plannedCoverageHandleDigest(handle)!;
      const fingerprint = locationFingerprint(lat, lon);
      const proposedJobId = `plannedv2_${randomBytes(16).toString('hex')}`;
      const expiresAt = new Date(Date.now() + PLAN_TTL_MS);
      const { jobId, created } = await plannedCoverageRepository.createOrReuse({
        proposedJobId,
        fingerprint,
        lat,
        lon,
        expiresAt,
        handleHash: digest.hash,
        hashAlgorithm: digest.algorithm,
        maxJobs: MAX_OUTSTANDING_JOBS,
        maxHandles: MAX_OUTSTANDING_HANDLES,
        maxHandlesPerJob: MAX_HANDLES_PER_JOB,
      });

      if (created) {
        try {
          await queuePlannedViewshedJob(jobId, lat, lon);
        } catch (error) {
          console.error('[planned-coverage] durable job awaits recovery:', (error as Error).message);
        }
      }
      res.status(202).json({ plan_id: handle, expires_at: expiresAt.toISOString() });
    } catch (error) {
      if (error instanceof PlannedCoverageCapacityError) {
        res.status(429).json({
          error: 'planned coverage capacity reached',
          retryable: true,
          reason: error.reason,
        });
        return;
      }
      console.error('[api] POST /coverage/planned', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/coverage/planned/:planId', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'planned coverage disabled' });
      return;
    }
    try {
      const planId = String(req.params['planId'] ?? '').trim();
      const digest = plannedCoverageHandleDigest(planId);
      if (!digest) {
        res.status(400).json({ error: 'invalid plan id' });
        return;
      }
      const row = await plannedCoverageRepository.findByHandle(
        digest.hash,
        digest.algorithm,
      );
      if (!row) {
        res.status(404).json({ error: 'planned coverage unavailable' });
        return;
      }
      if (row.status === 'ready' && row.geom) {
        res.json({
          status: 'ready',
          expires_at: row.expires_at,
          coverage: {
            node_id: planId,
            geom: row.geom,
            strength_geoms: row.strength_geoms,
            antenna_height_m: row.antenna_height_m,
            radius_m: row.radius_m,
            predicted_links: row.predicted_links,
            calculated_at: row.calculated_at,
          },
        });
        return;
      }
      if (row.status === 'failed') {
        res.json({ status: 'failed', retryable: true, expires_at: row.expires_at });
        return;
      }
      res.json({ status: 'pending', expires_at: row.expires_at });
    } catch (error) {
      console.error('[api] GET /coverage/planned/:planId', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.delete('/coverage/planned/:planId', coverageLimiter, async (req, res) => {
    if (!isViewshedFeatureEnabled()) {
      res.status(404).json({ error: 'planned coverage disabled' });
      return;
    }
    try {
      const planId = String(req.params['planId'] ?? '').trim();
      const digest = plannedCoverageHandleDigest(planId);
      if (!digest) {
        res.status(400).json({ error: 'invalid plan id' });
        return;
      }
      // Capabilities are independent: deleting one handle never cancels or
      // removes another caller's shared computation.
      await plannedCoverageRepository.deleteHandle(digest.hash, digest.algorithm);
      await plannedCoverageRepository.cleanupExpired();
      res.status(204).send();
    } catch (error) {
      console.error('[api] DELETE /coverage/planned/:planId', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
