import { createHash, randomBytes } from 'node:crypto';
import type { Router } from 'express';
import type pg from 'pg';
import {
  isViewshedEligibleCoordinate,
  isViewshedWorkerHealthy,
  queuePlannedViewshedJob,
} from '../../queue/publisher.js';
import { isViewshedFeatureEnabled } from '../../features.js';

export type PlannedCoverageRouteDeps = {
  coverageLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pool: pg.Pool;
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

type PlannedCoverageCapacityReason = 'jobs' | 'handles' | 'job_handles';

class PlannedCoverageCapacityError extends Error {
  constructor(readonly reason: PlannedCoverageCapacityReason) {
    super(`PLANNED_COVERAGE_CAPACITY_${reason.toUpperCase()}`);
  }
}

export function plannedCoverageAdmissionDecision(input: {
  creatingJob: boolean;
  outstandingJobs: number;
  outstandingHandles: number;
  handlesForJob: number;
  maxJobs?: number;
  maxHandles?: number;
  maxHandlesPerJob?: number;
}): PlannedCoverageCapacityReason | null {
  const maxJobs = input.maxJobs ?? MAX_OUTSTANDING_JOBS;
  const maxHandles = input.maxHandles ?? MAX_OUTSTANDING_HANDLES;
  const maxHandlesPerJob = input.maxHandlesPerJob ?? MAX_HANDLES_PER_JOB;
  if (input.creatingJob && input.outstandingJobs >= maxJobs) return 'jobs';
  if (input.outstandingHandles >= maxHandles) return 'handles';
  if (input.handlesForJob >= maxHandlesPerJob) return 'job_handles';
  return null;
}

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

async function cleanupExpiredPlans(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM planned_coverage_handles WHERE expires_at <= NOW()');
    const expired = await client.query<{ job_id: string }>(
      `DELETE FROM planned_coverage_jobs jobs
        WHERE jobs.expires_at <= NOW()
          AND NOT EXISTS (
            SELECT 1 FROM planned_coverage_handles handles
             WHERE handles.job_id = jobs.job_id
          )
      RETURNING job_id`,
    );
    if (expired.rows.length > 0) {
      await client.query(
        `DELETE FROM node_coverage
          WHERE is_planned = TRUE
            AND node_id = ANY($1::text[])`,
        [expired.rows.map((row) => row.job_id)],
      );
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export function registerPlannedCoverageRoutes(router: Router, deps: PlannedCoverageRouteDeps): void {
  const { coverageLimiter, pool } = deps;

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

      await cleanupExpiredPlans(pool);
      const handle = `planv2_${randomBytes(32).toString('hex')}`;
      const digest = plannedCoverageHandleDigest(handle)!;
      const fingerprint = locationFingerprint(lat, lon);
      const proposedJobId = `plannedv2_${randomBytes(16).toString('hex')}`;
      const expiresAt = new Date(Date.now() + PLAN_TTL_MS);
      const client = await pool.connect();
      let jobId = proposedJobId;
      let created = false;
      try {
        await client.query('BEGIN');
        // This lock owns every durable admission decision. The fingerprint
        // lock remains second so all callers acquire locks in one order.
        await client.query(
          'SELECT pg_advisory_xact_lock(hashtext($1))',
          ['planned:global-admission'],
        );
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`planned:${fingerprint}`]);
        const reusable = await client.query<{ job_id: string }>(
          `SELECT job_id
             FROM planned_coverage_jobs
            WHERE fingerprint = $1
              AND expires_at > NOW()
              AND status IN ('queued', 'running', 'ready')
            LIMIT 1`,
          [fingerprint],
        );
        const counts = await client.query<{
          outstanding_jobs: number;
          outstanding_handles: number;
        }>(
          `SELECT
             (
               SELECT COUNT(*)::int
                 FROM planned_coverage_jobs
                WHERE expires_at > NOW()
                  AND status IN ('queued', 'running')
             ) AS outstanding_jobs,
             (
               SELECT COUNT(*)::int
                 FROM planned_coverage_handles
                WHERE expires_at > NOW()
             ) AS outstanding_handles`,
        );
        const reusableJobId = reusable.rows[0]?.job_id;
        const jobHandleCount = reusableJobId
          ? await client.query<{ count: number }>(
            `SELECT COUNT(*)::int AS count
               FROM planned_coverage_handles
              WHERE job_id = $1
                AND expires_at > NOW()`,
            [reusableJobId],
          )
          : { rows: [{ count: 0 }] };
        const capacityReason = plannedCoverageAdmissionDecision({
          creatingJob: !reusableJobId,
          outstandingJobs: Number(counts.rows[0]?.outstanding_jobs ?? 0),
          outstandingHandles: Number(counts.rows[0]?.outstanding_handles ?? 0),
          handlesForJob: Number(jobHandleCount.rows[0]?.count ?? 0),
        });
        if (capacityReason) throw new PlannedCoverageCapacityError(capacityReason);

        if (reusable.rows[0]) {
          jobId = reusable.rows[0].job_id;
          await client.query(
            `UPDATE planned_coverage_jobs
                SET expires_at = GREATEST(expires_at, $2),
                    updated_at = NOW()
              WHERE job_id = $1`,
            [jobId, expiresAt],
          );
          await client.query(
            `UPDATE node_coverage
                SET expires_at = GREATEST(expires_at, $2)
              WHERE node_id = $1 AND is_planned = TRUE`,
            [jobId, expiresAt],
          );
        } else {
          await client.query(
            `INSERT INTO planned_coverage_jobs
               (job_id, fingerprint, lat, lon, status, expires_at)
             VALUES ($1, $2, $3, $4, 'queued', $5)`,
            [jobId, fingerprint, lat, lon, expiresAt],
          );
          created = true;
        }
        await client.query(
          `INSERT INTO planned_coverage_handles
             (handle_hash, hash_alg, job_id, expires_at)
           VALUES ($1, $2, $3, $4)`,
          [digest.hash, digest.algorithm, jobId, expiresAt],
        );
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

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
      const result = await pool.query<{
        status: 'queued' | 'running' | 'ready' | 'failed';
        error: string | null;
        geom: unknown;
        strength_geoms: unknown;
        antenna_height_m: number | null;
        radius_m: number | null;
        predicted_links: unknown;
        calculated_at: string | null;
        expires_at: string;
      }>(
        `SELECT jobs.status, jobs.error, coverage.geom, coverage.strength_geoms,
                coverage.antenna_height_m, coverage.radius_m, coverage.predicted_links,
                coverage.calculated_at::text AS calculated_at,
                handles.expires_at::text AS expires_at
           FROM planned_coverage_handles handles
           JOIN planned_coverage_jobs jobs ON jobs.job_id = handles.job_id
           LEFT JOIN node_coverage coverage
             ON coverage.node_id = jobs.job_id AND coverage.is_planned = TRUE
          WHERE handles.handle_hash = $1
            AND handles.hash_alg = $2
            AND handles.expires_at > NOW()
            AND jobs.expires_at > NOW()
          LIMIT 1`,
        [digest.hash, digest.algorithm],
      );
      const row = result.rows[0];
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
      await pool.query(
        'DELETE FROM planned_coverage_handles WHERE handle_hash = $1 AND hash_alg = $2',
        [digest.hash, digest.algorithm],
      );
      await cleanupExpiredPlans(pool);
      res.status(204).send();
    } catch (error) {
      console.error('[api] DELETE /coverage/planned/:planId', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
