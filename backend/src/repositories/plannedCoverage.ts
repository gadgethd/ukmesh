import type pg from 'pg';

export type PlannedCoverageCapacityReason = 'jobs' | 'handles' | 'job_handles';

export class PlannedCoverageCapacityError extends Error {
  constructor(readonly reason: PlannedCoverageCapacityReason) {
    super(`PLANNED_COVERAGE_CAPACITY_${reason.toUpperCase()}`);
  }
}

export function plannedCoverageAdmissionDecision(input: {
  creatingJob: boolean;
  outstandingJobs: number;
  outstandingHandles: number;
  handlesForJob: number;
  maxJobs: number;
  maxHandles: number;
  maxHandlesPerJob: number;
}): PlannedCoverageCapacityReason | null {
  if (input.creatingJob && input.outstandingJobs >= input.maxJobs) return 'jobs';
  if (input.outstandingHandles >= input.maxHandles) return 'handles';
  if (input.handlesForJob >= input.maxHandlesPerJob) return 'job_handles';
  return null;
}

export type PlannedCoverageResultRow = {
  status: 'queued' | 'running' | 'ready' | 'failed';
  error: string | null;
  geom: unknown;
  strength_geoms: unknown;
  antenna_height_m: number | null;
  radius_m: number | null;
  predicted_links: unknown;
  calculated_at: string | null;
  expires_at: string;
};

export type PlannedCoverageRepository = {
  cleanupExpired: () => Promise<void>;
  createOrReuse: (input: {
    proposedJobId: string;
    fingerprint: string;
    lat: number;
    lon: number;
    expiresAt: Date;
    handleHash: string;
    hashAlgorithm: 'sha256' | 'md5';
    maxJobs: number;
    maxHandles: number;
    maxHandlesPerJob: number;
  }) => Promise<{ jobId: string; created: boolean }>;
  findByHandle: (
    handleHash: string,
    hashAlgorithm: 'sha256' | 'md5',
  ) => Promise<PlannedCoverageResultRow | null>;
  deleteHandle: (
    handleHash: string,
    hashAlgorithm: 'sha256' | 'md5',
  ) => Promise<void>;
};

export function createPlannedCoverageRepository(pool: pg.Pool): PlannedCoverageRepository {
  const cleanupExpired = async (): Promise<void> => {
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
  };

  return {
    cleanupExpired,

    async createOrReuse(input) {
      const client = await pool.connect();
      let jobId = input.proposedJobId;
      let created = false;
      try {
        await client.query('BEGIN');
        // Lock every admission decision in one global order. The fingerprint
        // lock then prevents duplicate work for the same coordinate.
        await client.query(
          'SELECT pg_advisory_xact_lock(hashtext($1))',
          ['planned:global-admission'],
        );
        await client.query(
          'SELECT pg_advisory_xact_lock(hashtext($1))',
          [`planned:${input.fingerprint}`],
        );
        const reusable = await client.query<{ job_id: string }>(
          `SELECT job_id
             FROM planned_coverage_jobs
            WHERE fingerprint = $1
              AND expires_at > NOW()
              AND status IN ('queued', 'running', 'ready')
            LIMIT 1`,
          [input.fingerprint],
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
        const handlesForJob = reusableJobId
          ? Number((await client.query<{ count: number }>(
            `SELECT COUNT(*)::int AS count
               FROM planned_coverage_handles
              WHERE job_id = $1
                AND expires_at > NOW()`,
            [reusableJobId],
          )).rows[0]?.count ?? 0)
          : 0;
        const capacityReason = plannedCoverageAdmissionDecision({
          creatingJob: !reusableJobId,
          outstandingJobs: Number(counts.rows[0]?.outstanding_jobs ?? 0),
          outstandingHandles: Number(counts.rows[0]?.outstanding_handles ?? 0),
          handlesForJob,
          maxJobs: input.maxJobs,
          maxHandles: input.maxHandles,
          maxHandlesPerJob: input.maxHandlesPerJob,
        });
        if (capacityReason) throw new PlannedCoverageCapacityError(capacityReason);

        if (reusableJobId) {
          jobId = reusableJobId;
          await client.query(
            `UPDATE planned_coverage_jobs
                SET expires_at = GREATEST(expires_at, $2),
                    updated_at = NOW()
              WHERE job_id = $1`,
            [jobId, input.expiresAt],
          );
          await client.query(
            `UPDATE node_coverage
                SET expires_at = GREATEST(expires_at, $2)
              WHERE node_id = $1 AND is_planned = TRUE`,
            [jobId, input.expiresAt],
          );
        } else {
          await client.query(
            `INSERT INTO planned_coverage_jobs
               (job_id, fingerprint, lat, lon, status, expires_at)
             VALUES ($1, $2, $3, $4, 'queued', $5)`,
            [jobId, input.fingerprint, input.lat, input.lon, input.expiresAt],
          );
          created = true;
        }
        await client.query(
          `INSERT INTO planned_coverage_handles
             (handle_hash, hash_alg, job_id, expires_at)
           VALUES ($1, $2, $3, $4)`,
          [input.handleHash, input.hashAlgorithm, jobId, input.expiresAt],
        );
        await client.query('COMMIT');
        return { jobId, created };
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    },

    async findByHandle(handleHash, hashAlgorithm) {
      const result = await pool.query<PlannedCoverageResultRow>(
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
        [handleHash, hashAlgorithm],
      );
      return result.rows[0] ?? null;
    },

    async deleteHandle(handleHash, hashAlgorithm) {
      await pool.query(
        'DELETE FROM planned_coverage_handles WHERE handle_hash = $1 AND hash_alg = $2',
        [handleHash, hashAlgorithm],
      );
    },
  };
}
