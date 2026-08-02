import { createHash, randomUUID } from 'node:crypto';
import { pool, query } from '../db/index.js';
import type { BoundedRunStatus } from './boundedRun.js';

export type AnalysisRunHandle = {
  runId: string;
  workload: string;
  scope: string;
  windowStart: Date;
  windowEnd: Date;
  totalItems: number;
};

export class AnalysisRunAlreadyActiveError extends Error {
  constructor(
    readonly workload: string,
    readonly scope: string,
    readonly activeRunId: string,
  ) {
    super(`analysis run already active for ${workload}/${scope}: ${activeRunId}`);
  }
}

const ANALYSIS_RUN_STALE_AFTER_MS = Math.min(
  60 * 60_000,
  Math.max(60_000, Number(process.env['ANALYSIS_RUN_STALE_AFTER_MS'] ?? 5 * 60_000) || 5 * 60_000),
);

export function analysisRunHeartbeatIsStale(
  heartbeatAt: string | Date | null | undefined,
  nowMs = Date.now(),
  staleAfterMs = ANALYSIS_RUN_STALE_AFTER_MS,
): boolean {
  const heartbeatMs = heartbeatAt == null ? Number.NaN : new Date(heartbeatAt).getTime();
  return !Number.isFinite(heartbeatMs) || nowMs - heartbeatMs > staleAfterMs;
}

export async function beginAnalysisRun(input: Omit<AnalysisRunHandle, 'runId'>): Promise<AnalysisRunHandle> {
  const handle = { ...input, runId: randomUUID() };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO analysis_workload_state (workload, scope)
       VALUES ($1, $2)
       ON CONFLICT (workload, scope) DO NOTHING`,
      [handle.workload, handle.scope],
    );
    const state = await client.query<{ active_run_id: string | null }>(
      `SELECT active_run_id
         FROM analysis_workload_state
        WHERE workload = $1 AND scope = $2
        FOR UPDATE NOWAIT`,
      [handle.workload, handle.scope],
    );
    const activeRunId = state.rows[0]?.active_run_id;
    if (activeRunId) {
      const activeRun = await client.query<{ status: string; heartbeat_at: Date }>(
        `SELECT status, heartbeat_at
           FROM analysis_runs
          WHERE run_id = $1
          FOR UPDATE`,
        [activeRunId],
      );
      const row = activeRun.rows[0];
      const stale = !row
        || row.status !== 'running'
        || analysisRunHeartbeatIsStale(row.heartbeat_at);
      if (!stale) {
        throw new AnalysisRunAlreadyActiveError(handle.workload, handle.scope, activeRunId);
      }
      if (row?.status === 'running') {
        await client.query(
          `UPDATE analysis_runs SET
             status = 'failed',
             heartbeat_at = NOW(),
             completed_at = NOW(),
             metadata = metadata || jsonb_build_object('recoveredAsStale', TRUE)
           WHERE run_id = $1 AND status = 'running'`,
          [activeRunId],
        );
      }
      await client.query(
        `UPDATE analysis_workload_state SET
           active_run_id = NULL,
           last_status = 'failed',
           last_error = 'stale analysis run recovered after heartbeat expiry',
           updated_at = NOW()
         WHERE workload = $1 AND scope = $2 AND active_run_id = $3`,
        [handle.workload, handle.scope, activeRunId],
      );
    }
    await client.query(
      `INSERT INTO analysis_runs (
         run_id, workload, scope, status, window_start, window_end, total_items
       ) VALUES ($1, $2, $3, 'running', $4, $5, $6)`,
      [
        handle.runId,
        handle.workload,
        handle.scope,
        handle.windowStart,
        handle.windowEnd,
        handle.totalItems,
      ],
    );
    await client.query(
      `UPDATE analysis_workload_state SET
         active_run_id = $3,
         last_status = 'running',
         last_error = NULL,
         updated_at = NOW()
       WHERE workload = $1 AND scope = $2`,
      [handle.workload, handle.scope, handle.runId],
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
  return handle;
}

export async function heartbeatAnalysisRun(handle: AnalysisRunHandle): Promise<boolean> {
  const result = await query(
    `WITH heartbeat AS (
       UPDATE analysis_runs SET heartbeat_at = NOW()
        WHERE run_id = $1 AND status = 'running'
        RETURNING run_id
     )
     UPDATE analysis_workload_state state SET updated_at = NOW()
       FROM heartbeat
      WHERE state.workload = $2
        AND state.scope = $3
        AND state.active_run_id = heartbeat.run_id
     RETURNING state.active_run_id`,
    [handle.runId, handle.workload, handle.scope],
  );
  return (result.rowCount ?? 0) === 1;
}

export function startAnalysisRunHeartbeat(
  handle: AnalysisRunHandle,
  intervalMs = 30_000,
): {
  ownsLease: () => boolean;
  stop: () => Promise<void>;
} {
  let ownsLease = true;
  let stopped = false;
  let inFlight: Promise<void> | null = null;
  const beat = () => {
    if (stopped || inFlight) return;
    inFlight = heartbeatAnalysisRun(handle)
      .then((updated) => {
        if (!updated) ownsLease = false;
      })
      .catch((error: unknown) => {
        console.warn(
          `[analysis-run] heartbeat failed for ${handle.workload}/${handle.scope}`,
          (error as Error).message,
        );
      })
      .finally(() => {
        inFlight = null;
      });
  };
  const timer = setInterval(beat, intervalMs);
  timer.unref();
  return {
    ownsLease: () => ownsLease,
    stop: async () => {
      stopped = true;
      clearInterval(timer);
      if (inFlight) await inFlight;
    },
  };
}

export function analysisGeneration(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

export async function finishAnalysisRun(
  handle: AnalysisRunHandle,
  input: {
    status: Exclude<BoundedRunStatus, 'stale'> | 'stale';
    checkpoint: number;
    generation?: string;
    error?: string;
    metadata?: unknown;
  },
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const finished = await client.query(
      `UPDATE analysis_runs SET
         status = $2,
         checkpoint = $3,
         generation = $4,
         metadata = $5::jsonb,
         heartbeat_at = NOW(),
         completed_at = NOW()
       WHERE run_id = $1 AND status = 'running'
       RETURNING run_id`,
      [
        handle.runId,
        input.status,
        input.checkpoint,
        input.generation ?? null,
        JSON.stringify(input.metadata ?? {}),
      ],
    );
    if ((finished.rowCount ?? 0) !== 1) throw new Error('ANALYSIS_RUN_NOT_ACTIVE');
    const stateFinished = await client.query(
      `UPDATE analysis_workload_state SET
         active_run_id = NULL,
         last_complete_generation = CASE WHEN $4 = 'complete' THEN $5 ELSE last_complete_generation END,
         last_complete_at = CASE WHEN $4 = 'complete' THEN NOW() ELSE last_complete_at END,
         last_status = $4,
         last_error = $6,
         updated_at = NOW()
       WHERE workload = $1 AND scope = $2 AND active_run_id = $3`,
      [
        handle.workload,
        handle.scope,
        handle.runId,
        input.status,
        input.generation ?? null,
        input.error ?? null,
      ],
    );
    if ((stateFinished.rowCount ?? 0) !== 1) throw new Error('ANALYSIS_RUN_LEASE_LOST');
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function getAnalysisWorkloadStates(): Promise<Array<{
  workload: string;
  scope: string;
  lastStatus: string | null;
  lastCompleteAt: string | null;
  activeRunId: string | null;
  lastError: string | null;
}>> {
  const result = await query<{
    workload: string;
    scope: string;
    last_status: string | null;
    last_complete_at: string | null;
    active_run_id: string | null;
    last_error: string | null;
  }>(
    `SELECT workload, scope, last_status, last_complete_at, active_run_id, last_error
       FROM analysis_workload_state
      ORDER BY workload, scope`,
  );
  return result.rows.map((row) => ({
    workload: row.workload,
    scope: row.scope,
    lastStatus: row.last_status,
    lastCompleteAt: row.last_complete_at,
    activeRunId: row.active_run_id,
    lastError: row.last_error,
  }));
}
