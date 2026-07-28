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
      throw new AnalysisRunAlreadyActiveError(handle.workload, handle.scope, activeRunId);
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
  await query(
    `UPDATE analysis_runs SET
       status = $2,
       checkpoint = $3,
       generation = $4,
       metadata = $5::jsonb,
       heartbeat_at = NOW(),
       completed_at = NOW()
     WHERE run_id = $1`,
    [
      handle.runId,
      input.status,
      input.checkpoint,
      input.generation ?? null,
      JSON.stringify(input.metadata ?? {}),
    ],
  );
  await query(
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
