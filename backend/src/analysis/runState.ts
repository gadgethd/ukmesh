import { createHash, randomUUID } from 'node:crypto';
import os from 'node:os';
import { pool, query } from '../db/index.js';
import type { BoundedRunStatus } from './boundedRun.js';
import { assertAnalysisPublicationLease } from './publicationFence.js';
import {
  analysisActiveLeases,
  analysisLeaseEventsTotal,
  boundedAnalysisWorkloadLabel,
} from '../metrics.js';

const ANALYSIS_RUN_LEASE_MS = Math.max(
  30_000,
  Math.min(24 * 60 * 60_000, Number(process.env['ANALYSIS_RUN_LEASE_MS'] ?? 5 * 60_000) || 5 * 60_000),
);
const ANALYSIS_RUN_DEFAULT_DEADLINE_MS = Math.max(
  ANALYSIS_RUN_LEASE_MS,
  Math.min(
    24 * 60 * 60_000,
    Number(process.env['ANALYSIS_RUN_DEADLINE_MS'] ?? 2 * 60 * 60_000)
      || 2 * 60 * 60_000,
  ),
);
const ANALYSIS_RUN_OWNER = String(
  process.env['ANALYSIS_RUN_OWNER']
    ?? `${os.hostname()}:${process.pid}`,
).slice(0, 200);
const metricActiveRuns = new Map<string, Set<string>>();

function trackMetricLease(workload: string, runId: string): void {
  const label = boundedAnalysisWorkloadLabel(workload);
  const runs = metricActiveRuns.get(label) ?? new Set<string>();
  runs.add(runId);
  metricActiveRuns.set(label, runs);
  analysisActiveLeases.set({ workload: label }, runs.size);
}

function untrackMetricLease(workload: string, runId: string): void {
  const label = boundedAnalysisWorkloadLabel(workload);
  const runs = metricActiveRuns.get(label) ?? new Set<string>();
  runs.delete(runId);
  metricActiveRuns.set(label, runs);
  analysisActiveLeases.set({ workload: label }, runs.size);
}

export type AnalysisRunHandle = {
  runId: string;
  workload: string;
  scope: string;
  windowStart: Date;
  windowEnd: Date;
  totalItems: number;
  leaseOwner: string;
  leaseToken: string;
  attempt: number;
  deadlineAt: Date;
  privacyGeneration?: number;
  modelGeneration?: string;
};

export type BeginAnalysisRunInput = Omit<
  AnalysisRunHandle,
  'runId' | 'leaseOwner' | 'leaseToken' | 'attempt' | 'deadlineAt'
> & {
  deadlineMs?: number;
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

export class AnalysisRunLeaseLostError extends Error {
  constructor(readonly runId: string) {
    super(`analysis run lease lost: ${runId}`);
  }
}

export class AnalysisRunDeadlineExceededError extends Error {
  constructor(readonly runId: string) {
    super(`analysis run deadline exceeded: ${runId}`);
  }
}

export class AnalysisLegacyRunRequiresCleanupError extends Error {
  constructor(readonly workload: string, readonly scope: string) {
    super(`legacy active analysis run requires audited cleanup: ${workload}/${scope}`);
  }
}

export async function beginAnalysisRun(
  input: BeginAnalysisRunInput,
): Promise<AnalysisRunHandle> {
  const runId = randomUUID();
  const leaseToken = randomUUID();
  const deadlineMs = Math.max(
    1_000,
    Math.min(
      24 * 60 * 60_000,
      Number(input.deadlineMs ?? ANALYSIS_RUN_DEFAULT_DEADLINE_MS)
        || ANALYSIS_RUN_DEFAULT_DEADLINE_MS,
    ),
  );
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO analysis_workload_state (workload, scope)
       VALUES ($1, $2)
       ON CONFLICT (workload, scope) DO NOTHING`,
      [input.workload, input.scope],
    );
    const state = await client.query<{
      active_run_id: string | null;
      active_lease_token: string | null;
      active_attempt: number;
      active_valid: boolean;
    }>(
      `SELECT active_run_id, active_lease_token, active_attempt,
              (
                active_lease_expires_at > clock_timestamp()
                AND active_run_deadline_at > clock_timestamp()
              ) AS active_valid
         FROM analysis_workload_state
        WHERE workload = $1 AND scope = $2
        FOR UPDATE`,
      [input.workload, input.scope],
    );
    const active = state.rows[0];
    if (active?.active_run_id && !active.active_lease_token) {
      throw new AnalysisLegacyRunRequiresCleanupError(input.workload, input.scope);
    }
    if (active?.active_run_id && active.active_valid) {
      throw new AnalysisRunAlreadyActiveError(input.workload, input.scope, active.active_run_id);
    }
    if (active?.active_run_id) {
      await client.query(
        `UPDATE analysis_runs
            SET status = 'failed',
                completed_at = clock_timestamp(),
                heartbeat_at = clock_timestamp(),
                terminal_reason = 'lease_expired_reclaimed',
                metadata = metadata || '{"leaseExpired":true,"retryable":true}'::jsonb
          WHERE run_id = $1 AND status = 'running'`,
        [active.active_run_id],
      );
    }
    const attempt = Math.max(1, Number(active?.active_attempt ?? 0) + 1);
    const inserted = await client.query<{ run_deadline_at: Date }>(
      `INSERT INTO analysis_runs (
         run_id, workload, scope, status, window_start, window_end, total_items,
         lease_owner, lease_token, lease_expires_at, run_deadline_at, attempt,
         privacy_generation, model_generation
       ) VALUES (
         $1, $2, $3, 'running', $4, $5, $6,
         $7, $8,
         clock_timestamp() + ($9::text || ' milliseconds')::interval,
         clock_timestamp() + ($10::text || ' milliseconds')::interval,
         $11, $12, $13
       )
       RETURNING run_deadline_at`,
      [
        runId,
        input.workload,
        input.scope,
        input.windowStart,
        input.windowEnd,
        input.totalItems,
        ANALYSIS_RUN_OWNER,
        leaseToken,
        String(ANALYSIS_RUN_LEASE_MS),
        String(deadlineMs),
        attempt,
        input.privacyGeneration ?? null,
        input.modelGeneration ?? null,
      ],
    );
    await client.query(
      `UPDATE analysis_workload_state SET
         active_run_id = $3,
         active_lease_expires_at = NOW() + ($4::text || ' milliseconds')::interval,
         active_lease_owner = $5,
         active_lease_token = $6,
         active_run_deadline_at = clock_timestamp() + ($7::text || ' milliseconds')::interval,
         active_attempt = $8,
         expected_privacy_generation = $9,
         expected_model_generation = $10,
         last_status = 'running',
         last_error = NULL,
         last_terminal_reason = NULL,
         updated_at = NOW()
       WHERE workload = $1 AND scope = $2`,
      [
        input.workload,
        input.scope,
        runId,
        String(ANALYSIS_RUN_LEASE_MS),
        ANALYSIS_RUN_OWNER,
        leaseToken,
        String(deadlineMs),
        attempt,
        input.privacyGeneration ?? null,
        input.modelGeneration ?? null,
      ],
    );
    await client.query('COMMIT');
    if (active?.active_run_id) untrackMetricLease(input.workload, active.active_run_id);
    trackMetricLease(input.workload, runId);
    analysisLeaseEventsTotal.inc({
      workload: boundedAnalysisWorkloadLabel(input.workload),
      outcome: active?.active_run_id ? 'reclaimed' : 'acquired',
    });
    return {
      workload: input.workload,
      scope: input.scope,
      windowStart: input.windowStart,
      windowEnd: input.windowEnd,
      totalItems: input.totalItems,
      ...(input.privacyGeneration === undefined
        ? {}
        : { privacyGeneration: input.privacyGeneration }),
      ...(input.modelGeneration === undefined
        ? {}
        : { modelGeneration: input.modelGeneration }),
      runId,
      leaseOwner: ANALYSIS_RUN_OWNER,
      leaseToken,
      attempt,
      deadlineAt: inserted.rows[0]!.run_deadline_at,
    };
  } catch (error) {
    await client.query('ROLLBACK');
    analysisLeaseEventsTotal.inc({
      workload: boundedAnalysisWorkloadLabel(input.workload),
      outcome: error instanceof AnalysisRunAlreadyActiveError
        ? 'already_active'
        : error instanceof AnalysisLegacyRunRequiresCleanupError
          ? 'legacy_blocked'
          : 'failure',
    });
    throw error;
  } finally {
    client.release();
  }
}

export function analysisGeneration(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

export async function heartbeatAnalysisRun(handle: AnalysisRunHandle): Promise<void> {
  const result = await query(
    `WITH owned AS (
       UPDATE analysis_workload_state
          SET active_lease_expires_at = NOW() + ($4::text || ' milliseconds')::interval,
              updated_at = NOW()
        WHERE workload = $1
          AND scope = $2
          AND active_run_id = $3
          AND active_lease_token = $5
          AND active_lease_expires_at > clock_timestamp()
          AND active_run_deadline_at > clock_timestamp()
        RETURNING active_run_id
     )
     UPDATE analysis_runs
        SET heartbeat_at = clock_timestamp(),
            lease_expires_at = clock_timestamp() + ($4::text || ' milliseconds')::interval
      WHERE run_id = $3
        AND lease_token = $5
        AND status = 'running'
        AND EXISTS (SELECT 1 FROM owned)
      RETURNING run_id`,
    [
      handle.workload,
      handle.scope,
      handle.runId,
      String(ANALYSIS_RUN_LEASE_MS),
      handle.leaseToken,
    ],
  );
  if (result.rows.length !== 1) {
    analysisLeaseEventsTotal.inc({
      workload: boundedAnalysisWorkloadLabel(handle.workload),
      outcome: 'heartbeat_lost',
    });
    untrackMetricLease(handle.workload, handle.runId);
    throw new AnalysisRunLeaseLostError(handle.runId);
  }
  analysisLeaseEventsTotal.inc({
    workload: boundedAnalysisWorkloadLabel(handle.workload),
    outcome: 'heartbeat',
  });
}

export async function updateAnalysisRunTotalItems(
  handle: AnalysisRunHandle,
  totalItems: number,
  signal?: AbortSignal,
): Promise<void> {
  const result = await query(
    `UPDATE analysis_runs run
        SET total_items = $4
       FROM analysis_workload_state state
      WHERE state.workload = $1
        AND state.scope = $2
        AND state.active_run_id = run.run_id
        AND state.active_run_id = $3
        AND state.active_lease_token = $5
        AND state.active_lease_expires_at > clock_timestamp()
        AND state.active_run_deadline_at > clock_timestamp()
        AND run.lease_token = $5
        AND run.status = 'running'
      RETURNING run.run_id`,
    [
      handle.workload,
      handle.scope,
      handle.runId,
      Math.max(0, Math.trunc(totalItems)),
      handle.leaseToken,
    ],
    signal,
  );
  if (result.rows.length !== 1) throw new AnalysisRunLeaseLostError(handle.runId);
}

export type AnalysisRunHeartbeat = (() => Promise<void>) & {
  signal: AbortSignal;
  assertOwned: () => void;
  stopForTerminal: () => Promise<'owned' | 'deadline'>;
};

export function startAnalysisRunHeartbeat(handle: AnalysisRunHandle): AnalysisRunHeartbeat {
  let inFlight: Promise<void> | null = null;
  let lost: unknown;
  const abortController = new AbortController();
  const markLost = (error: unknown) => {
    lost = error;
    abortController.abort(error);
  };
  const beat = () => {
    if (inFlight || lost) return;
    inFlight = heartbeatAnalysisRun(handle)
      .catch(markLost)
      .finally(() => { inFlight = null; });
  };
  const timer = setInterval(beat, Math.max(10_000, Math.floor(ANALYSIS_RUN_LEASE_MS / 3)));
  timer.unref();
  const deadlineTimer = setTimeout(() => {
    markLost(new AnalysisRunDeadlineExceededError(handle.runId));
  }, Math.max(1, handle.deadlineAt.getTime() - Date.now()));
  deadlineTimer.unref();
  const stop = async () => {
    clearInterval(timer);
    clearTimeout(deadlineTimer);
    await inFlight;
    if (lost) throw lost;
  };
  const assertOwned = () => {
    if (lost || abortController.signal.aborted || Date.now() >= handle.deadlineAt.getTime()) {
      throw lost instanceof Error ? lost : new AnalysisRunLeaseLostError(handle.runId);
    }
  };
  const stopForTerminal = async (): Promise<'owned' | 'deadline'> => {
    clearInterval(timer);
    clearTimeout(deadlineTimer);
    await inFlight;
    if (lost instanceof AnalysisRunDeadlineExceededError) return 'deadline';
    if (lost) throw lost;
    return Date.now() >= handle.deadlineAt.getTime() ? 'deadline' : 'owned';
  };
  return Object.assign(stop, {
    signal: abortController.signal,
    assertOwned,
    stopForTerminal,
  });
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
    const state = await client.query(
      `SELECT active_run_id, active_lease_token
         FROM analysis_workload_state
        WHERE workload = $1 AND scope = $2
        FOR UPDATE`,
      [handle.workload, handle.scope],
    );
    if (
      state.rows[0]?.active_run_id !== handle.runId
      || state.rows[0]?.active_lease_token !== handle.leaseToken
    ) {
      throw new AnalysisRunLeaseLostError(handle.runId);
    }
    if (input.status === 'complete') {
      await assertAnalysisPublicationLease(client, handle);
    }
    const run = await client.query(
      `UPDATE analysis_runs SET
         status = $2,
         checkpoint = $3,
         generation = $4,
         metadata = $5::jsonb,
         heartbeat_at = clock_timestamp(),
         completed_at = clock_timestamp(),
         terminal_reason = $6
       WHERE run_id = $1
         AND lease_token = $7
         AND status = 'running'
       RETURNING run_id`,
      [
        handle.runId,
        input.status,
        input.checkpoint,
        input.generation ?? null,
        JSON.stringify(input.metadata ?? {}),
        input.error ?? input.status,
        handle.leaseToken,
      ],
    );
    if (run.rows.length !== 1) throw new AnalysisRunLeaseLostError(handle.runId);
    const cleared = await client.query(
      `UPDATE analysis_workload_state SET
         active_run_id = NULL,
         active_lease_expires_at = NULL,
         active_lease_owner = NULL,
         active_lease_token = NULL,
         active_run_deadline_at = NULL,
         expected_privacy_generation = NULL,
         expected_model_generation = NULL,
         last_complete_generation = CASE WHEN $4 = 'complete' THEN $5 ELSE last_complete_generation END,
         last_complete_at = CASE WHEN $4 = 'complete' THEN NOW() ELSE last_complete_at END,
         last_status = $4,
         last_error = $6,
         last_terminal_reason = $7,
         updated_at = NOW()
       WHERE workload = $1
         AND scope = $2
         AND active_run_id = $3
         AND active_lease_token = $8
       RETURNING workload`,
      [
        handle.workload,
        handle.scope,
        handle.runId,
        input.status,
        input.generation ?? null,
        input.error ?? null,
        input.error ?? input.status,
        handle.leaseToken,
      ],
    );
    if (cleared.rows.length !== 1) throw new AnalysisRunLeaseLostError(handle.runId);
    await client.query('COMMIT');
    untrackMetricLease(handle.workload, handle.runId);
    analysisLeaseEventsTotal.inc({
      workload: boundedAnalysisWorkloadLabel(handle.workload),
      outcome: input.status,
    });
  } catch (error) {
    await client.query('ROLLBACK');
    analysisLeaseEventsTotal.inc({
      workload: boundedAnalysisWorkloadLabel(handle.workload),
      outcome: error instanceof AnalysisRunLeaseLostError ? 'finish_lease_lost' : 'finish_failure',
    });
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
  activeLeaseOwner: string | null;
  activeLeaseExpiresAt: string | null;
  activeRunDeadlineAt: string | null;
  activeAttempt: number;
  lastTerminalReason: string | null;
  lastError: string | null;
}>> {
  const result = await query<{
    workload: string;
    scope: string;
    last_status: string | null;
    last_complete_at: string | null;
    active_run_id: string | null;
    active_lease_owner: string | null;
    active_lease_expires_at: string | null;
    active_run_deadline_at: string | null;
    active_attempt: number;
    last_terminal_reason: string | null;
    last_error: string | null;
  }>(
    `SELECT workload, scope, last_status, last_complete_at, active_run_id,
            active_lease_owner, active_lease_expires_at::text,
            active_run_deadline_at::text, active_attempt,
            last_terminal_reason, last_error
       FROM analysis_workload_state
      ORDER BY workload, scope`,
  );
  return result.rows.map((row) => ({
    workload: row.workload,
    scope: row.scope,
    lastStatus: row.last_status,
    lastCompleteAt: row.last_complete_at,
    activeRunId: row.active_run_id,
    activeLeaseOwner: row.active_lease_owner,
    activeLeaseExpiresAt: row.active_lease_expires_at,
    activeRunDeadlineAt: row.active_run_deadline_at,
    activeAttempt: row.active_attempt,
    lastTerminalReason: row.last_terminal_reason,
    lastError: row.last_error,
  }));
}
