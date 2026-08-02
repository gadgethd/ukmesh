import { pool } from '../db/index.js';

const APPROVAL = 'clear-orphaned-analysis-runs';
const apply = process.argv.includes('--apply');
const approved = process.argv.includes(`--approve=${APPROVAL}`);

type LegacyRunRow = {
  workload: string;
  scope: string;
  active_run_id: string;
  run_status: string | null;
  started_at: string | null;
};

async function inventory(lock = false): Promise<LegacyRunRow[]> {
  const result = await pool.query<LegacyRunRow>(
    `SELECT state.workload,
            state.scope,
            state.active_run_id,
            run.status AS run_status,
            run.started_at::text
       FROM analysis_workload_state state
       LEFT JOIN analysis_runs run ON run.run_id = state.active_run_id
      WHERE state.active_run_id IS NOT NULL
        AND state.active_lease_token IS NULL
      ORDER BY state.workload, state.scope
      ${lock ? 'FOR UPDATE OF state' : ''}`,
  );
  return result.rows;
}

function report(mode: 'dry-run' | 'apply', rows: LegacyRunRow[]): void {
  console.log(JSON.stringify({
    event: 'analysis_legacy_orphan_inventory',
    mode,
    count: rows.length,
  }));
  for (const row of rows) {
    console.log(JSON.stringify({
      event: 'analysis_legacy_orphan',
      mode,
      workload: row.workload,
      scope: row.scope,
      activeRunId: row.active_run_id,
      runStatus: row.run_status,
      startedAt: row.started_at,
    }));
  }
}

async function main(): Promise<void> {
  if (!apply) {
    report('dry-run', await inventory());
    console.log(
      `[analysis-cleanup] dry run only; apply with --apply --approve=${APPROVAL}`,
    );
    return;
  }
  if (!approved) {
    throw new Error(`OPERATOR_APPROVAL_REQUIRED:--approve=${APPROVAL}`);
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `SELECT pg_advisory_xact_lock(
         hashtext('meshcore-analytics'),
         hashtext('legacy-analysis-run-cleanup')
       )`,
    );
    const candidates = await client.query<LegacyRunRow>(
      `SELECT state.workload,
              state.scope,
              state.active_run_id,
              run.status AS run_status,
              run.started_at::text
         FROM analysis_workload_state state
         LEFT JOIN analysis_runs run ON run.run_id = state.active_run_id
        WHERE state.active_run_id IS NOT NULL
          AND state.active_lease_token IS NULL
        ORDER BY state.workload, state.scope
        FOR UPDATE OF state`,
    );
    report('apply', candidates.rows);
    const runIds = candidates.rows.map((row) => row.active_run_id);
    if (runIds.length > 0) {
      await client.query(
        `UPDATE analysis_runs
            SET status = 'failed',
                completed_at = clock_timestamp(),
                heartbeat_at = clock_timestamp(),
                terminal_reason = 'legacy_orphan_cleanup',
                metadata = metadata || '{"retryable":true,"legacyOrphanCleanup":true}'::jsonb
          WHERE run_id = ANY($1::text[])
            AND status = 'running'`,
        [runIds],
      );
      const cleared = await client.query(
        `UPDATE analysis_workload_state
            SET active_run_id = NULL,
                active_lease_expires_at = NULL,
                active_lease_owner = NULL,
                active_lease_token = NULL,
                active_run_deadline_at = NULL,
                expected_privacy_generation = NULL,
                expected_model_generation = NULL,
                last_status = 'failed',
                last_error = 'legacy orphan cleared before lease enforcement',
                last_terminal_reason = 'legacy_orphan_cleanup',
                updated_at = clock_timestamp()
          WHERE active_run_id = ANY($1::text[])
            AND active_lease_token IS NULL
          RETURNING workload, scope`,
        [runIds],
      );
      if (cleared.rows.length !== candidates.rows.length) {
        throw new Error(
          `ANALYSIS_ORPHAN_CLEANUP_MISMATCH:${cleared.rows.length}/${candidates.rows.length}`,
        );
      }
    }
    const remaining = await client.query<{ count: string }>(
      `SELECT COUNT(*)::text AS count
         FROM analysis_workload_state
        WHERE active_run_id IS NOT NULL
          AND active_lease_token IS NULL`,
    );
    if (Number(remaining.rows[0]?.count ?? -1) !== 0) {
      throw new Error(`ANALYSIS_ORPHAN_CLEANUP_READBACK_FAILED:${remaining.rows[0]?.count}`);
    }
    await client.query('COMMIT');
    console.log(JSON.stringify({
      event: 'analysis_legacy_orphan_cleanup_complete',
      cleared: candidates.rows.length,
      remaining: 0,
    }));
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

main()
  .catch((error: unknown) => {
    console.error(
      '[analysis-cleanup] failed:',
      error instanceof Error ? error.message : String(error),
    );
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
