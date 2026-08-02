import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';

const databaseUrl = String(process.env['TEST_ANALYSIS_DATABASE_URL'] ?? '').trim();
const schema = 'analysis_lease_integration';

if (databaseUrl) {
  process.env['DATABASE_URL'] = databaseUrl;
  process.env['DATABASE_SCHEMA'] = schema;
  process.env['DATABASE_POOL_MAX'] = '8';
  process.env['ANALYSIS_RUN_LEASE_MS'] = '30000';
  process.env['ANALYSIS_RUN_DEADLINE_MS'] = '60000';
}

test('analysis leases fence publication, reclaim once, and recover transaction faults', {
  skip: databaseUrl ? false : 'TEST_ANALYSIS_DATABASE_URL is not configured',
  timeout: 30_000,
}, async () => {
  const admin = new pg.Client({ connectionString: databaseUrl });
  await admin.connect();
  await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
  await admin.query(`CREATE SCHEMA ${schema}`);
  await admin.query(`SET search_path TO ${schema}, public`);
  await admin.query(`
    CREATE TABLE public_visibility_state (
      singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
      generation BIGINT NOT NULL
    );
    INSERT INTO public_visibility_state (singleton, generation) VALUES (TRUE, 1);
  `);
  const { readFile } = await import('node:fs/promises');
  const { fileURLToPath } = await import('node:url');
  const migrationDir = fileURLToPath(new URL('../db/migrations/', import.meta.url));
  for (const name of ['012_analysis_run_protocol.sql', '024_analysis_run_leases.sql']) {
    await admin.query(await readFile(`${migrationDir}${name}`, 'utf8'));
  }
  await admin.query(`
    CREATE TABLE canonical_analysis_result (
      workload TEXT NOT NULL,
      scope TEXT NOT NULL,
      run_id TEXT NOT NULL,
      generation TEXT NOT NULL,
      PRIMARY KEY (workload, scope, generation)
    )
  `);

  const runState = await import('./runState.js');
  const { assertAnalysisPublicationLease } = await import('./publicationFence.js');
  const { pool, query } = await import('../db/index.js');

  const input = (scope: string) => ({
    workload: 'integration-analysis',
    scope,
    windowStart: new Date('2026-01-01T00:00:00Z'),
    windowEnd: new Date('2026-01-01T01:00:00Z'),
    totalItems: 10,
    deadlineMs: 60_000,
    privacyGeneration: 1,
    modelGeneration: 'integration-v1',
  });

  try {
    const claims = await Promise.allSettled([
      runState.beginAnalysisRun(input('reclaim')),
      runState.beginAnalysisRun(input('reclaim')),
    ]);
    const fulfilledClaims = claims.filter((claim) => claim.status === 'fulfilled');
    assert.equal(fulfilledClaims.length, 1);
    const first = fulfilledClaims[0]!.value;
    assert.ok(
      claims.some(
        (claim) => claim.status === 'rejected'
          && claim.reason instanceof runState.AnalysisRunAlreadyActiveError,
      ),
    );

    await admin.query(
      `UPDATE ${schema}.analysis_workload_state
          SET active_lease_expires_at = clock_timestamp() - INTERVAL '1 second'
        WHERE workload = $1 AND scope = $2`,
      [first.workload, first.scope],
    );
    await admin.query(
      `UPDATE ${schema}.analysis_runs
          SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
        WHERE run_id = $1`,
      [first.runId],
    );
    const reclaims = await Promise.allSettled([
      runState.beginAnalysisRun(input('reclaim')),
      runState.beginAnalysisRun(input('reclaim')),
    ]);
    const fulfilledReclaims = reclaims.filter((claim) => claim.status === 'fulfilled');
    assert.equal(fulfilledReclaims.length, 1);
    const reclaimed = fulfilledReclaims[0]!.value;
    assert.equal(reclaimed.attempt, 2);

    const oldClient = await pool.connect();
    try {
      await oldClient.query('BEGIN');
      await assert.rejects(
        assertAnalysisPublicationLease(oldClient, first),
        /publication fence rejected/,
      );
      await oldClient.query('ROLLBACK');
    } finally {
      oldClient.release();
    }

    const publishClient = await pool.connect();
    try {
      await publishClient.query('BEGIN');
      await assertAnalysisPublicationLease(publishClient, reclaimed);
      await publishClient.query(
        `INSERT INTO canonical_analysis_result
           (workload, scope, run_id, generation)
         VALUES ($1, $2, $3, $4)`,
        [reclaimed.workload, reclaimed.scope, reclaimed.runId, 'generation-1'],
      );
      await assertAnalysisPublicationLease(publishClient, reclaimed);
      await publishClient.query('COMMIT');
    } finally {
      publishClient.release();
    }
    await runState.finishAnalysisRun(reclaimed, {
      status: 'complete',
      checkpoint: 10,
      generation: 'generation-1',
    });
    const reclaimReadback = await admin.query<{
      active_run_id: string | null;
      status: string;
      terminal_reason: string;
    }>(
      `SELECT state.active_run_id, run.status, run.terminal_reason
         FROM ${schema}.analysis_workload_state state
         JOIN ${schema}.analysis_runs run ON run.run_id = $1
        WHERE state.workload = $2 AND state.scope = $3`,
      [first.runId, first.workload, first.scope],
    );
    assert.equal(reclaimReadback.rows[0]?.active_run_id, null);
    assert.equal(reclaimReadback.rows[0]?.status, 'failed');
    assert.equal(reclaimReadback.rows[0]?.terminal_reason, 'lease_expired_reclaimed');

    const boundaryRun = await runState.beginAnalysisRun(input('boundary'));
    await admin.query(`
      CREATE FUNCTION ${schema}.reject_analysis_clear() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.active_run_id IS NOT NULL AND NEW.active_run_id IS NULL THEN
          RAISE EXCEPTION 'injected clear failure';
        END IF;
        RETURN NEW;
      END
      $$;
      CREATE TRIGGER reject_analysis_clear
      BEFORE UPDATE ON ${schema}.analysis_workload_state
      FOR EACH ROW EXECUTE FUNCTION ${schema}.reject_analysis_clear();
    `);
    await assert.rejects(
      runState.finishAnalysisRun(boundaryRun, {
        status: 'complete',
        checkpoint: 10,
        generation: 'boundary-generation',
      }),
      /injected clear failure/,
    );
    const rolledBack = await admin.query<{ active_run_id: string; status: string }>(
      `SELECT state.active_run_id, run.status
         FROM ${schema}.analysis_workload_state state
         JOIN ${schema}.analysis_runs run ON run.run_id = state.active_run_id
        WHERE state.workload = $1 AND state.scope = $2`,
      [boundaryRun.workload, boundaryRun.scope],
    );
    assert.equal(rolledBack.rows[0]?.active_run_id, boundaryRun.runId);
    assert.equal(rolledBack.rows[0]?.status, 'running');
    await admin.query(`DROP TRIGGER reject_analysis_clear ON ${schema}.analysis_workload_state`);
    await admin.query(`DROP FUNCTION ${schema}.reject_analysis_clear()`);
    await admin.query(
      `UPDATE ${schema}.analysis_workload_state
          SET active_lease_expires_at = clock_timestamp() - INTERVAL '1 second'
        WHERE workload = $1 AND scope = $2`,
      [boundaryRun.workload, boundaryRun.scope],
    );
    const boundaryReclaim = await runState.beginAnalysisRun(input('boundary'));
    await runState.finishAnalysisRun(boundaryReclaim, {
      status: 'failed',
      checkpoint: 0,
      error: 'injected test terminal',
    });
    const boundaryState = await admin.query<{ active_run_id: string | null }>(
      `SELECT active_run_id
         FROM ${schema}.analysis_workload_state
        WHERE workload = $1 AND scope = $2`,
      [boundaryRun.workload, boundaryRun.scope],
    );
    assert.equal(boundaryState.rows[0]?.active_run_id, null);

    const deadlineRun = await runState.beginAnalysisRun(input('deadline'));
    await admin.query(
      `UPDATE ${schema}.analysis_workload_state
          SET active_run_deadline_at = clock_timestamp() - INTERVAL '1 second'
        WHERE workload = $1 AND scope = $2`,
      [deadlineRun.workload, deadlineRun.scope],
    );
    await assert.rejects(
      runState.heartbeatAnalysisRun(deadlineRun),
      runState.AnalysisRunLeaseLostError,
    );
    await runState.finishAnalysisRun(deadlineRun, {
      status: 'timed_out',
      checkpoint: 4,
      error: 'deadline exceeded',
    });

    const controller = new AbortController();
    const startedAt = Date.now();
    const sleeping = query('SELECT pg_sleep(10)', undefined, controller.signal);
    setTimeout(() => controller.abort(new Error('cancel integration query')), 50).unref();
    await assert.rejects(sleeping, /cancel integration query/);
    assert.ok(Date.now() - startedAt < 2_000);
    assert.equal((await query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1);
  } finally {
    await pool.end();
    await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await admin.end();
  }
});
