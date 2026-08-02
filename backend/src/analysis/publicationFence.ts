import type { PoolClient } from 'pg';

export type AnalysisPublicationHandle = {
  runId: string;
  workload: string;
  scope: string;
  leaseToken: string;
  privacyGeneration?: number;
  modelGeneration?: string;
};

export class AnalysisPublicationFenceError extends Error {
  constructor(readonly runId: string) {
    super(`analysis publication fence rejected run: ${runId}`);
  }
}

export async function assertAnalysisPublicationLease(
  client: PoolClient,
  handle: AnalysisPublicationHandle,
): Promise<void> {
  const result = await client.query(
    `SELECT 1
       FROM analysis_workload_state state
       JOIN analysis_runs run ON run.run_id = state.active_run_id
      WHERE state.workload = $1
        AND state.scope = $2
        AND state.active_run_id = $3
        AND state.active_lease_token = $4
        AND run.lease_token = $4
        AND run.status = 'running'
        AND state.active_lease_expires_at > clock_timestamp()
        AND state.active_run_deadline_at > clock_timestamp()
        AND run.privacy_generation IS NOT DISTINCT FROM $5::bigint
        AND run.model_generation IS NOT DISTINCT FROM $6::text
        AND (
          $5::bigint IS NULL
          OR EXISTS (
            SELECT 1
              FROM public_visibility_state visibility
             WHERE visibility.singleton = TRUE
               AND visibility.generation = $5::bigint
          )
        )`,
    [
      handle.workload,
      handle.scope,
      handle.runId,
      handle.leaseToken,
      handle.privacyGeneration ?? null,
      handle.modelGeneration ?? null,
    ],
  );
  if (result.rows.length !== 1) {
    throw new AnalysisPublicationFenceError(handle.runId);
  }
}
