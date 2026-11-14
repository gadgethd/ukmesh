import { query } from '../db/runtime.js';
import { DATA_LIFECYCLE_POLICIES } from '../db/dataLifecycle.js';

export type RetentionTarget = {
  table: 'packets' | 'worker_health_snapshots' | 'frontend_error_events' | 'owner_alert_deliveries' | 'owner_packet_share_deliveries' | 'public_feed_deliveries' | 'operational_check_results' | 'observer_region_packet_sightings' | 'observer_region_observer_sightings' | 'link_job_commits' | 'analytics.ml_model_variant_packet_results' | 'packet_decryptions' | 'observer_registration_requests' | 'operator_audit_events' | 'node_status_samples';
  timestampColumn: 'ts' | 'time' | 'created_at' | 'updated_at' | 'completed_at' | 'last_seen';
  retention: string;
  batchSize: number;
  extraPredicate?: string;
  /** Physical relation when the logical table was moved to a schema. */
  qualifiedTable?: string;
};

const ROW_CLEANUP_OPTIONS: readonly { table: RetentionTarget['table']; batchSize: number; extraPredicate?: string }[] = [
  { table: 'packets', batchSize: 25_000 },
  { table: 'node_status_samples', batchSize: 25_000 },
  { table: 'worker_health_snapshots', batchSize: 10_000 },
  { table: 'frontend_error_events', batchSize: 2_000 },
  { table: 'owner_alert_deliveries', batchSize: 1_000, extraPredicate: `(status = 'succeeded' OR attempts >= 5)` },
  { table: 'owner_packet_share_deliveries', batchSize: 2_000, extraPredicate: `status IN ('succeeded', 'dead_lettered')` },
  { table: 'public_feed_deliveries', batchSize: 2_000, extraPredicate: `status IN ('succeeded', 'dead_lettered')` },
  { table: 'operational_check_results', batchSize: 2_000 },
  { table: 'observer_region_packet_sightings', batchSize: 25_000 },
  { table: 'observer_region_observer_sightings', batchSize: 5_000 },
  { table: 'link_job_commits', batchSize: 5_000 },
  { table: 'analytics.ml_model_variant_packet_results', batchSize: 5_000 },
  { table: 'packet_decryptions', batchSize: 2_000 },
  { table: 'observer_registration_requests', batchSize: 1_000, extraPredicate: `status IN ('rejected', 'expired', 'provisioned')` },
  { table: 'operator_audit_events', batchSize: 2_000 },
];

function policyForTarget(table: string) {
  return DATA_LIFECYCLE_POLICIES.find(policy => policy.table === table || policy.qualifiedTable === table)!;
}

export const RETENTION_TARGETS: readonly RetentionTarget[] = Object.freeze(ROW_CLEANUP_OPTIONS.map(options => {
  const policy = policyForTarget(options.table);
  if (!policy) throw new Error(`unsupported data lifecycle target: ${options.table}`);
  return Object.freeze({
    ...options,
    ...(options.table.includes('.') ? {} : { qualifiedTable: policy.qualifiedTable }),
    timestampColumn: policy.timestampColumn as RetentionTarget['timestampColumn'],
    retention: policy.retention,
  });
}));

export function retentionDeleteSql(target: RetentionTarget): string {
  // Identifiers come from the closed RetentionTarget union above. PostgreSQL
  // cannot parameterize identifiers, but the retention interval and batch size
  // remain parameters.
  // Compressed hypertables reject ctid (transparent decompression only
  // supports tableoid), so bound the batch by the oldest retained timestamp
  // instead of physical row identity. A NULL boundary (nothing expired) makes
  // the DELETE a no-op.
  return `WITH expired AS MATERIALIZED (
       SELECT MAX(${target.timestampColumn}) AS boundary
       FROM (
         SELECT ${target.timestampColumn}
         FROM ${target.qualifiedTable ?? target.table}
         WHERE ${target.timestampColumn} < NOW() - $1::interval
           ${target.extraPredicate ? `AND ${target.extraPredicate}` : ''}
         ORDER BY ${target.timestampColumn} ASC
         LIMIT $2
       ) AS oldest
     )
     DELETE FROM ${target.qualifiedTable ?? target.table} AS target
     WHERE target.${target.timestampColumn} <= (SELECT boundary FROM expired)
       ${target.extraPredicate ? `AND ${target.extraPredicate}` : ''}`;
}

export async function deleteExpiredRows(
  target: RetentionTarget,
  execute: (text: string, params?: unknown[]) => Promise<unknown> = query,
): Promise<void> {
  // Row-level DELETE on a compressed hypertable forces transparent
  // decompression and can exceed TimescaleDB's per-DML decompression limit.
  // The database capability handles whole chunks and the compressed boundary
  // without granting their ownership to the health process.
  if (policyForTarget(target.table).kind === 'hypertable') {
    try {
      const qualifiedTable = target.qualifiedTable ?? target.table;
      // The database owns chunk discovery, ownership and compressed-boundary
      // cleanup. Runtime credentials cannot operate on arbitrary chunks.
      await execute(
        'SELECT ops.prune_expired_hypertable($1::regclass, $2::interval)',
        [qualifiedTable, target.retention],
      );
      return;
    } catch (error) {
      // Non-Timescale databases (unit/integration fixtures) have plain tables.
      const code = (error as { code?: string }).code;
      if (code !== '42809' && code !== '42883' && code !== '42P01' && code !== '3F000') throw error;
    }
  }
  await execute(retentionDeleteSql(target), [target.retention, target.batchSize]);
}
