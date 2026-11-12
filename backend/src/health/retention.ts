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

async function rowsOf(
  execute: (text: string, params?: unknown[]) => Promise<unknown>,
  text: string,
  params?: unknown[],
): Promise<Array<Record<string, unknown>>> {
  const result = (await execute(text, params)) as { rows?: Array<Record<string, unknown>> } | undefined;
  return result?.rows ?? [];
}

export async function deleteExpiredRows(
  target: RetentionTarget,
  execute: (text: string, params?: unknown[]) => Promise<unknown> = query,
): Promise<void> {
  // Row-level DELETE on a compressed hypertable forces transparent
  // decompression and can exceed TimescaleDB's per-DML decompression limit.
  // Whole-chunk drops are the supported retention path; the boundary chunk is
  // kept until it is fully expired, so retention can overshoot by one chunk.
  if (policyForTarget(target.table).kind === 'hypertable') {
    try {
      const qualifiedTable = target.qualifiedTable ?? target.table;
      const [schemaName, tableName] = qualifiedTable.includes('.')
        ? qualifiedTable.split('.', 2) as [string, string]
        : ['public', qualifiedTable] as [string, string];
      // Remove every chunk that is entirely past the retention horizon without
      // decompressing anything.
      await execute(
        'SELECT drop_chunks($1::regclass, older_than => $2::interval)',
        [qualifiedTable, target.retention],
      );
      // The boundary chunk can still hold a few expired rows next to retained
      // ones. Row DML on a compressed chunk is rejected by TimescaleDB's
      // decompression limit, so decompress just that chunk, delete, recompress.
      const chunks = await rowsOf(
        execute,
        `SELECT format('%I.%I', chunk_schema, chunk_name) AS chunk, is_compressed
           FROM timescaledb_information.chunks
          WHERE hypertable_schema = $1
            AND hypertable_name = $2
            AND range_end > now() - $3::interval
          ORDER BY range_end ASC
          LIMIT 1`,
        [schemaName, tableName, target.retention],
      );
      const chunk = chunks[0];
      if (!chunk) return;
      const chunkName = String(chunk['chunk']);
      const expired = await rowsOf(
        execute,
        `SELECT 1 FROM ${chunkName}
          WHERE ${target.timestampColumn} < now() - $1::interval
          LIMIT 1`,
        [target.retention],
      );
      if (expired.length === 0) return;
      await execute('SELECT decompress_chunk($1::regclass, if_compressed => true)', [chunkName]);
      try {
        // Delete only inside this chunk. A table-wide boundary can reach into
        // older compressed chunks that were not decompressed and trip the
        // per-DML decompression limit again.
        await execute(
          `DELETE FROM ${chunkName}
            WHERE ${target.timestampColumn} < now() - $1::interval
            ${target.extraPredicate ? `AND ${target.extraPredicate}` : ''}`,
          [target.retention],
        );
      } finally {
        if (chunk['is_compressed'] === true) {
          await execute('SELECT compress_chunk($1::regclass, if_not_compressed => true)', [chunkName]);
        }
      }
      return;
    } catch (error) {
      // Non-Timescale databases (unit/integration fixtures) have plain tables.
      const code = (error as { code?: string }).code;
      if (code !== '42809' && code !== '42883' && code !== '42P01') throw error;
    }
  }
  await execute(retentionDeleteSql(target), [target.retention, target.batchSize]);
}
