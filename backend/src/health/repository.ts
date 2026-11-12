import type { DatabaseQueryFn } from '../db/contracts.js';
import type { WorkerSnapshot, OperationalCheckRow } from './types.js';
import { lifecyclePolicy, compressionPolicy } from '../db/dataLifecycle.js';
import { INGEST_HEALTH_SQL, PATH_HASH_HEALTH_SQL } from './packetDiagnostics.js';
import { DATABASE_MAINTENANCE_SQL } from './vacuumBacklog.js';

export const CORE_CAPACITY_TABLES = ['packets', 'node_status_samples', 'node_neighbor_samples'] as const;
export const CORE_CAPACITY_TARGETS = Object.freeze(CORE_CAPACITY_TABLES.map(table => {
  const retention = lifecyclePolicy(table), compression = compressionPolicy(table);
  const relation = retention.qualifiedTable ?? retention.table;
  const parts = relation.split('.');
  return Object.freeze({ schema_name: parts.length > 1 ? parts[0]! : 'public', table_name: table,
    retention: retention.retention, compress_after: compression.compressAfter });
}));
const packetPaths = compressionPolicy('packet_paths');
const packetPathSchema = packetPaths.qualifiedTable?.split('.')[0] ?? 'public';

export function createHealthRepository(query: DatabaseQueryFn) {
  return {
    loadPacketPathCapacity() {
      return query<{
      rows_30d: string;
      approximate_rows: string;
      total_bytes: string;
      overdue_uncompressed_chunks: string;
    }>(`
      SELECT
        (SELECT COUNT(*)::text FROM ${packetPaths.qualifiedTable ?? packetPaths.table}
          WHERE time >= NOW() - INTERVAL '30 days') AS rows_30d,
        approximate_row_count($1::regclass)::text AS approximate_rows,
        pg_total_relation_size($1::regclass)::text AS total_bytes,
        (SELECT COUNT(*)::text
           FROM timescaledb_information.chunks
          WHERE hypertable_schema = $2
            AND hypertable_name = $3
            AND range_end < NOW() - $4::interval - INTERVAL '1 day'
            AND NOT is_compressed) AS overdue_uncompressed_chunks
    `, [packetPaths.qualifiedTable ?? packetPaths.table, packetPathSchema, packetPaths.table, packetPaths.compressAfter]);
    },
    loadCoreCapacity() {
      return query<{
      table_name: string;
      chunk_count: string;
      total_bytes: string;
      expired_chunks: string;
      overdue_uncompressed_chunks: string;
    }>(`
      WITH lifecycle_targets AS (
        SELECT schema_name, table_name, retention::interval AS retention, compress_after::interval AS compress_after
        FROM jsonb_to_recordset($1::jsonb) AS value(schema_name text, table_name text, retention text, compress_after text)
      )
      SELECT target.table_name,
             COUNT(chunk.chunk_name)::text AS chunk_count,
             COALESCE(SUM(CASE
               WHEN chunk.chunk_name IS NULL THEN 0
               ELSE pg_total_relation_size(
                 format('%I.%I', chunk.chunk_schema, chunk.chunk_name)::regclass
               )
             END), 0)::text AS total_bytes,
             COUNT(chunk.chunk_name) FILTER (
               WHERE chunk.range_end < NOW() - target.retention
             )::text AS expired_chunks,
             COUNT(chunk.chunk_name) FILTER (
               WHERE NOT COALESCE(chunk.is_compressed, FALSE)
                 AND chunk.range_end < NOW() - target.compress_after - INTERVAL '1 day'
             )::text AS overdue_uncompressed_chunks
        FROM lifecycle_targets target
        LEFT JOIN timescaledb_information.chunks chunk
          ON chunk.hypertable_schema = target.schema_name
         AND chunk.hypertable_name = target.table_name
       GROUP BY target.table_name
    `, [JSON.stringify(CORE_CAPACITY_TARGETS)]);
    },
    loadRecentLinkCount() {
      return query<{ count: string }>(`SELECT COUNT(*) AS count FROM core.node_links WHERE itm_computed_at > NOW() - INTERVAL '1 hour'`);
    },
    loadLatestLinkComputation() {
      return query<{ ts: string | null }>(`SELECT MAX(itm_computed_at)::text AS ts FROM core.node_links`);
    },
    loadLatestLearningCalibration() {
      return query<{ ts: string | null }>(`SELECT MAX(updated_at)::text AS ts FROM analytics.path_model_calibration`);
    },
    loadRecentHealthActivity() {
      return query<{ count: string; ts: string | null }>(
        `SELECT COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '1 hour') AS count,
                MAX(ts)::text AS ts
         FROM ops.worker_health_snapshots
         WHERE worker_name = 'health-worker'`,
      );
    },
    loadBackfillActivity() {
      return query<{ links: string; last_observed: string | null }>(
        `SELECT COUNT(*)::text AS links, MAX(last_observed)::text AS last_observed
         FROM core.node_links`,
      );
    },
    loadCurrentWorkers() {
      return query<WorkerSnapshot & { captured_at: string }>(
      `SELECT worker_name, status, queue_depth, processed_1h,
              last_activity_at::text, cpu_load_1m, cpu_usage_pct,
              mem_used_pct, disk_used_pct, queue_bytes, dead_jobs,
              retries, active_leases, oldest_age_s, captured_at::text
         FROM ops.worker_health_current
        ORDER BY worker_name`,
    );
    },
    saveWorkerSnapshot(capturedAt: string, rows: readonly WorkerSnapshot[]) {
      return query(
      `WITH snapshot AS MATERIALIZED (
         SELECT *
           FROM jsonb_to_recordset($2::jsonb) AS value(
             worker_name text,
             status text,
             queue_depth integer,
             processed_1h integer,
             last_activity_at timestamptz,
             cpu_load_1m double precision,
             cpu_usage_pct double precision,
             mem_used_pct double precision,
             disk_used_pct double precision,
             queue_bytes bigint,
             dead_jobs integer,
             retries integer,
             active_leases integer,
             oldest_age_s double precision
           )
       ), history_write AS (
         INSERT INTO ops.worker_health_snapshots
           (ts, worker_name, status, queue_depth, processed_5m, processed_1h,
            last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct)
         SELECT $1::timestamptz, worker_name, status, queue_depth, 0, processed_1h,
                last_activity_at, cpu_load_1m, mem_used_pct, disk_used_pct
           FROM snapshot
         RETURNING worker_name
       ), current_cleanup AS (
         DELETE FROM ops.worker_health_current current
          WHERE NOT EXISTS (
            SELECT 1 FROM snapshot WHERE snapshot.worker_name = current.worker_name
          )
         RETURNING worker_name
       )
       INSERT INTO ops.worker_health_current
         (worker_name, captured_at, status, queue_depth, processed_1h,
          last_activity_at, cpu_load_1m, cpu_usage_pct, mem_used_pct,
          disk_used_pct, queue_bytes, dead_jobs, retries, active_leases, oldest_age_s)
       SELECT worker_name, $1::timestamptz, status, queue_depth, processed_1h,
              last_activity_at, cpu_load_1m, cpu_usage_pct, mem_used_pct,
              disk_used_pct, queue_bytes, dead_jobs, retries, active_leases, oldest_age_s
         FROM snapshot
       ON CONFLICT (worker_name) DO UPDATE SET
         captured_at = EXCLUDED.captured_at,
         status = EXCLUDED.status,
         queue_depth = EXCLUDED.queue_depth,
         processed_1h = EXCLUDED.processed_1h,
         last_activity_at = EXCLUDED.last_activity_at,
         cpu_load_1m = EXCLUDED.cpu_load_1m,
         cpu_usage_pct = EXCLUDED.cpu_usage_pct,
         mem_used_pct = EXCLUDED.mem_used_pct,
         disk_used_pct = EXCLUDED.disk_used_pct,
         queue_bytes = EXCLUDED.queue_bytes,
         dead_jobs = EXCLUDED.dead_jobs,
         retries = EXCLUDED.retries,
         active_leases = EXCLUDED.active_leases,
         oldest_age_s = EXCLUDED.oldest_age_s`,
      [capturedAt, JSON.stringify(rows)],
    );
    },
    loadOperationalChecks() {
      return query<OperationalCheckRow>(
        `SELECT DISTINCT ON (check_name)
           check_name, status, latency_ms, detail, ts::text
         FROM ops.operational_check_results
         ORDER BY check_name, ts DESC`,
      );
    },
    loadHistory() {
      return query<{
        ts: string;
        worker_name: string;
        status: string;
        queue_depth: number;
        processed_1h: number;
        cpu_load_1m: number | null;
        mem_used_pct: number | null;
        disk_used_pct: number | null;
      }>(
        `SELECT ts::text, worker_name, status, queue_depth, processed_1h, cpu_load_1m, mem_used_pct, disk_used_pct
         FROM ops.worker_health_snapshots
         ORDER BY ts DESC
         LIMIT 720`,
      );
    },
    loadFrontendErrors() {
      return query<{ count: string }>(`SELECT COUNT(*) AS count FROM ops.frontend_error_events WHERE time > NOW() - INTERVAL '1 hour'`);
    },
    loadIngest() {
      return query<{
        stale_nodes: string;
        active_nodes: string;
        max_stale_minutes: string | null;
        stale_threshold_minutes: string;
        global_last_packet_at: string | null;
      }>(
        INGEST_HEALTH_SQL,
      );
    },
    loadPathDiagnostics() {
      return query<{
        one_byte: string;
        two_byte: string;
        three_byte: string;
        latest_multibyte_at: string | null;
        multibyte_packets_24h: string;
      }>(
        PATH_HASH_HEALTH_SQL,
      );
    },
    loadDatabaseMaintenance() {
      return query<{
        database_size_bytes: string;
        dead_rows: string;
        oldest_vacuum_at: string | null;
        tables_needing_vacuum: string;
      }>(DATABASE_MAINTENANCE_SQL);
    },
    loadDatabaseRuntime() {
      return query<{
        connection_count: string;
        max_connections: string;
        cache_hit_ratio: string | null;
      }>(
        `SELECT
           (SELECT COUNT(*)::text FROM pg_stat_activity WHERE datname = current_database()) AS connection_count,
           current_setting('max_connections') AS max_connections,
           ROUND(
             100 * blks_hit::numeric / NULLIF(blks_hit + blks_read, 0),
             2
           )::text AS cache_hit_ratio
         FROM pg_stat_database
         WHERE datname = current_database()`,
      );
    },
    loadPrivacyBacklog(maxAttempts: number) {
      return query<{
        pending: string;
        processing: string;
        failed: string;
        failed_retryable: string;
        failed_exhausted: string;
        oldest_pending_age_seconds: string | null;
      }>(`
        SELECT COUNT(*) FILTER (WHERE status = 'pending')::text AS pending,
               COUNT(*) FILTER (WHERE status = 'processing')::text AS processing,
               COUNT(*) FILTER (WHERE status = 'failed')::text AS failed,
               COUNT(*) FILTER (WHERE status = 'failed' AND attempts < $1)::text AS failed_retryable,
               COUNT(*) FILTER (WHERE status = 'failed' AND attempts >= $1)::text AS failed_exhausted,
               MAX(EXTRACT(EPOCH FROM (NOW() - requested_at)))
                 FILTER (WHERE status = 'pending')::text AS oldest_pending_age_seconds
          FROM ops.privacy_rematerialization_queue
      `, [maxAttempts]);
    },
  };
}
