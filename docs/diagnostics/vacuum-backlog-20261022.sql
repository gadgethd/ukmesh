\set ON_ERROR_STOP on
\pset pager off
BEGIN READ ONLY;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '30s';
SELECT now() AS measured_at, current_database(), current_user,
  current_setting('server_version') AS server_version,
  current_setting('transaction_read_only') AS read_only;
SELECT name, setting, unit, source FROM pg_settings
WHERE name LIKE 'autovacuum%'
   OR name IN ('track_counts', 'vacuum_cost_delay', 'vacuum_cost_limit',
     'maintenance_work_mem', 'max_prepared_transactions', 'max_worker_processes')
ORDER BY name;
SELECT
  pg_database_size(current_database())::text AS database_size_bytes,
  COALESCE(SUM(n_dead_tup), 0)::text AS dead_rows,
  MIN(last_autovacuum)::text AS oldest_vacuum_at,
  COUNT(*) FILTER (WHERE n_dead_tup > health_dead_row_threshold)::text AS tables_needing_vacuum
FROM (WITH settings AS (
  SELECT
  current_setting('autovacuum')::boolean AS autovacuum_enabled,
  current_setting('track_counts')::boolean AS track_counts,
  current_setting('autovacuum_vacuum_threshold')::double precision AS vacuum_threshold,
  current_setting('autovacuum_vacuum_scale_factor')::double precision AS vacuum_scale_factor
)
SELECT s.*, c.reltuples, c.reloptions,
  GREATEST(
  10000::double precision,
  CASE WHEN settings.autovacuum_enabled AND settings.track_counts
    AND COALESCE((SELECT option_value::boolean
      FROM pg_catalog.pg_options_to_table(c.reloptions)
      WHERE option_name = 'autovacuum_enabled'), TRUE)
  THEN COALESCE((SELECT option_value::double precision
      FROM pg_catalog.pg_options_to_table(c.reloptions)
      WHERE option_name = 'autovacuum_vacuum_threshold'), settings.vacuum_threshold)
    + COALESCE((SELECT option_value::double precision
      FROM pg_catalog.pg_options_to_table(c.reloptions)
      WHERE option_name = 'autovacuum_vacuum_scale_factor'), settings.vacuum_scale_factor)
      * GREATEST(c.reltuples, 0)
  ELSE 0 END
) AS health_dead_row_threshold
FROM pg_catalog.pg_stat_user_tables s
JOIN pg_catalog.pg_class c ON c.oid = s.relid
CROSS JOIN settings) table_health;
SELECT schemaname, relname, reltuples, n_live_tup, n_dead_tup,
  health_dead_row_threshold, n_dead_tup > health_dead_row_threshold AS needs_vacuum,
  n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del,
  last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
  vacuum_count, autovacuum_count, reloptions,
  pg_table_size(relid) AS table_bytes, pg_indexes_size(relid) AS index_bytes,
  pg_total_relation_size(relid) AS total_bytes
FROM (WITH settings AS (
  SELECT
  current_setting('autovacuum')::boolean AS autovacuum_enabled,
  current_setting('track_counts')::boolean AS track_counts,
  current_setting('autovacuum_vacuum_threshold')::double precision AS vacuum_threshold,
  current_setting('autovacuum_vacuum_scale_factor')::double precision AS vacuum_scale_factor
)
SELECT s.*, c.reltuples, c.reloptions,
  GREATEST(
  10000::double precision,
  CASE WHEN settings.autovacuum_enabled AND settings.track_counts
    AND COALESCE((SELECT option_value::boolean
      FROM pg_catalog.pg_options_to_table(c.reloptions)
      WHERE option_name = 'autovacuum_enabled'), TRUE)
  THEN COALESCE((SELECT option_value::double precision
      FROM pg_catalog.pg_options_to_table(c.reloptions)
      WHERE option_name = 'autovacuum_vacuum_threshold'), settings.vacuum_threshold)
    + COALESCE((SELECT option_value::double precision
      FROM pg_catalog.pg_options_to_table(c.reloptions)
      WHERE option_name = 'autovacuum_vacuum_scale_factor'), settings.vacuum_scale_factor)
      * GREATEST(c.reltuples, 0)
  ELSE 0 END
) AS health_dead_row_threshold
FROM pg_catalog.pg_stat_user_tables s
JOIN pg_catalog.pg_class c ON c.oid = s.relid
CROSS JOIN settings) table_health
ORDER BY n_dead_tup DESC LIMIT 30;
-- Intentionally omit SQL text: activity queries can contain secrets/user data.
SELECT pid, datname, backend_type, application_name, state,
  now() - xact_start AS transaction_age, now() - query_start AS query_age,
  backend_xid, backend_xmin, wait_event_type, wait_event,
  pg_blocking_pids(pid) AS blockers
FROM pg_stat_activity
WHERE pid <> pg_backend_pid() AND (
  xact_start < now() - interval '1 minute'
  OR backend_xmin IS NOT NULL OR backend_type = 'autovacuum worker')
ORDER BY xact_start NULLS LAST;
-- GIDs are also omitted; age and database suffice to identify pinned horizons.
SELECT transaction, prepared, now() - prepared AS prepared_age, owner, database
FROM pg_prepared_xacts;
SELECT slot_name, slot_type, database, active, xmin, catalog_xmin,
  restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;
SELECT pid, datname, relid::regclass AS relation, phase, heap_blks_total,
  heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count
FROM pg_stat_progress_vacuum;
SELECT datname, stats_reset, deadlocks, xact_commit, xact_rollback
FROM pg_stat_database WHERE datname = current_database();
SELECT age(datfrozenxid) AS database_xid_age,
  current_setting('autovacuum_freeze_max_age') AS freeze_max_age
FROM pg_database WHERE datname = current_database();
COMMIT;
