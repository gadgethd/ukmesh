BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SELECT COUNT(*) FILTER (WHERE (n_live_tup>10000 AND n_dead_tup>GREATEST(10000,n_live_tup*0.1)) OR n_dead_tup>50000) AS previous_tables_needing_vacuum FROM pg_stat_user_tables;
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
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) SELECT
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
COMMIT;
