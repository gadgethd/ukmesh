/** PostgreSQL 16's update/delete vacuum trigger, with a health noise floor.
 *
 * Use pg_class.reltuples, not n_live_tup: it is the estimate the autovacuum
 * scheduler uses. Table storage parameters override the server defaults.
 * Disabled autovacuum (or statistics collection) must not hide a backlog.
 * Insert/freeze-triggered vacuums are separate from this dead-row check.
 */
export const DATABASE_VACUUM_SETTINGS_SQL = `SELECT
  current_setting('autovacuum')::boolean AS autovacuum_enabled,
  current_setting('track_counts')::boolean AS track_counts,
  current_setting('autovacuum_vacuum_threshold')::double precision AS vacuum_threshold,
  current_setting('autovacuum_vacuum_scale_factor')::double precision AS vacuum_scale_factor`;

// c and settings are supplied by both the catalog query and SQL test fixtures.
export const DATABASE_VACUUM_THRESHOLD_SQL = `GREATEST(
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
)`;

/** Catalog-only: never scans application rows or invokes maintenance. */
export const DATABASE_VACUUM_TABLES_SQL = `WITH settings AS (
  ${DATABASE_VACUUM_SETTINGS_SQL}
)
SELECT s.*, c.reltuples, c.reloptions,
  ${DATABASE_VACUUM_THRESHOLD_SQL} AS health_dead_row_threshold
FROM pg_catalog.pg_stat_user_tables s
JOIN pg_catalog.pg_class c ON c.oid = s.relid
CROSS JOIN settings`;

export const DATABASE_MAINTENANCE_SQL = `SELECT
  pg_database_size(current_database())::text AS database_size_bytes,
  COALESCE(SUM(n_dead_tup), 0)::text AS dead_rows,
  MIN(last_autovacuum)::text AS oldest_vacuum_at,
  COUNT(*) FILTER (WHERE n_dead_tup > health_dead_row_threshold)::text AS tables_needing_vacuum
FROM (${DATABASE_VACUUM_TABLES_SQL}) table_health`;
