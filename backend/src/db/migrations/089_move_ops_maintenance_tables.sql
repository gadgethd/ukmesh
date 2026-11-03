-- 089_move_ops_maintenance_tables.sql
-- P1-4 ops domain: backfill checkpoints and removed-record archive.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.maintenance_backfill_checkpoints SET SCHEMA ops;
ALTER TABLE public.maintenance_removed_records SET SCHEMA ops;
