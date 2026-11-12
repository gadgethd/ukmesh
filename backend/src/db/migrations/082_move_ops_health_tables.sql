-- 082_move_ops_health_tables.sql
-- P1-4 ops domain: worker health and operational check tables.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.worker_health_current SET SCHEMA ops;
ALTER TABLE public.worker_health_snapshots SET SCHEMA ops;
ALTER TABLE public.operational_check_results SET SCHEMA ops;
