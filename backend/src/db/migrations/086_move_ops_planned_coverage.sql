-- 086_move_ops_planned_coverage.sql
-- P1-4 ops domain: planned coverage jobs, handles and planned nodes.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.planned_coverage_handles SET SCHEMA ops;
ALTER TABLE public.planned_coverage_jobs SET SCHEMA ops;
ALTER TABLE public.planned_nodes SET SCHEMA ops;
