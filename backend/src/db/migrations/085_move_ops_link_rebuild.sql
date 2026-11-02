-- 085_move_ops_link_rebuild.sql
-- P1-4 ops domain: link rebuild run history.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.link_rebuild_runs SET SCHEMA ops;
