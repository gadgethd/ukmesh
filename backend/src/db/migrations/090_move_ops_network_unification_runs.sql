-- 090_move_ops_network_unification_runs.sql
-- P1-4 ops domain: network unification run history.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.network_unification_runs SET SCHEMA ops;
