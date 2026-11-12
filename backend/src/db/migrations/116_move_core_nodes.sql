-- 116_move_core_nodes.sql
-- P1-4 core domain: canonical node registry.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE public.nodes SET SCHEMA core;
