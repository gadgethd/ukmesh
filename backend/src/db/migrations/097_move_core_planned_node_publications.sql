-- 097_move_core_planned_node_publications.sql
-- P1-4 core domain: planned node publication state.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.planned_node_publications SET SCHEMA core;
