-- 094_move_core_node_coverage.sql
-- P1-4 core domain: node coverage map.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.node_coverage SET SCHEMA core;
