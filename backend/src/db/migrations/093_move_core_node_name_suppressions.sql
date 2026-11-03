-- 093_move_core_node_name_suppressions.sql
-- P1-4 core domain: node name history and map suppressions.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.node_name_history SET SCHEMA core;
ALTER TABLE public.node_map_suppressions SET SCHEMA core;
