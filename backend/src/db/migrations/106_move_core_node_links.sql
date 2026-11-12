-- 106_move_core_node_links.sql
-- P1-4 core domain: computed RF link graph.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.node_links SET SCHEMA core;
