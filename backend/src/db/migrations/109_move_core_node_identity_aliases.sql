-- 109_move_core_node_identity_aliases.sql
-- P1-4 core domain: canonical identity aliases.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.node_identity_aliases SET SCHEMA core;
