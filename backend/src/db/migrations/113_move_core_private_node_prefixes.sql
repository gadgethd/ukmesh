-- 113_move_core_private_node_prefixes.sql
-- P1-4 core domain: private node prefix registry and its visibility triggers.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.private_node_prefixes SET SCHEMA core;
