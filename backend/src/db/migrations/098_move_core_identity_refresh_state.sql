-- 098_move_core_identity_refresh_state.sql
-- P1-4 core domain: identity refresh generation state.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.node_identity_refresh_state SET SCHEMA core;
