-- 095_move_core_identity_match_evidence.sql
-- P1-4 core domain: node identity match evidence.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.node_identity_match_evidence SET SCHEMA core;
