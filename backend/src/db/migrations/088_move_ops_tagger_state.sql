-- 088_move_ops_tagger_state.sql
-- P1-4 ops domain: tagger cursor and retry state.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.tagger_state SET SCHEMA ops;
ALTER TABLE public.tagger_retry SET SCHEMA ops;
