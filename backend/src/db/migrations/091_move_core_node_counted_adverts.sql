-- 091_move_core_node_counted_adverts.sql
-- P1-4 core domain: counted advert deduplication ledger.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.node_counted_adverts SET SCHEMA core;
