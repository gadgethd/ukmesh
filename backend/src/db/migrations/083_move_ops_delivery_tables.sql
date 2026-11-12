-- 083_move_ops_delivery_tables.sql
-- P1-4 ops domain: feed deliveries, link commits and ingest rejections.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.public_feed_deliveries SET SCHEMA ops;
ALTER TABLE public.link_job_commits SET SCHEMA ops;
ALTER TABLE public.ingest_rejections SET SCHEMA ops;
