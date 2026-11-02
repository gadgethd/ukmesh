-- 081_move_analytics_unreferenced_tables.sql
-- P1-4 analytics domain: two unreferenced derived tables.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.last_hop_snr_daily SET SCHEMA analytics;
ALTER TABLE public.multihop_paths_weekly SET SCHEMA analytics;
