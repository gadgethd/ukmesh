-- 102_move_core_owner_public_feed_tables.sql
-- P1-4 core domain: owner public-feed settings and node allowlists.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.owner_public_feed_settings SET SCHEMA core;
ALTER TABLE public.owner_public_feed_nodes SET SCHEMA core;
