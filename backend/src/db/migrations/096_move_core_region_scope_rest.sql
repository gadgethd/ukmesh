-- 096_move_core_region_scope_rest.sql
-- P1-4 core domain: region stats, membership and repeater scope status.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.region_scope_stats SET SCHEMA core;
ALTER TABLE public.region_scope_members SET SCHEMA core;
ALTER TABLE public.repeater_scope_status SET SCHEMA core;
