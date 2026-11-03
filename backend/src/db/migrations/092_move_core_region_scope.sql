-- 092_move_core_region_scope.sql
-- P1-4 core domain: region participation run and unattributed counts.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.region_scope_run SET SCHEMA core;
ALTER TABLE public.region_scope_unattributed SET SCHEMA core;
