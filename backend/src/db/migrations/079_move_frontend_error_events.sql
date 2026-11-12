-- 079_move_frontend_error_events.sql
-- P1-4 ops-domain move: browser diagnostics table only.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.frontend_error_events SET SCHEMA ops;
