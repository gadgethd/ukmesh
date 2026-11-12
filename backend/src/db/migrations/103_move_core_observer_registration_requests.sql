-- 103_move_core_observer_registration_requests.sql
-- P1-4 core domain: observer registration requests.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.observer_registration_requests SET SCHEMA core;
