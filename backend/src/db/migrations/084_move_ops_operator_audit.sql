-- 084_move_ops_operator_audit.sql
-- P1-4 ops domain: operator audit events.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.operator_audit_events SET SCHEMA ops;
