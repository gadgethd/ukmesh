-- 100_move_core_owner_alert_tables.sql
-- P1-4 core domain: owner alert rules, deliveries and delivery attempts.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.owner_alert_rules SET SCHEMA core;
ALTER TABLE public.owner_alert_deliveries SET SCHEMA core;
ALTER TABLE public.owner_alert_delivery_attempts SET SCHEMA core;
