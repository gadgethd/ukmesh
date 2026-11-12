-- 104_move_core_node_link_radio_reports.sql
-- P1-4 core domain: link radio report evidence and its identity-mapped view.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.node_link_radio_reports SET SCHEMA core;
ALTER VIEW public.node_identity_link_radio_reports SET SCHEMA core;
