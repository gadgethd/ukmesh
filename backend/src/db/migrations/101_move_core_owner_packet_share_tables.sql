-- 101_move_core_owner_packet_share_tables.sql
-- P1-4 core domain: owner packet-sharing rules, destinations and deliveries.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.owner_packet_share_rules SET SCHEMA core;
ALTER TABLE public.owner_packet_share_destinations SET SCHEMA core;
ALTER TABLE public.owner_packet_share_deliveries SET SCHEMA core;
