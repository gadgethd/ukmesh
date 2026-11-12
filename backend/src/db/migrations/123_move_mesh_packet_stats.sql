-- 123_move_mesh_packet_stats.sql
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE public.packet_daily_stats SET SCHEMA mesh;
ALTER TABLE public.packet_hourly_stats SET SCHEMA mesh;
ALTER TABLE public.packet_decryptions SET SCHEMA mesh;
ALTER TABLE public.packet_forwarding_outbox SET SCHEMA mesh;
