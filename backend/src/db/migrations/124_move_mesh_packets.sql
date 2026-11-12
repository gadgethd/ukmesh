-- 124_move_mesh_packets.sql
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '180s';

ALTER TABLE public.packets SET SCHEMA mesh;
CREATE VIEW public.packets AS SELECT * FROM mesh.packets;
