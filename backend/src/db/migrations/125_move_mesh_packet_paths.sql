-- 125_move_mesh_packet_paths.sql
SET LOCAL lock_timeout = '20s';
SET LOCAL statement_timeout = '300s';

ALTER TABLE public.packet_paths SET SCHEMA mesh;
CREATE VIEW public.packet_paths AS SELECT * FROM mesh.packet_paths;
