-- 121_move_mesh_message_tags.sql
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.message_tags SET SCHEMA mesh;
