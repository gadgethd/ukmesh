-- 078_move_message_tags_local.sql
-- P1-4 first mechanical move: unreferenced table, no views, no triggers,
-- no application SQL references.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.message_tags_local SET SCHEMA mesh;
