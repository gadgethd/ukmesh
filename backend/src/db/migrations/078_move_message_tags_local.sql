-- 078_move_message_tags_local.sql
-- P1-4 first mechanical move: unreferenced table, no views, no triggers,
-- no application SQL references.
--
-- `public.message_tags_local` is a legacy deployment object: no migration in
-- this tree creates it, so only a database provisioned before the move has it.
-- A database rebuilt from the shipped base schema has nothing to move, so the
-- move is guarded rather than assumed and the band continues past 078.
-- Where the table does exist the statement below is exactly the shipped move.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
  IF to_regclass('public.message_tags_local') IS NULL THEN
    RAISE NOTICE '078: public.message_tags_local is absent (base-schema database); move to mesh skipped';
    RETURN;
  END IF;
  ALTER TABLE public.message_tags_local SET SCHEMA mesh;
END
$$;
