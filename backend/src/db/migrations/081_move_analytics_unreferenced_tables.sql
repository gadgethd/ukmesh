-- 081_move_analytics_unreferenced_tables.sql
-- P1-4 analytics domain: two unreferenced derived tables.
--
-- Both tables are legacy deployment objects: no migration in this tree creates
-- them, so only a database provisioned before the move has them. A database
-- rebuilt from the shipped base schema has nothing to move, so each move is
-- guarded rather than assumed. Where a table exists, the statement run for it
-- is exactly the shipped move.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
  IF to_regclass('public.last_hop_snr_daily') IS NULL THEN
    RAISE NOTICE '081: public.last_hop_snr_daily is absent (base-schema database); move to analytics skipped';
  ELSE
    ALTER TABLE public.last_hop_snr_daily SET SCHEMA analytics;
  END IF;

  IF to_regclass('public.multihop_paths_weekly') IS NULL THEN
    RAISE NOTICE '081: public.multihop_paths_weekly is absent (base-schema database); move to analytics skipped';
  ELSE
    ALTER TABLE public.multihop_paths_weekly SET SCHEMA analytics;
  END IF;
END
$$;
