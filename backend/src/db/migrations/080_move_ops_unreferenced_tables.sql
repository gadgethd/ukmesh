-- 080_move_ops_unreferenced_tables.sql
-- P1-4 ops domain: three unreferenced simulation tables.
--
-- The `path_sim_*` tables are legacy deployment objects: no migration in this
-- tree creates them, so only a database provisioned before the move has them.
-- A database rebuilt from the shipped base schema has nothing to move, so each
-- move is guarded rather than assumed. Where a table exists, the statement run
-- for it is exactly the shipped move.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
  IF to_regclass('public.path_sim_evolution_state') IS NULL THEN
    RAISE NOTICE '080: public.path_sim_evolution_state is absent (base-schema database); move to ops skipped';
  ELSE
    ALTER TABLE public.path_sim_evolution_state SET SCHEMA ops;
  END IF;

  IF to_regclass('public.path_sim_population') IS NULL THEN
    RAISE NOTICE '080: public.path_sim_population is absent (base-schema database); move to ops skipped';
  ELSE
    ALTER TABLE public.path_sim_population SET SCHEMA ops;
  END IF;

  IF to_regclass('public.path_simulation_runs') IS NULL THEN
    RAISE NOTICE '080: public.path_simulation_runs is absent (base-schema database); move to ops skipped';
  ELSE
    ALTER TABLE public.path_simulation_runs SET SCHEMA ops;
  END IF;
END
$$;
