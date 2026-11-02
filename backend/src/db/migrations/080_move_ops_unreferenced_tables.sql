-- 080_move_ops_unreferenced_tables.sql
-- P1-4 ops domain: three unreferenced simulation tables.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.path_sim_evolution_state SET SCHEMA ops;
ALTER TABLE public.path_sim_population SET SCHEMA ops;
ALTER TABLE public.path_simulation_runs SET SCHEMA ops;
