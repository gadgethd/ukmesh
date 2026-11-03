-- 115_move_core_node_sample_hypertables.sql
-- P1-4 core domain: status and neighbour sample hypertables.
-- Timescale moves the hypertable and its chunks; policies remain attached.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE public.node_status_samples SET SCHEMA core;
ALTER TABLE public.node_neighbor_samples SET SCHEMA core;
