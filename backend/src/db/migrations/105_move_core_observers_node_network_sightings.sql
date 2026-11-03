-- 105_move_core_observers_node_network_sightings.sql
-- P1-4 core domain: observer registry and node network sightings.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.observers SET SCHEMA core;
ALTER TABLE public.node_network_sightings SET SCHEMA core;
