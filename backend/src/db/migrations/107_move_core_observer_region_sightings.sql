-- 107_move_core_observer_region_sightings.sql
-- P1-4 core domain: observer region packet/observer sightings.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.observer_region_packet_sightings SET SCHEMA core;
ALTER TABLE public.observer_region_observer_sightings SET SCHEMA core;
