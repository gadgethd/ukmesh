-- 112_move_core_identity_materialized_views.sql
-- P1-4 core domain: identity link and sighting materialized views.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER MATERIALIZED VIEW public.node_identity_links SET SCHEMA core;
ALTER MATERIALIZED VIEW public.node_identity_sightings SET SCHEMA core;
