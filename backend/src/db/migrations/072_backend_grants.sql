-- 072_backend_grants.sql
-- Phase P1-3 schema-boundary grants. Table-level grants are intentionally
-- deferred to the migrations that create/move each target table.
-- Requires scripts/bootstrap-backend-roles.sql to have run first (P1-8).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
  missing text;
BEGIN
  SELECT string_agg(r, ', ') INTO missing
  FROM unnest(ARRAY[
    'meshcore_migrate', 'meshcore_ingest', 'meshcore_api',
    'meshcore_owner', 'meshcore_worker',
    'corescope_ro', 'ukmesh_planner_reader', 'meshgremlin_ro'
  ]) AS r
  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r);
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'bootstrap roles missing: %', missing;
  END IF;
END
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

GRANT USAGE ON SCHEMA core, mesh, analytics, archive, ops TO meshcore_migrate;

GRANT USAGE ON SCHEMA core, mesh, ops TO meshcore_ingest;
GRANT USAGE ON SCHEMA core, mesh, analytics TO meshcore_api;
GRANT USAGE ON SCHEMA core TO meshcore_owner;
GRANT USAGE ON SCHEMA mesh, analytics, ops TO meshcore_worker;

GRANT USAGE ON SCHEMA analytics TO corescope_ro;
GRANT USAGE ON SCHEMA analytics TO ukmesh_planner_reader;
GRANT USAGE ON SCHEMA analytics TO meshgremlin_ro;

-- No blanket table grants, no PUBLIC grants, no role inheritance.
-- The view owner (P1-12) gets SELECT on ops.channel_blocklist and
-- ops.node_privacy; app/reader roles do not get direct access to those tables.
