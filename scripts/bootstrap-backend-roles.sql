-- P1-3 / P1-8 role bootstrap proposal.
-- Run as the operator against the canonical meshcore database, NOT by the
-- application or migration runner. This file creates roles only; it grants no
-- table privileges and sets no passwords. Credentials are provisioned by the
-- operator's secret store and rotated outside this file.
--
-- Idempotent: safe to re-run. Existing roles are left untouched.
DO $$
DECLARE
  role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY[
    'meshcore_migrate',
    'meshcore_ingest',
    'meshcore_api',
    'meshcore_owner',
    'meshcore_worker'
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT',
        role_name
      );
    END IF;
  END LOOP;
END
$$;

COMMENT ON ROLE meshcore_migrate IS 'DDL/owner role; migration runner only, never an application DSN';
COMMENT ON ROLE meshcore_ingest IS 'MQTT ingest/packet batch writer';
COMMENT ON ROLE meshcore_api IS 'public/owner API read path';
COMMENT ON ROLE meshcore_owner IS 'owner auth/ACL database role';
COMMENT ON ROLE meshcore_worker IS 'health/link/privacy/tagger worker role';

-- No passwords are set here. The operator must provision credentials via the
-- secret store and verify `rolpassword IS NOT NULL` without printing values.
-- No app role is a member of meshcore or pg_read_all_data.
