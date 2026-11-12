-- P1-3 reader-role bootstrap proposal.
-- 072_backend_grants.sql grants schema USAGE to these reader roles and fails
-- closed if they do not exist. They are NOLOGIN stubs until a reader service
-- needs a credential; no passwords and no table grants are set here.
DO $$
DECLARE
  role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY[
    'corescope_ro',
    'ukmesh_planner_reader',
    'meshgremlin_ro'
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT',
        role_name
      );
    END IF;
  END LOOP;
END
$$;

COMMENT ON ROLE corescope_ro IS 'CoreScope analytics reader; NOLOGIN stub until credential provisioning';
COMMENT ON ROLE ukmesh_planner_reader IS 'Planner analytics reader; NOLOGIN stub until credential provisioning';
COMMENT ON ROLE meshgremlin_ro IS 'MeshGremlin analytics reader; NOLOGIN stub until credential provisioning';

-- No passwords, no memberships, no table grants.
