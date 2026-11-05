-- 132_restore_reader_schema_usage.sql
-- The P1 schema moves (108-129) relocated relations with ALTER TABLE SET SCHEMA.
-- Table ACLs follow the relation, but schema USAGE does not: external readers
-- that previously read public.packets / public.nodes kept SELECT on the moved
-- tables while losing the ability to reach them. MeshGremlin (meshgremlin_ro)
-- reads the unqualified names packets, nodes, packet_decryptions and
-- message_tags, so restore schema USAGE and pin its search_path to the
-- canonical schemas. public stays last for any legacy compatibility view.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'meshgremlin_ro') THEN
    EXECUTE 'GRANT USAGE ON SCHEMA core, mesh TO meshgremlin_ro';
    EXECUTE 'GRANT SELECT ON core.nodes, mesh.packets, mesh.packet_decryptions, mesh.message_tags TO meshgremlin_ro';
    EXECUTE format(
      'ALTER ROLE meshgremlin_ro IN DATABASE %I SET search_path = core, mesh, public',
      current_database()
    );
  END IF;
END
$$;
