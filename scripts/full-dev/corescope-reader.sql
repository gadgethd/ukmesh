-- Apply with the fixture migration owner through psql. A dedicated CoreScope
-- backend reads public content and the four-column policy needed to revoke
-- its SQLite copies; it receives no raw table or Redis credential.
\if :{?corescope_reader_role}
\else
\set corescope_reader_role corescope_ro
\endif

BEGIN;
SELECT pg_catalog.set_config('ukmesh.corescope_reader_role', :'corescope_reader_role', true);
DO $guard$
DECLARE
  reader pg_catalog.pg_roles%ROWTYPE;
  target regclass;
  marker text;
BEGIN
  IF current_database() !~ '^lean_(verify|fixture|1x|10x|soak)_[a-z0-9_]{1,38}$' THEN
    RAISE EXCEPTION 'CoreScope reader provisioning requires an owned lean fixture database';
  END IF;
  SELECT shobj_description(oid, 'pg_database') INTO marker
    FROM pg_database WHERE datname = current_database();
  IF marker IS NULL OR marker !~ '^ukmesh-owned-fixture:[a-z0-9_]{1,38}$'
    OR right(current_database(), length(split_part(marker, ':', 2)) + 1)
       <> '_' || split_part(marker, ':', 2) THEN
    RAISE EXCEPTION 'CoreScope reader fixture ownership marker is missing or mismatched';
  END IF;
  SELECT * INTO reader FROM pg_catalog.pg_roles
    WHERE rolname = current_setting('ukmesh.corescope_reader_role');
  IF NOT FOUND OR reader.rolsuper OR reader.rolcreatedb OR reader.rolcreaterole
      OR reader.rolreplication OR reader.rolbypassrls OR reader.rolinherit THEN
    RAISE EXCEPTION 'CoreScope reader must be an existing unprivileged NOINHERIT role';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member = reader.oid) THEN
    RAISE EXCEPTION 'CoreScope reader must not inherit or assume other roles';
  END IF;
  FOREACH target IN ARRAY ARRAY[
    to_regclass('mesh.packets'), to_regclass('core.nodes'),
    to_regclass('core.private_node_prefixes'), to_regclass('mesh.ingest_observation_ledger'),
    to_regclass('public.packets'), to_regclass('public.nodes'),
    to_regclass('public.private_node_prefixes')
  ] LOOP
    IF target IS NOT NULL AND has_any_column_privilege(reader.oid, target, 'SELECT') THEN
      RAISE EXCEPTION 'CoreScope reader has raw relation privileges; provision a dedicated role';
    END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_namespace
    WHERE nspname = 'corescope_public' AND nspowner = reader.oid) THEN
    RAISE EXCEPTION 'CoreScope reader cannot own its projection schema';
  END IF;
END
$guard$;

CREATE SCHEMA IF NOT EXISTS corescope_public;
REVOKE ALL ON SCHEMA corescope_public FROM PUBLIC;

CREATE OR REPLACE FUNCTION corescope_public.privacy_ready()
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $ready$
  SELECT EXISTS (
    SELECT 1 FROM ops.public_visibility_state visibility
    JOIN ops.packet_visibility_materialization_state materialized
      ON materialized.singleton = visibility.singleton
    WHERE visibility.singleton = TRUE AND visibility.generation > 0
      AND materialized.visibility_generation = visibility.generation
  ) AND EXISTS (
    SELECT 1 FROM mesh.ingest_observation_ledger_coverage
    WHERE singleton = TRUE AND version = 1
  ) AND NOT EXISTS (
    SELECT 1 FROM ops.privacy_rematerialization_queue
    WHERE status IN ('pending', 'processing', 'failed')
  )
$ready$;

CREATE OR REPLACE FUNCTION corescope_public.meshcore_path_is_valid(
  path_hashes text[], path_hash_size_bytes integer
) RETURNS boolean LANGUAGE sql IMMUTABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $valid$
  SELECT public.meshcore_path_is_valid(path_hashes, path_hash_size_bytes)
$valid$;

CREATE OR REPLACE FUNCTION corescope_public.meshcore_path_matches_private(
  network text, rx_node_id text, src_node_id text,
  path_hashes text[], path_hash_size_bytes integer
) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $private$
  SELECT public.meshcore_path_matches_private(
    network, rx_node_id, src_node_id, path_hashes, path_hash_size_bytes
  )
$private$;

CREATE OR REPLACE VIEW corescope_public.packets WITH (security_barrier = true) AS
SELECT p.time, p.topic, p.raw_hex, p.snr, p.rssi,
       pg_catalog.jsonb_build_object('direction', p.payload->>'direction') AS payload,
       p.observation_id, p.network, p.rx_node_id, p.src_node_id,
       p.path_hashes, p.path_hash_size_bytes
FROM mesh.packets p
WHERE (SELECT corescope_public.privacy_ready())
  AND p.visibility_ok IS TRUE AND p.is_private IS NOT TRUE
  AND corescope_public.meshcore_path_is_valid(p.path_hashes, p.path_hash_size_bytes)
  AND NOT corescope_public.meshcore_path_matches_private(
    p.network, p.rx_node_id, p.src_node_id, p.path_hashes, p.path_hash_size_bytes
  );

CREATE OR REPLACE VIEW corescope_public.nodes WITH (security_barrier = true) AS
SELECT n.public_key, n.name
FROM core.nodes n
WHERE (SELECT corescope_public.privacy_ready())
  AND COALESCE(n.name, '') NOT LIKE '%🚫%'
  AND NOT EXISTS (
    SELECT 1 FROM core.private_node_prefixes prefix
    WHERE prefix.node_id IN (n.node_id, n.public_key)
  );

CREATE OR REPLACE VIEW corescope_public.ingest_observation_ledger
WITH (security_barrier = true) AS
SELECT ledger.seq, ledger.observation_id, ledger.observed_at, ledger.network
FROM mesh.ingest_observation_ledger ledger
WHERE EXISTS (
  SELECT 1 FROM corescope_public.packets packet
  WHERE packet.observation_id = ledger.observation_id
    AND packet.time = ledger.observed_at
);

CREATE OR REPLACE VIEW corescope_public.ingest_observation_ledger_coverage AS
SELECT singleton, version FROM mesh.ingest_observation_ledger_coverage;
CREATE OR REPLACE VIEW corescope_public.public_visibility_state AS
SELECT singleton, generation FROM ops.public_visibility_state;
CREATE OR REPLACE VIEW corescope_public.packet_visibility_materialization_state AS
SELECT singleton, visibility_generation FROM ops.packet_visibility_materialization_state;
CREATE OR REPLACE VIEW corescope_public.privacy_rematerialization_queue AS
SELECT DISTINCT status FROM ops.privacy_rematerialization_queue
WHERE status IN ('pending', 'processing', 'failed');

-- These exact four policy columns are required by loadCorePublicPolicy to
-- remove previously public observations from its private replay journal.
-- They are backend revocation metadata, never an HTTP/public-content export.
CREATE OR REPLACE VIEW corescope_public.private_node_prefixes AS
SELECT node_id, network, prefix_size_bytes, prefix FROM core.private_node_prefixes;

REVOKE ALL ON FUNCTION corescope_public.privacy_ready() FROM PUBLIC;
REVOKE ALL ON FUNCTION corescope_public.meshcore_path_is_valid(text[], integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION corescope_public.meshcore_path_matches_private(text, text, text, text[], integer) FROM PUBLIC;
GRANT USAGE ON SCHEMA corescope_public TO :"corescope_reader_role";
GRANT SELECT ON corescope_public.packets, corescope_public.nodes,
  corescope_public.ingest_observation_ledger,
  corescope_public.ingest_observation_ledger_coverage,
  corescope_public.public_visibility_state,
  corescope_public.packet_visibility_materialization_state,
  corescope_public.privacy_rematerialization_queue,
  corescope_public.private_node_prefixes TO :"corescope_reader_role";
GRANT EXECUTE ON FUNCTION corescope_public.privacy_ready(),
  corescope_public.meshcore_path_is_valid(text[], integer),
  corescope_public.meshcore_path_matches_private(text, text, text, text[], integer)
  TO :"corescope_reader_role";
SELECT pg_catalog.format(
  'ALTER ROLE %I IN DATABASE %I SET search_path = pg_catalog, corescope_public',
  :'corescope_reader_role', current_database()
) \gexec
COMMIT;
