-- Stage 1: an independent, restricted UKMesh history source for Beacon.
-- Apply only through the owned-fixture migration owner. This provisions no
-- Beacon rollout, MQTT replay, public HTTP route, or durable projection.
-- Packet hashes are different protocols: packet_uid/version is authoritative
-- UKMesh identity; packet_hash is a retained alias, never a Beacon hash.
\if :{?beacon_reader_role}
\else
\set beacon_reader_role beacon_ro
\endif

BEGIN;
SELECT pg_catalog.set_config('ukmesh.beacon_reader_role', :'beacon_reader_role', true);
DO $guard$
DECLARE
  reader pg_catalog.pg_roles%ROWTYPE;
  target regclass;
  marker text;
BEGIN
  IF current_database() !~ '^lean_(verify|fixture|1x|10x|soak)_[a-z0-9_]{1,38}$' THEN
    RAISE EXCEPTION 'Beacon reader provisioning requires an owned lean fixture database';
  END IF;
  SELECT shobj_description(oid, 'pg_database') INTO marker
    FROM pg_database WHERE datname = current_database();
  IF marker IS NULL OR marker !~ '^ukmesh-owned-fixture:[a-z0-9_]{1,38}$'
    OR right(current_database(), length(split_part(marker, ':', 2)) + 1)
       <> '_' || split_part(marker, ':', 2) THEN
    RAISE EXCEPTION 'Beacon reader fixture ownership marker is missing or mismatched';
  END IF;
  SELECT * INTO reader FROM pg_catalog.pg_roles
    WHERE rolname = current_setting('ukmesh.beacon_reader_role');
  IF NOT FOUND OR reader.rolsuper OR reader.rolcreatedb OR reader.rolcreaterole
      OR reader.rolreplication OR reader.rolbypassrls OR reader.rolinherit THEN
    RAISE EXCEPTION 'Beacon reader must be an existing unprivileged NOINHERIT role';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member = reader.oid) THEN
    RAISE EXCEPTION 'Beacon reader must not inherit or assume other roles';
  END IF;
  FOREACH target IN ARRAY ARRAY[
    to_regclass('mesh.packets'), to_regclass('mesh.ingest_observation_ledger'),
    to_regclass('mesh.packet_identity_aliases'), to_regclass('core.nodes'),
    to_regclass('core.private_node_prefixes'), to_regclass('public.packets'),
    to_regclass('public.nodes'), to_regclass('public.private_node_prefixes')
  ] LOOP
    IF target IS NOT NULL AND has_any_column_privilege(reader.oid, target, 'SELECT') THEN
      RAISE EXCEPTION 'Beacon reader has raw relation privileges; provision a dedicated role';
    END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_namespace
    WHERE nspname = 'beacon_public' AND nspowner = reader.oid) THEN
    RAISE EXCEPTION 'Beacon reader cannot own its source schema';
  END IF;
END
$guard$;

CREATE SCHEMA IF NOT EXISTS beacon_public;
REVOKE ALL ON SCHEMA beacon_public FROM PUBLIC;

CREATE OR REPLACE FUNCTION beacon_public.privacy_ready()
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

-- The reader can execute this function, so bind it to an existing observation
-- and return the same result for hidden and unknown observations. Arbitrary
-- keys and paths must never become queries against the private prefix list.
CREATE OR REPLACE FUNCTION beacon_public.is_public_observation(
  input_id uuid, input_time timestamptz
) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $visible$
  SELECT beacon_public.privacy_ready() AND EXISTS (
    SELECT 1 FROM mesh.packets p
    WHERE p.observation_id = input_id AND p.time = input_time
      AND p.packet_type = 5 AND p.visibility_ok IS TRUE AND p.is_private IS NOT TRUE
      AND public.meshcore_path_is_valid(p.path_hashes, p.path_hash_size_bytes)
      AND NOT public.meshcore_path_matches_private(
        p.network, p.rx_node_id, p.src_node_id, p.path_hashes, p.path_hash_size_bytes
      )
  )
$visible$;

-- This source is only for GroupText (packet_type 5), not general raw packets.
-- Consumers bind their network set and page by seq, never by observation time.
-- Existing ledger (network,seq) and packets (time,observation_id) indexes own
-- the lookup path; this source creates neither a materialization nor an index.
-- Retain public metadata for ineligible rows so missing raw/identity is an
-- explicit coverage outcome. No arbitrary JSON payload/decryption is exposed.
-- The consumer must decode raw, recompute both versioned identities and check
-- the UID before accepting it; candidate-for-decode is not that verification.
CREATE OR REPLACE VIEW beacon_public.packets WITH (security_barrier = true) AS
SELECT ledger.seq, p.observation_id, p.time, p.network, p.iata,
       p.packet_uid, p.identity_version, p.identity_provenance, p.packet_hash, p.packet_type,
       p.topic, p.rx_node_id, p.src_node_id, p.path_hashes, p.path_hash_size_bytes,
       p.rssi, p.snr, p.payload->>'direction' AS direction,
       eligibility.raw_input_status,
       CASE WHEN eligibility.raw_input_status = 'candidate-for-decode'
            THEN p.raw_hex ELSE NULL END AS raw_hex
FROM mesh.packets p
JOIN mesh.ingest_observation_ledger ledger
  ON ledger.observation_id = p.observation_id AND ledger.observed_at = p.time
 AND ledger.network = p.network
CROSS JOIN LATERAL (
  SELECT CASE
    WHEN p.packet_uid IS NULL THEN 'missing-uid'
    WHEN octet_length(p.packet_uid) <> 32 THEN 'invalid-uid'
    WHEN p.identity_version IS DISTINCT FROM 'wire-sha256-v1' THEN 'unsupported-identity-version'
    WHEN p.rx_node_id IS NULL OR p.rx_node_id !~ '^[0-9A-Fa-f]{64}$' THEN 'invalid-observer-key'
    WHEN p.raw_hex IS NULL OR btrim(p.raw_hex) = '' THEN 'missing-raw'
    WHEN length(btrim(p.raw_hex)) % 2 <> 0
      OR btrim(p.raw_hex) !~ '^[0-9A-Fa-f]+$' THEN 'invalid-raw-hex'
    ELSE 'candidate-for-decode'
  END AS raw_input_status
) eligibility
WHERE (SELECT beacon_public.privacy_ready())
  AND p.packet_type = 5
  AND p.visibility_ok IS TRUE AND p.is_private IS NOT TRUE
  AND beacon_public.is_public_observation(p.observation_id, p.time);

CREATE OR REPLACE VIEW beacon_public.ingest_observation_ledger
WITH (security_barrier = true) AS
SELECT seq, observation_id, time AS observed_at, network FROM beacon_public.packets;

-- A caller must refuse an unavailable source rather than certify empty history.
CREATE OR REPLACE VIEW beacon_public.source_state AS
SELECT 1::integer AS source_contract_version,
       (SELECT generation FROM ops.public_visibility_state WHERE singleton = TRUE) AS generation,
       (SELECT visibility_generation FROM ops.packet_visibility_materialization_state
         WHERE singleton = TRUE) AS visibility_generation,
       (SELECT version FROM mesh.ingest_observation_ledger_coverage
         WHERE singleton = TRUE) AS ledger_coverage_version,
       beacon_public.privacy_ready() AS ready;

REVOKE ALL ON FUNCTION beacon_public.privacy_ready() FROM PUBLIC;
REVOKE ALL ON FUNCTION beacon_public.is_public_observation(uuid, timestamptz) FROM PUBLIC;
REVOKE ALL ON beacon_public.packets, beacon_public.ingest_observation_ledger,
  beacon_public.source_state FROM PUBLIC;
GRANT USAGE ON SCHEMA beacon_public TO :"beacon_reader_role";
GRANT SELECT ON beacon_public.packets, beacon_public.ingest_observation_ledger,
  beacon_public.source_state TO :"beacon_reader_role";
GRANT EXECUTE ON FUNCTION beacon_public.privacy_ready(),
  beacon_public.is_public_observation(uuid, timestamptz)
  TO :"beacon_reader_role";
SELECT pg_catalog.format(
  'ALTER ROLE %I IN DATABASE %I SET search_path = pg_catalog, beacon_public',
  :'beacon_reader_role', current_database()
) \gexec
COMMIT;
