-- 051: make the reset-rebuilt database match the reviewed live invariants.
--
-- This migration deliberately owns only reproducible schema/policy state. It
-- does not assume that the live-only identity materializations already exist.

SET LOCAL lock_timeout = '30s';
SET LOCAL statement_timeout = '0';

-- ---------------------------------------------------------------------------
-- Identity materializations. On the live database relkind=m is preserved; on
-- a fresh database the ordinary views from 036/037 are replaced and populated
-- (including the valid zero-row state) before their refresh indexes are made.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  relation_kind "char";
BEGIN
  SELECT relkind INTO relation_kind
    FROM pg_class
   WHERE oid = to_regclass('public.node_identity_sightings');
  IF relation_kind = 'v' THEN
    DROP VIEW public.node_identity_sightings;
  ELSIF relation_kind IS NOT NULL AND relation_kind <> 'm' THEN
    RAISE EXCEPTION 'node_identity_sightings has unexpected relkind %', relation_kind;
  END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS node_identity_sightings AS
SELECT COALESCE(alias.canonical_node_id, UPPER(BTRIM(sighting.node_id))) AS node_id,
       sighting.network,
       MIN(sighting.first_seen_at) AS first_seen_at,
       MAX(sighting.last_seen_at) AS last_seen_at
  FROM node_network_sightings sighting
  LEFT JOIN node_identity_aliases alias
    ON alias.source_node_id = UPPER(BTRIM(sighting.node_id))
 GROUP BY COALESCE(alias.canonical_node_id, UPPER(BTRIM(sighting.node_id))), sighting.network;

CREATE UNIQUE INDEX IF NOT EXISTS node_identity_sightings_node_network_uidx
  ON node_identity_sightings (node_id, network);
CREATE INDEX IF NOT EXISTS node_identity_sightings_network_node_idx
  ON node_identity_sightings (network, node_id);

DO $$
DECLARE
  relation_kind "char";
BEGIN
  SELECT relkind INTO relation_kind
    FROM pg_class
   WHERE oid = to_regclass('public.node_identity_links');
  IF relation_kind = 'v' THEN
    DROP VIEW public.node_identity_links;
  ELSIF relation_kind IS NOT NULL AND relation_kind <> 'm' THEN
    RAISE EXCEPTION 'node_identity_links has unexpected relkind %', relation_kind;
  END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS node_identity_links AS
WITH mapped AS (
  SELECT COALESCE(a_alias.canonical_node_id, UPPER(BTRIM(link.node_a_id))) AS raw_a,
         COALESCE(b_alias.canonical_node_id, UPPER(BTRIM(link.node_b_id))) AS raw_b,
         link.observed_count,
         link.last_observed,
         link.itm_path_loss_db,
         link.itm_viable,
         link.itm_computed_at,
         link.count_a_to_b,
         link.count_b_to_a,
         link.force_viable,
         link.multibyte_observed_count
    FROM node_links link
    LEFT JOIN node_identity_aliases a_alias
      ON a_alias.source_node_id = UPPER(BTRIM(link.node_a_id))
    LEFT JOIN node_identity_aliases b_alias
      ON b_alias.source_node_id = UPPER(BTRIM(link.node_b_id))
   WHERE link.node_a_id IS NOT NULL AND link.node_b_id IS NOT NULL
), oriented AS (
  SELECT LEAST(raw_a, raw_b) AS node_a_id,
         GREATEST(raw_a, raw_b) AS node_b_id,
         observed_count,
         last_observed,
         itm_path_loss_db,
         itm_viable,
         itm_computed_at,
         CASE WHEN raw_a <= raw_b THEN count_a_to_b ELSE count_b_to_a END AS count_a_to_b,
         CASE WHEN raw_a <= raw_b THEN count_b_to_a ELSE count_a_to_b END AS count_b_to_a,
         force_viable,
         multibyte_observed_count
    FROM mapped
   WHERE raw_a <> raw_b
)
SELECT node_a_id,
       node_b_id,
       SUM(observed_count)::integer AS observed_count,
       MAX(last_observed) AS last_observed,
       MIN(itm_path_loss_db) AS itm_path_loss_db,
       BOOL_OR(itm_viable) AS itm_viable,
       MAX(itm_computed_at) AS itm_computed_at,
       SUM(count_a_to_b)::integer AS count_a_to_b,
       SUM(count_b_to_a)::integer AS count_b_to_a,
       BOOL_OR(force_viable) AS force_viable,
       SUM(multibyte_observed_count)::integer AS multibyte_observed_count
  FROM oriented
 GROUP BY node_a_id, node_b_id;

CREATE UNIQUE INDEX IF NOT EXISTS node_identity_links_pair_uidx
  ON node_identity_links (node_a_id, node_b_id);
CREATE INDEX IF NOT EXISTS node_identity_links_node_a_idx
  ON node_identity_links (node_a_id);
CREATE INDEX IF NOT EXISTS node_identity_links_node_b_idx
  ON node_identity_links (node_b_id);

-- ---------------------------------------------------------------------------
-- Durable packet-path identity and nullable-observer idempotency.
-- ---------------------------------------------------------------------------
ALTER TABLE packet_paths DROP CONSTRAINT IF EXISTS packet_paths_pkey;
ALTER TABLE packet_paths ALTER COLUMN rx_node_id DROP NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS packet_paths_observation_key_uidx
  ON packet_paths (time, packet_hash, network, COALESCE(rx_node_id, ''), topic);

UPDATE packet_paths path
   SET observation_id = packet.observation_id
  FROM (
    SELECT DISTINCT ON (time, packet_hash, network, COALESCE(rx_node_id, ''), topic)
           time, packet_hash, network, rx_node_id, topic, observation_id
      FROM packets
     WHERE time >= NOW() - INTERVAL '31 days'
       AND path_hashes IS NOT NULL
       AND path_hash_size_bytes > 1
       AND observation_id IS NOT NULL
     ORDER BY time, packet_hash, network, COALESCE(rx_node_id, ''), topic
  ) packet
 WHERE path.time >= NOW() - INTERVAL '31 days'
   AND path.time = packet.time
   AND path.packet_hash = packet.packet_hash
   AND path.network = packet.network
   AND path.rx_node_id IS NOT DISTINCT FROM packet.rx_node_id
   AND path.topic = packet.topic
   AND path.observation_id IS DISTINCT FROM packet.observation_id;

-- ---------------------------------------------------------------------------
-- Packet-path privacy is always derivable from the current prefix tables.
-- Stored flags remain as an ingest/index cache, so DB triggers maintain them
-- too; application derivations use the current-state helper rather than trust
-- indefinitely retained bits.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION meshcore_path_matches_private(
  input_network TEXT,
  input_rx_node_id TEXT,
  input_src_node_id TEXT,
  input_path_hashes TEXT[],
  input_path_hash_size_bytes INTEGER
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
PARALLEL SAFE
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1
      FROM private_node_prefixes prefix
     WHERE (
         prefix.network = input_network
         OR (
           prefix.network IN ('ukmesh', 'northeast', 'teesside')
           AND input_network IN ('ukmesh', 'northeast', 'teesside')
         )
       )
       AND (
         prefix.node_id IN (input_rx_node_id, input_src_node_id)
         OR (
           input_path_hash_size_bytes = prefix.prefix_size_bytes
           AND UPPER(prefix.prefix) = ANY(COALESCE(input_path_hashes, ARRAY[]::text[]))
         )
       )
  )
$$;

CREATE OR REPLACE FUNCTION meshcore_path_is_valid(
  input_path_hashes TEXT[],
  input_path_hash_size_bytes INTEGER
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT (
    COALESCE(CARDINALITY(input_path_hashes), 0) = 0
    OR input_path_hash_size_bytes BETWEEN 1 AND 3
  ) AND NOT EXISTS (
    SELECT 1
      FROM UNNEST(COALESCE(input_path_hashes, ARRAY[]::text[])) path_hash
     WHERE path_hash IS NULL
        OR LENGTH(path_hash) <> input_path_hash_size_bytes * 2
        OR path_hash !~ '^[0-9A-Fa-f]+$'
  )
$$;

CREATE OR REPLACE FUNCTION classify_packet_path_privacy()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.is_private := meshcore_path_matches_private(
    NEW.network, NEW.rx_node_id, NEW.src_node_id,
    NEW.path_hashes, NEW.path_hash_size_bytes
  );
  NEW.visibility_ok := meshcore_path_is_valid(
    NEW.path_hashes, NEW.path_hash_size_bytes
  ) AND NOT NEW.is_private;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS packet_paths_classify_privacy ON packet_paths;
CREATE TRIGGER packet_paths_classify_privacy
BEFORE INSERT OR UPDATE OF rx_node_id, src_node_id, path_hashes,
  path_hash_size_bytes, network ON packet_paths
FOR EACH ROW
EXECUTE FUNCTION classify_packet_path_privacy();

CREATE OR REPLACE FUNCTION rematerialize_packet_path_privacy()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
  old_private BOOLEAN := TG_OP <> 'INSERT' AND COALESCE(OLD.name, '') LIKE '%🚫%';
  new_private BOOLEAN := TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%';
BEGIN
  IF old_private IS DISTINCT FROM new_private THEN
    UPDATE packet_paths path
       SET is_private = meshcore_path_matches_private(
             path.network, path.rx_node_id, path.src_node_id,
             path.path_hashes, path.path_hash_size_bytes
           ),
           visibility_ok = meshcore_path_is_valid(
             path.path_hashes, path.path_hash_size_bytes
           ) AND NOT meshcore_path_matches_private(
             path.network, path.rx_node_id, path.src_node_id,
             path.path_hashes, path.path_hash_size_bytes
           )
     WHERE path.rx_node_id = changed_node_id
        OR path.src_node_id = changed_node_id
        OR (
          (
            path.network = changed_network
            OR (
              path.network IN ('ukmesh', 'northeast', 'teesside')
              AND changed_network IN ('ukmesh', 'northeast', 'teesside')
            )
          )
          AND path.path_hashes && ARRAY[
            UPPER(LEFT(changed_node_id, 2)),
            UPPER(LEFT(changed_node_id, 4)),
            UPPER(LEFT(changed_node_id, 6))
          ]::text[]
        );
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nodes_private_zz_packet_path_materialization ON nodes;
CREATE TRIGGER nodes_private_zz_packet_path_materialization
AFTER INSERT OR UPDATE OR DELETE ON nodes
FOR EACH ROW
EXECUTE FUNCTION rematerialize_packet_path_privacy();

CREATE INDEX IF NOT EXISTS packet_paths_rx_privacy_idx
  ON packet_paths (rx_node_id, time DESC) WHERE rx_node_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS packet_paths_src_privacy_idx
  ON packet_paths (src_node_id, time DESC) WHERE src_node_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS packet_paths_path_prefix_privacy_idx
  ON packet_paths USING GIN (path_hashes) WHERE path_hashes IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Reproducible Timescale capacity policy. Preserve the reviewed live 14-day
-- compression window for packets/status, apply it to indefinite packet paths,
-- and compress seven-day neighbor samples after one day. packet_paths never
-- receives a retention job.
-- ---------------------------------------------------------------------------
SELECT set_chunk_time_interval('packets', INTERVAL '1 day');

ALTER TABLE packets SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'network',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT remove_compression_policy('packets', if_exists => TRUE);
SELECT add_compression_policy('packets', INTERVAL '14 days', if_not_exists => TRUE);

ALTER TABLE packet_paths SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'network',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT remove_compression_policy('packet_paths', if_exists => TRUE);
SELECT add_compression_policy('packet_paths', INTERVAL '14 days', if_not_exists => TRUE);

ALTER TABLE node_status_samples SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'network',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT remove_compression_policy('node_status_samples', if_exists => TRUE);
SELECT add_compression_policy('node_status_samples', INTERVAL '14 days', if_not_exists => TRUE);

ALTER TABLE node_neighbor_samples SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'network',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT remove_compression_policy('node_neighbor_samples', if_exists => TRUE);
SELECT add_compression_policy('node_neighbor_samples', INTERVAL '1 day', if_not_exists => TRUE);

-- Bounded row-retention indexes (the worker orders by timestamp + ctid).
CREATE INDEX IF NOT EXISTS packet_decryptions_created_at_idx
  ON packet_decryptions (created_at);
CREATE INDEX IF NOT EXISTS observer_registration_terminal_updated_at_idx
  ON observer_registration_requests (updated_at, id)
  WHERE status IN ('rejected', 'expired', 'provisioned');
CREATE INDEX IF NOT EXISTS operator_audit_events_created_at_idx
  ON operator_audit_events (created_at, id);
