-- Incremental, observation-row-keyed facts for the expensive multibyte charts.
-- Historical population is deliberately handled by the bounded backfill tool;
-- migrations must remain additive and must not scan the packets hypertable.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE packets
  ADD COLUMN IF NOT EXISTS observation_id UUID;

CREATE TABLE IF NOT EXISTS multibyte_path_facts (
  observation_id       UUID PRIMARY KEY,
  observed_at          TIMESTAMPTZ NOT NULL,
  packet_hash          TEXT NOT NULL,
  network              TEXT NOT NULL,
  rx_node_id           TEXT,
  src_node_id          TEXT,
  topic                TEXT NOT NULL,
  topic_prefix         TEXT NOT NULL DEFAULT '',
  path_hashes          TEXT[] NOT NULL,
  path_hash_size_bytes SMALLINT NOT NULL CHECK (path_hash_size_bytes BETWEEN 2 AND 3),
  visibility_ok        BOOLEAN NOT NULL,
  is_private           BOOLEAN NOT NULL,
  visibility_generation BIGINT NOT NULL CHECK (visibility_generation > 0),
  fully_decoded        BOOLEAN NOT NULL,
  decoded_hops         SMALLINT,
  decoded_path         TEXT,
  decoded_node_ids     TEXT[],
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (
    (fully_decoded AND decoded_hops >= 2 AND decoded_path IS NOT NULL AND decoded_node_ids IS NOT NULL)
    OR
    (NOT fully_decoded AND decoded_hops IS NULL AND decoded_path IS NULL AND decoded_node_ids IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS multibyte_path_facts_generation_network_time_idx
  ON multibyte_path_facts (visibility_generation, network, observed_at DESC);

CREATE INDEX IF NOT EXISTS multibyte_path_facts_generation_decoded_time_idx
  ON multibyte_path_facts (visibility_generation, observed_at DESC)
  WHERE fully_decoded IS TRUE;

CREATE TABLE IF NOT EXISTS multibyte_path_fact_state (
  singleton             BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  visibility_generation BIGINT NOT NULL,
  covered_from          TIMESTAMPTZ NOT NULL,
  covered_through       TIMESTAMPTZ NOT NULL,
  row_count             BIGINT NOT NULL DEFAULT 0,
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (covered_through >= covered_from)
);

CREATE OR REPLACE FUNCTION meshcore_decode_multibyte_path(
  input_rx_node_id TEXT,
  input_hop_count INTEGER,
  input_path_hash_size_bytes INTEGER,
  input_path_hashes TEXT[]
)
RETURNS TABLE (
  fully_decoded BOOLEAN,
  decoded_hops SMALLINT,
  decoded_path TEXT,
  decoded_node_ids TEXT[]
)
LANGUAGE SQL
STABLE
PARALLEL SAFE
SET search_path = public
AS $$
  WITH receiver AS (
    SELECT rx.role,
           COALESCE(alias.canonical_node_id, upper(btrim(input_rx_node_id))) AS node_id
      FROM (VALUES (1)) singleton(value)
      LEFT JOIN node_identity_aliases alias
        ON alias.source_node_id = upper(btrim(input_rx_node_id))
      LEFT JOIN node_identity_nodes rx
        ON rx.node_id = COALESCE(alias.canonical_node_id, upper(btrim(input_rx_node_id)))
  ), valid_hops AS (
    SELECT COALESCE(array_agg(upper(h.hash) ORDER BY h.ord), ARRAY[]::text[]) AS hashes
      FROM unnest(COALESCE(input_path_hashes, ARRAY[]::text[]))
        WITH ORDINALITY h(hash, ord)
     WHERE length(h.hash) = input_path_hash_size_bytes * 2
       AND (input_hop_count IS NULL OR h.ord <= GREATEST(input_hop_count, 0))
  ), trimmed AS (
    SELECT CASE
             WHEN receiver.role = 2
              AND cardinality(valid_hops.hashes) > 1
              AND receiver.node_id LIKE valid_hops.hashes[cardinality(valid_hops.hashes)] || '%'
               THEN valid_hops.hashes[1:cardinality(valid_hops.hashes) - 1]
             ELSE valid_hops.hashes
           END AS hashes
      FROM valid_hops
      CROSS JOIN receiver
  ), hop_matches AS (
    SELECT hop.ord,
           COUNT(node.node_id)::integer AS match_count,
           MIN(node.node_id) AS node_id
      FROM trimmed
      CROSS JOIN LATERAL unnest(trimmed.hashes) WITH ORDINALITY hop(hash, ord)
      LEFT JOIN node_identity_nodes node
        ON length(hop.hash) IN (4, 6)
       AND upper(left(node.node_id, length(hop.hash))) = hop.hash
       AND node.lat IS NOT NULL
       AND node.lon IS NOT NULL
       AND (node.role IS NULL OR node.role = 2)
     GROUP BY hop.ord
  ), evaluated AS (
    SELECT trimmed.hashes,
           COUNT(hop_matches.ord)::integer AS matched_hops,
           COALESCE(bool_and(hop_matches.match_count = 1), FALSE) AS every_hop_unique,
           COUNT(DISTINCT hop_matches.node_id)
             FILTER (WHERE hop_matches.match_count = 1)::integer AS distinct_nodes,
           array_agg(hop_matches.node_id ORDER BY hop_matches.ord)
             FILTER (WHERE hop_matches.match_count = 1) AS node_ids
      FROM trimmed
      LEFT JOIN hop_matches ON TRUE
     GROUP BY trimmed.hashes
  ), decoded AS (
    SELECT *,
           cardinality(hashes) >= 2
             AND matched_hops = cardinality(hashes)
             AND every_hop_unique
             AND distinct_nodes = cardinality(hashes) AS ok
      FROM evaluated
  )
  SELECT ok,
         CASE WHEN ok THEN cardinality(hashes)::smallint ELSE NULL END,
         CASE WHEN ok THEN (
           SELECT string_agg(upper(left(node_id, 6)), ' -> ' ORDER BY ord)
             FROM unnest(node_ids) WITH ORDINALITY ids(node_id, ord)
         ) ELSE NULL END,
         CASE WHEN ok THEN node_ids ELSE NULL END
    FROM decoded
$$;

CREATE OR REPLACE FUNCTION persist_multibyte_path_fact()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public
AS $$
DECLARE
  current_generation BIGINT;
  decoded RECORD;
BEGIN
  IF NEW.observation_id IS NULL THEN
    NEW.observation_id := gen_random_uuid();
  END IF;
  IF NEW.path_hash_size_bytes NOT BETWEEN 2 AND 3
     OR COALESCE(cardinality(NEW.path_hashes), 0) = 0 THEN
    RETURN NEW;
  END IF;

  SELECT generation INTO STRICT current_generation
    FROM public_visibility_state
   WHERE singleton = TRUE;
  SELECT * INTO STRICT decoded
    FROM meshcore_decode_multibyte_path(
      NEW.rx_node_id,
      NEW.hop_count,
      NEW.path_hash_size_bytes,
      NEW.path_hashes
    );

  INSERT INTO multibyte_path_facts (
    observation_id, observed_at, packet_hash, network, rx_node_id, src_node_id,
    topic, topic_prefix, path_hashes, path_hash_size_bytes,
    visibility_ok, is_private, visibility_generation,
    fully_decoded, decoded_hops, decoded_path, decoded_node_ids
  ) VALUES (
    NEW.observation_id, NEW.time, NEW.packet_hash, NEW.network, NEW.rx_node_id, NEW.src_node_id,
    NEW.topic, NEW.topic_prefix, NEW.path_hashes, NEW.path_hash_size_bytes,
    NEW.visibility_ok, NEW.is_private, current_generation,
    decoded.fully_decoded, decoded.decoded_hops, decoded.decoded_path, decoded.decoded_node_ids
  )
  ON CONFLICT (observation_id) DO UPDATE SET
    observed_at = EXCLUDED.observed_at,
    packet_hash = EXCLUDED.packet_hash,
    network = EXCLUDED.network,
    rx_node_id = EXCLUDED.rx_node_id,
    src_node_id = EXCLUDED.src_node_id,
    topic = EXCLUDED.topic,
    topic_prefix = EXCLUDED.topic_prefix,
    path_hashes = EXCLUDED.path_hashes,
    path_hash_size_bytes = EXCLUDED.path_hash_size_bytes,
    visibility_ok = EXCLUDED.visibility_ok,
    is_private = EXCLUDED.is_private,
    visibility_generation = EXCLUDED.visibility_generation,
    fully_decoded = EXCLUDED.fully_decoded,
    decoded_hops = EXCLUDED.decoded_hops,
    decoded_path = EXCLUDED.decoded_path,
    decoded_node_ids = EXCLUDED.decoded_node_ids,
    updated_at = NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS packets_multibyte_path_fact ON packets;
CREATE TRIGGER packets_multibyte_path_fact
BEFORE INSERT ON packets
FOR EACH ROW
EXECUTE FUNCTION persist_multibyte_path_fact();

-- Decoding and public visibility both depend on these identity inputs. Bump the
-- same transaction-fenced generation so stored facts and response caches fail
-- closed until the bounded backfill has rebuilt them.
CREATE OR REPLACE FUNCTION bump_public_visibility_generation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  old_private BOOLEAN := FALSE;
  new_private BOOLEAN := FALSE;
  decode_inputs_changed BOOLEAN := FALSE;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_private := COALESCE(OLD.name, '') LIKE '%🚫%';
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_private := COALESCE(NEW.name, '') LIKE '%🚫%';
  END IF;

  IF TG_OP IN ('INSERT', 'DELETE') THEN
    decode_inputs_changed := TRUE;
  ELSE
    decode_inputs_changed := OLD.node_id IS DISTINCT FROM NEW.node_id
      OR OLD.lat IS DISTINCT FROM NEW.lat
      OR OLD.lon IS DISTINCT FROM NEW.lon
      OR OLD.role IS DISTINCT FROM NEW.role;
  END IF;

  IF old_private IS DISTINCT FROM new_private OR decode_inputs_changed THEN
    UPDATE public_visibility_state
       SET generation = generation + 1,
           updated_at = NOW()
     WHERE singleton = TRUE;
  END IF;

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION bump_visibility_for_identity_table()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  -- private_node_prefixes is normally maintained by the nodes trigger. The
  -- outer nodes statement performs the single generation bump in that case;
  -- direct prefix maintenance must still invalidate atomically.
  IF TG_TABLE_NAME = 'private_node_prefixes' AND pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;
  UPDATE public_visibility_state
     SET generation = generation + 1,
         updated_at = NOW()
   WHERE singleton = TRUE;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS node_identity_aliases_visibility_generation ON node_identity_aliases;
CREATE TRIGGER node_identity_aliases_visibility_generation
AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON node_identity_aliases
FOR EACH STATEMENT
EXECUTE FUNCTION bump_visibility_for_identity_table();

DROP TRIGGER IF EXISTS private_node_prefixes_visibility_generation ON private_node_prefixes;
CREATE TRIGGER private_node_prefixes_visibility_generation
AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON private_node_prefixes
FOR EACH STATEMENT
EXECUTE FUNCTION bump_visibility_for_identity_table();
