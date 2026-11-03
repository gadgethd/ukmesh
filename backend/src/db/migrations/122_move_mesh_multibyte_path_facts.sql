-- 122_move_mesh_multibyte_path_facts.sql
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE public.multibyte_path_facts SET SCHEMA mesh;
ALTER TABLE public.multibyte_path_fact_state SET SCHEMA mesh;

CREATE OR REPLACE FUNCTION public.persist_multibyte_path_fact()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
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
    FROM ops.public_visibility_state
   WHERE singleton = TRUE;
  SELECT * INTO STRICT decoded
    FROM meshcore_decode_multibyte_path(
      NEW.rx_node_id,
      NEW.hop_count,
      NEW.path_hash_size_bytes,
      NEW.path_hashes
    );

  INSERT INTO mesh.multibyte_path_facts (
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
$function$;
