-- 087_move_privacy_visibility_cluster.sql
-- P1-4 ops domain: privacy/visibility cluster.
-- Replaces the six trigger functions and the planner view with ops-qualified
-- references, then moves all three tables in one transaction.
--
-- The six functions are redefined identically in every database: they only
-- resolve `ops.*` at run time, so they do not depend on the moves below having
-- happened first. The moves themselves are guarded rather than assumed, and the
-- `planner_export` schema the view lives in is created here because nothing in
-- this tree ever provisioned it.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.bump_visibility_for_changed_prefixes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- Node-trigger maintenance is covered by the asynchronous queue in 056.
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;

  IF TG_OP = 'INSERT' THEN
    IF NOT EXISTS (SELECT 1 FROM new_prefix_rows) THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'DELETE' THEN
    IF NOT EXISTS (SELECT 1 FROM old_prefix_rows) THEN RETURN NULL; END IF;
  ELSIF TG_OP = 'UPDATE' THEN
    IF NOT EXISTS (SELECT 1 FROM new_prefix_rows) THEN RETURN NULL; END IF;
  END IF;
  -- TRUNCATE has no transition table; conservatively fence it, even if empty.
  UPDATE ops.public_visibility_state
     SET generation = generation + 1, updated_at = NOW()
   WHERE singleton = TRUE;
  RETURN NULL;
END;
$function$;

CREATE OR REPLACE FUNCTION public.bump_visibility_for_identity_table()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  previous_generation BIGINT;
BEGIN
  IF TG_TABLE_NAME = 'private_node_prefixes' AND pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;
  SELECT generation INTO STRICT previous_generation
    FROM ops.public_visibility_state WHERE singleton = TRUE FOR UPDATE;
  UPDATE ops.public_visibility_state
     SET generation = generation + 1, updated_at = NOW()
   WHERE singleton = TRUE;
  IF TG_TABLE_NAME = 'node_identity_aliases' THEN
    UPDATE ops.packet_visibility_materialization_state
       SET visibility_generation = previous_generation + 1, updated_at = NOW()
     WHERE singleton = TRUE AND visibility_generation = previous_generation;
  END IF;
  RETURN NULL;
END;
$function$;

CREATE OR REPLACE FUNCTION public.lock_packet_visibility_for_node_privacy_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  old_private BOOLEAN := TG_OP <> 'INSERT' AND COALESCE(OLD.name, '') LIKE '%🚫%';
  new_private BOOLEAN := TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%';
  privacy_scope_changed BOOLEAN;
BEGIN
  privacy_scope_changed := old_private IS DISTINCT FROM new_private
    OR ((old_private OR new_private)
      AND TG_OP = 'UPDATE'
      AND OLD.network IS DISTINCT FROM NEW.network);
  IF privacy_scope_changed THEN
    PERFORM generation
      FROM ops.public_visibility_state
     WHERE singleton = TRUE
     FOR UPDATE;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$function$;

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

  INSERT INTO public.multibyte_path_facts (
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

CREATE OR REPLACE FUNCTION public.rematerialize_packet_path_privacy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
  old_private BOOLEAN := TG_OP <> 'INSERT' AND COALESCE(OLD.name, '') LIKE '%🚫%';
  new_private BOOLEAN := TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%';
BEGIN
  IF old_private IS DISTINCT FROM new_private THEN
    INSERT INTO ops.privacy_rematerialization_queue AS q (node_id, network, target)
    VALUES (changed_node_id, changed_network, 'packet_paths')
    ON CONFLICT (target, node_id) DO UPDATE SET
      requested_at = NOW(),
      network = EXCLUDED.network,
      status = CASE WHEN q.status = 'processing'
                    THEN 'processing' ELSE 'pending' END,
      attempts = CASE WHEN q.status = 'processing'
                      THEN q.attempts ELSE 0 END,
      finished_at = CASE WHEN q.status = 'processing'
                         THEN q.finished_at ELSE NULL END;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.sync_private_node_prefixes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  changed_node_id TEXT := COALESCE(NEW.node_id, OLD.node_id);
  changed_network TEXT := COALESCE(NEW.network, OLD.network, 'ukmesh');
  old_private BOOLEAN := FALSE;
  new_private BOOLEAN := FALSE;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    old_private := COALESCE(OLD.name, '') LIKE '%🚫%';
  END IF;
  IF TG_OP <> 'DELETE' THEN
    new_private := COALESCE(NEW.name, '') LIKE '%🚫%';
  END IF;

  IF TG_OP = 'UPDATE'
     AND OLD.name IS NOT DISTINCT FROM NEW.name
     AND OLD.network IS NOT DISTINCT FROM NEW.network THEN
    RETURN NEW;
  END IF;

  DELETE FROM public.private_node_prefixes WHERE node_id = changed_node_id;

  IF TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%' THEN
    INSERT INTO public.private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
    VALUES
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 1, UPPER(LEFT(NEW.node_id, 2))),
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 2, UPPER(LEFT(NEW.node_id, 4))),
      (NEW.node_id, COALESCE(NEW.network, 'ukmesh'), 3, UPPER(LEFT(NEW.node_id, 6)))
    ON CONFLICT (node_id, network, prefix_size_bytes) DO UPDATE SET
      prefix = EXCLUDED.prefix,
      updated_at = NOW();
  END IF;

  IF old_private IS DISTINCT FROM new_private THEN
    INSERT INTO ops.privacy_rematerialization_queue AS q (node_id, network, target)
    VALUES (changed_node_id, changed_network, 'packets')
    ON CONFLICT (target, node_id) DO UPDATE SET
      requested_at = NOW(),
      network = EXCLUDED.network,
      status = CASE WHEN q.status = 'processing'
                    THEN 'processing' ELSE 'pending' END,
      attempts = CASE WHEN q.status = 'processing'
                      THEN q.attempts ELSE 0 END,
      finished_at = CASE WHEN q.status = 'processing'
                         THEN q.finished_at ELSE NULL END;
  END IF;

  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$function$;

DO $$
BEGIN
  IF to_regclass('public.privacy_rematerialization_queue') IS NULL THEN
    RAISE NOTICE '087: public.privacy_rematerialization_queue is absent; move to ops skipped';
  ELSE
    ALTER TABLE public.privacy_rematerialization_queue SET SCHEMA ops;
  END IF;

  IF to_regclass('public.packet_visibility_materialization_state') IS NULL THEN
    RAISE NOTICE '087: public.packet_visibility_materialization_state is absent; move to ops skipped';
  ELSE
    ALTER TABLE public.packet_visibility_materialization_state SET SCHEMA ops;
  END IF;

  IF to_regclass('public.public_visibility_state') IS NULL THEN
    RAISE NOTICE '087: public.public_visibility_state is absent; move to ops skipped';
  ELSE
    ALTER TABLE public.public_visibility_state SET SCHEMA ops;
  END IF;
END
$$;

-- Nothing in this tree provisioned `planner_export`; the shipped view assumed
-- an operator had created it. A database rebuilt from the shipped base schema
-- only has `public`, so the export schema is created here. IF NOT EXISTS leaves
-- a provisioned schema, its owner and its grants untouched.
CREATE SCHEMA IF NOT EXISTS planner_export;

DO $$
BEGIN
  IF to_regclass('ops.public_visibility_state') IS NULL THEN
    RAISE NOTICE '087: ops.public_visibility_state is absent; planner export view skipped';
    RETURN;
  END IF;
  CREATE OR REPLACE VIEW planner_export.visibility_fence_v1 AS
  SELECT generation, updated_at, now() AS queried_at
    FROM ops.public_visibility_state
   WHERE singleton = true;
END
$$;
