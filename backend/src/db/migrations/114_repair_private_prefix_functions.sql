-- 114_repair_private_prefix_functions.sql
-- 113 moved private_node_prefixes; repoint SQL functions to core.

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

  DELETE FROM core.private_node_prefixes WHERE node_id = changed_node_id;

  IF TG_OP <> 'DELETE' AND COALESCE(NEW.name, '') LIKE '%🚫%' THEN
    INSERT INTO core.private_node_prefixes (node_id, network, prefix_size_bytes, prefix)
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

CREATE OR REPLACE FUNCTION public.classify_packet_privacy()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  matches_private BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM core.private_node_prefixes pp
    WHERE (
        pp.network = NEW.network
        OR (
          pp.network IN ('ukmesh', 'northeast', 'teesside')
          AND NEW.network IN ('ukmesh', 'northeast', 'teesside')
        )
      )
      AND (
        pp.node_id IN (NEW.rx_node_id, NEW.src_node_id)
        OR (
          NEW.path_hash_size_bytes = pp.prefix_size_bytes
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(NEW.path_hashes, ARRAY[]::text[])) AS packet_prefix
            WHERE UPPER(packet_prefix) = pp.prefix
          )
        )
      )
  ) INTO matches_private;

  NEW.is_private := matches_private;
  NEW.visibility_ok := (
    (COALESCE(cardinality(NEW.path_hashes), 0) = 0 OR NEW.path_hash_size_bytes BETWEEN 1 AND 3)
    AND NOT EXISTS (
      SELECT 1
      FROM unnest(COALESCE(NEW.path_hashes, ARRAY[]::text[])) AS path_hash
      WHERE path_hash IS NULL
         OR length(path_hash) <> NEW.path_hash_size_bytes * 2
         OR path_hash !~ '^[0-9A-Fa-f]+$'
    )
    AND matches_private IS NOT TRUE
  );
  RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.meshcore_path_matches_private(input_network text, input_rx_node_id text, input_src_node_id text, input_path_hashes text[], input_path_hash_size_bytes integer)
 RETURNS boolean
 LANGUAGE sql
 STABLE PARALLEL SAFE COST 1
 SET search_path TO 'public'
AS $function$
  SELECT EXISTS (
    SELECT 1
      FROM core.private_node_prefixes prefix
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
$function$;
