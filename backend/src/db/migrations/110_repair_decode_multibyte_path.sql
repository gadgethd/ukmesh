-- 110_repair_decode_multibyte_path.sql
-- 109 moved node_identity_aliases; pin the SQL function to core.

CREATE OR REPLACE FUNCTION public.meshcore_decode_multibyte_path(input_rx_node_id text, input_hop_count integer, input_path_hash_size_bytes integer, input_path_hashes text[])
 RETURNS TABLE(fully_decoded boolean, decoded_hops smallint, decoded_path text, decoded_node_ids text[])
 LANGUAGE sql
 STABLE PARALLEL SAFE
 SET search_path TO 'public'
AS $function$
  WITH receiver AS (
    SELECT rx.role,
           COALESCE(alias.canonical_node_id, upper(btrim(input_rx_node_id))) AS node_id
      FROM (VALUES (1)) singleton(value)
      LEFT JOIN core.node_identity_aliases alias
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
$function$
;
