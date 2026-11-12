-- 111_repair_canonical_node_id.sql
-- 109 moved node_identity_aliases; pin the canonical resolver to core.

CREATE OR REPLACE FUNCTION public.meshcore_canonical_node_id(input_node_id text)
 RETURNS text
 LANGUAGE sql
 STABLE PARALLEL SAFE
 SET search_path TO 'public'
AS $function$
  SELECT CASE
    WHEN input_node_id IS NULL OR btrim(input_node_id) = '' THEN input_node_id
    ELSE COALESCE(
      (
        SELECT alias.canonical_node_id
        FROM core.node_identity_aliases alias
        WHERE alias.source_node_id = upper(btrim(input_node_id))
      ),
      upper(btrim(input_node_id))
    )
  END
$function$
;
