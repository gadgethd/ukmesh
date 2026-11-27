-- Render one classifier into the INSERT trigger and the maintenance function.
-- Calling a table-returning SQL function from every ingest row was materially
-- slower than the deployed direct trigger query on the lean VM.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $migration$
DECLARE
  classifier constant text := $classifier$
    SELECT EXISTS (
      SELECT 1
        FROM core.private_node_prefixes AS prefix
       WHERE (prefix.network = %1$s OR (
                prefix.network IN ('ukmesh', 'northeast', 'teesside')
                AND %1$s IN ('ukmesh', 'northeast', 'teesside')
             ))
         AND (prefix.node_id IN (%2$s, %3$s) OR (
                %5$s = prefix.prefix_size_bytes
                AND EXISTS (
                  SELECT 1 FROM unnest(COALESCE(%4$s, ARRAY[]::text[])) AS hop
                   WHERE UPPER(hop) = prefix.prefix
                )
             ))
    ) INTO matches_private;

    %6$s := matches_private;
    %7$s := (
      (COALESCE(cardinality(%4$s), 0) = 0 OR %5$s BETWEEN 1 AND 3)
      AND NOT EXISTS (
        SELECT 1 FROM unnest(COALESCE(%4$s, ARRAY[]::text[])) AS hop
         WHERE hop IS NULL OR length(hop) <> %5$s * 2 OR hop !~ '^[0-9A-Fa-f]+$'
      )
      AND matches_private IS NOT TRUE
    );
  $classifier$;
BEGIN
  EXECUTE format($definition$
    CREATE OR REPLACE FUNCTION public.classify_packet_privacy()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $function$
    DECLARE matches_private boolean;
    BEGIN
      %s
      RETURN NEW;
    END;
    $function$
  $definition$, format(classifier,
    'NEW.network', 'NEW.rx_node_id', 'NEW.src_node_id',
    'NEW.path_hashes', 'NEW.path_hash_size_bytes', 'NEW.is_private', 'NEW.visibility_ok'));

  EXECUTE format($definition$
    CREATE FUNCTION public.meshcore_packet_visibility(
      input_network text, input_rx_node_id text, input_src_node_id text,
      input_path_hashes text[], input_path_hash_size_bytes integer
    )
    RETURNS TABLE(is_private boolean, visibility_ok boolean)
    LANGUAGE plpgsql
    STABLE PARALLEL SAFE ROWS 1
    AS $function$
    DECLARE matches_private boolean;
    BEGIN
      %s
      RETURN NEXT;
    END;
    $function$
  $definition$, format(classifier,
    'input_network', 'input_rx_node_id', 'input_src_node_id',
    'input_path_hashes', 'input_path_hash_size_bytes', 'is_private', 'visibility_ok'));
END;
$migration$;
