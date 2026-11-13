-- Reuse the canonical projection for point/prefix lookups, including the SQL
-- decoder used by ingest. Keep the existing global view and its row type.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE FUNCTION core.lookup_node_identity(input_node_id text)
RETURNS SETOF core.node_identity_nodes
LANGUAGE sql STABLE PARALLEL SAFE ROWS 1
SET search_path = pg_catalog, public
AS $function$
  SELECT input_node_id AS node_id, n.name, n.lat, n.lon, n.last_seen,
    bool_or(COALESCE(n.is_online,FALSE)) OVER () AS is_online,
    n.hardware_model,n.firmware_version,n.public_key,
    min(n.created_at) OVER () AS created_at,n.iata,n.role,
    (sum(COALESCE(n.advert_count,0)) OVER ())::integer AS advert_count,
    n.elevation_m,n.network,
    max(n.last_predicted_online_at) OVER () AS last_predicted_online_at,
    max(n.last_path_evidence_at) OVER () AS last_path_evidence_at,
    max(n.last_mqtt_observer_seen_at) OVER () AS last_mqtt_observer_seen_at,
    max(n.last_rx_at) OVER () AS last_rx_at,
    max(n.last_status_at) OVER () AS last_status_at,n.observer_iata,
    array_agg(n.node_id) OVER (ORDER BY n.node_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS identity_source_ids,
    (count(*) OVER ())::integer AS identity_member_count
  FROM core.nodes n LEFT JOIN core.node_identity_aliases actual_alias
    ON actual_alias.source_node_id=UPPER(BTRIM(n.node_id))
  WHERE UPPER(BTRIM(n.node_id))=ANY(ARRAY(
    SELECT input_node_id UNION SELECT a.source_node_id FROM core.node_identity_aliases a
    WHERE a.canonical_node_id=input_node_id
  ))
  AND COALESCE(actual_alias.canonical_node_id,UPPER(BTRIM(n.node_id)))=input_node_id
  ORDER BY CASE
    WHEN n.role=2 AND n.lat BETWEEN -90 AND 90 AND n.lon BETWEEN -180 AND 180
      AND NOT(ABS(n.lat)<1e-9 AND ABS(n.lon)<1e-9) THEN 0
    WHEN n.role=2 THEN 1 WHEN COALESCE(n.advert_count,0)>0 THEN 2 ELSE 3 END,
    COALESCE(n.advert_count,0) DESC,n.last_seen DESC NULLS LAST,n.node_id
  LIMIT 1
$function$;

-- Existing prefix indexes handle normal keys. This small partial index covers
-- legacy leading whitespace without scanning every node for every path hop.
CREATE INDEX nodes_whitespace_id_idx ON core.nodes(node_id) WHERE node_id<>BTRIM(node_id);

CREATE FUNCTION core.lookup_node_identity_prefix(input_hash text)
RETURNS SETOF core.node_identity_nodes
LANGUAGE sql STABLE PARALLEL SAFE ROWS 4
SET search_path = pg_catalog, public
AS $function$
  SELECT identity.* FROM (
    SELECT COALESCE(a.canonical_node_id,UPPER(BTRIM(n.node_id))) AS node_id
    FROM core.nodes n LEFT JOIN core.node_identity_aliases a ON a.source_node_id=UPPER(BTRIM(n.node_id))
    WHERE LENGTH(input_hash)=4 AND UPPER(LEFT(n.node_id,4))=UPPER(input_hash)
    UNION
    SELECT COALESCE(a.canonical_node_id,UPPER(BTRIM(n.node_id)))
    FROM core.nodes n LEFT JOIN core.node_identity_aliases a ON a.source_node_id=UPPER(BTRIM(n.node_id))
    WHERE LENGTH(input_hash)=6 AND UPPER(LEFT(n.node_id,6))=UPPER(input_hash)
    UNION
    SELECT canonical_node_id FROM core.node_identity_aliases
    WHERE LENGTH(input_hash) IN (4,6) AND UPPER(LEFT(canonical_node_id,LENGTH(input_hash)))=UPPER(input_hash)
    UNION
    SELECT COALESCE(a.canonical_node_id,UPPER(BTRIM(n.node_id)))
    FROM core.nodes n LEFT JOIN core.node_identity_aliases a ON a.source_node_id=UPPER(BTRIM(n.node_id))
    WHERE n.node_id<>BTRIM(n.node_id) AND LENGTH(input_hash) IN (4,6)
      AND UPPER(LEFT(BTRIM(n.node_id),LENGTH(input_hash)))=UPPER(input_hash)
  ) candidates
  CROSS JOIN LATERAL core.lookup_node_identity(candidates.node_id) identity
  WHERE UPPER(LEFT(identity.node_id,LENGTH(input_hash)))=UPPER(input_hash)
$function$;
