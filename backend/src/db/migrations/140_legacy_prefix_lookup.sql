-- Uppercasing a legacy key can expand a character (for example a ligature).
-- Match prefixes AFTER the same normalization used by the canonical view.
-- Normal hex keys retain the existing prefix indexes; the small partial
-- index covers only keys whose stored and canonical forms differ.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DROP INDEX core.nodes_whitespace_id_idx;
CREATE INDEX nodes_legacy_id_idx ON core.nodes(node_id)
  WHERE node_id <> UPPER(BTRIM(node_id));

CREATE OR REPLACE FUNCTION core.lookup_node_identity_prefix(input_hash text)
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
    WHERE n.node_id<>UPPER(BTRIM(n.node_id)) AND LENGTH(input_hash) IN (4,6)
      AND LEFT(UPPER(BTRIM(n.node_id)),LENGTH(input_hash))=UPPER(input_hash)
  ) candidates
  CROSS JOIN LATERAL core.lookup_node_identity(candidates.node_id) identity
  WHERE UPPER(LEFT(identity.node_id,LENGTH(input_hash)))=UPPER(input_hash)
$function$;
