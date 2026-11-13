-- The duplicate-identity guard otherwise reads nodes_pkey and normalizes every
-- key on every map request. Cover its base variable as well as the expression
-- so PostgreSQL can scan the stored normalized values without heap reads.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE INDEX nodes_normalized_lookup_idx ON core.nodes (UPPER(BTRIM(node_id))) INCLUDE (node_id);
DROP INDEX core.nodes_normalized_id_idx;
