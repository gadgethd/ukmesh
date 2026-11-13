-- Scoped canonical hydration must also find mixed-case/whitespace legacy IDs.
-- Non-unique: preserve those identities and let the canonical projection rank
-- and aggregate their members exactly as before.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE INDEX nodes_normalized_id_idx ON core.nodes (UPPER(BTRIM(node_id)));
