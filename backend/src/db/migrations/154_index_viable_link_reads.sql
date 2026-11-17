-- Public snapshots read viable identity-link metrics without scanning the full heap.
-- Keep the unconditional identity pair unique index: concurrent refresh requires
-- a unique index covering every row (PostgreSQL 16 REFRESH MATERIALIZED VIEW).
-- Keep raw link indexes unchanged: their predicate maintenance increased write
-- cost in matched fixture trials. This cover belongs to the published read model.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE INDEX IF NOT EXISTS node_identity_links_viable_metrics_idx
  ON core.node_identity_links (node_a_id, node_b_id)
  INCLUDE (observed_count, multibyte_observed_count, itm_viable,
           itm_path_loss_db, count_a_to_b, count_b_to_a, last_observed)
  WHERE itm_viable = true OR force_viable = true;
