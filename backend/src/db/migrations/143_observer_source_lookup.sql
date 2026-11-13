-- Source discovery was scanning the globally ordered source index for each
-- observer. Bound both discovery and one-public-observation proofs by rx/src.
-- Ordinary transactional builds are intentional for the owned DEV fixtures:
-- interrupted builds roll back atomically; production online rollout is F2.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE INDEX packets_observer_source_time_idx
  ON mesh.packets (rx_node_id, src_node_id, time DESC) INCLUDE (network)
  WHERE src_node_id IS NOT NULL;
ANALYZE core.nodes;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid='mesh.packets_observer_source_time_idx'::regclass AND indisvalid) THEN
    RAISE EXCEPTION 'observer source index was not built completely';
  END IF;
END
$$;
