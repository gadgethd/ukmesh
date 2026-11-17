-- Keep ordered map reads local without adding another per-ingest index.
-- This is a one-time physical rewrite; later DEV bulk loads finalize layout
-- in prepareSampleDataset. CLUSTER preserves the relation and its row values.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CLUSTER core.nodes USING nodes_normalized_lookup_idx;
ANALYZE core.nodes;
