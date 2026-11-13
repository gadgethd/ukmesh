-- The map filters on effective presence rather than a single stored timestamp.
-- Expression statistics avoid the default 0.5% range estimate that caused a
-- complete scan/sort even when an ordered index can satisfy the first page.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE STATISTICS core.nodes_effective_presence_stats ON
  (GREATEST(last_seen,last_rx_at,last_status_at,last_path_evidence_at))
  FROM core.nodes;
ANALYZE core.nodes;
