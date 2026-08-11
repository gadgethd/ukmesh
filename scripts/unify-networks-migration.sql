-- ============================================================================
-- Network unification migration: collapse teesside / northeast -> ukmesh
-- ============================================================================
-- STAGED, DESTRUCTIVE, run only during the cutover maintenance window AFTER both
-- backends (.105 unified build, .108 nemesh region build with MQTT_INGEST_ENABLED=false)
-- are deployed. Run inside the timescaledb container:
--   docker compose exec -T timescaledb psql -U meshcore -d meshcore -v ON_ERROR_STOP=1 -f -  < scripts/unify-networks-migration.sql
--
-- Pre-flight (already captured 2026-06-19):
--   nodes:               ~1.9k non-ukmesh rows  (small, not a hypertable)
--   node_status_samples: ~2.2M non-ukmesh rows  (hypertable, 0 compressed chunks)
--   packets:             ~9M non-ukmesh rows     (8.2GB, 14/17 chunks COMPRESSED,
--                                                 network = compression segmentby key)
--   host free disk: ~30GB  -> packets MUST be relabelled chunk-by-chunk (below).
-- ============================================================================

\timing on

-- 1) Small / uncompressed tables -- safe single statements -------------------
BEGIN;
UPDATE nodes               SET network = 'ukmesh' WHERE network IN ('teesside','northeast');
-- node_status_samples is updated in bounded transactions by
-- scripts/unify-networks.sh; it is too large for this deployment transaction.
-- Preserve the full first/last-seen interval when collapsing the sightings PK.
INSERT INTO node_network_sightings (node_id, network, first_seen_at, last_seen_at)
SELECT node_id, 'ukmesh', MIN(first_seen_at), MAX(last_seen_at)
FROM node_network_sightings
WHERE network IN ('ukmesh','teesside','northeast')
GROUP BY node_id
ON CONFLICT (node_id, network) DO UPDATE SET
  first_seen_at = LEAST(node_network_sightings.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(node_network_sightings.last_seen_at, EXCLUDED.last_seen_at);
DELETE FROM node_network_sightings WHERE network IN ('teesside','northeast');
ALTER TABLE nodes ALTER COLUMN network SET DEFAULT 'ukmesh';
ALTER TABLE packets ALTER COLUMN network SET DEFAULT 'ukmesh';
ALTER TABLE node_status_samples ALTER COLUMN network SET DEFAULT 'ukmesh';
COMMIT;

-- 2) Derived / regenerable tables -- delete stale labels; workers rebuild -----
--    (path-learning-worker rebuilds 'ukmesh' priors on its next hourly run;
--     it discovers networks via SELECT DISTINCT network FROM packets/nodes.)
BEGIN;
DELETE FROM path_prefix_priors      WHERE network IN ('teesside','northeast');
DELETE FROM path_transition_priors  WHERE network IN ('teesside','northeast');
DELETE FROM path_edge_priors        WHERE network IN ('teesside','northeast');
DELETE FROM path_motif_priors       WHERE network IN ('teesside','northeast');
DELETE FROM path_model_calibration  WHERE network IN ('teesside','northeast');
DELETE FROM ml_gold_paths               WHERE network       IN ('teesside','northeast');
DELETE FROM ml_model_versions           WHERE network       IN ('teesside','northeast');
DELETE FROM ml_path_prefix_scores       WHERE network       IN ('teesside','northeast');
DELETE FROM ml_model_variant_runs       WHERE model_network IN ('teesside','northeast');
DELETE FROM ml_model_variant_packet_results WHERE model_network IN ('teesside','northeast')
                                            OR packet_network IN ('teesside','northeast');
DO $$
BEGIN
  IF to_regclass('path_simulation_runs') IS NOT NULL THEN
    EXECUTE 'DELETE FROM path_simulation_runs WHERE network IN (''teesside'',''northeast'')';
  END IF;
END $$;
UPDATE spam_suspects           SET network = 'ukmesh' WHERE network IN ('teesside','northeast');
UPDATE spam_message_incidents  SET network = 'ukmesh' WHERE network IN ('teesside','northeast');
UPDATE spam_message_members    SET network = 'ukmesh' WHERE network IN ('teesside','northeast');
COMMIT;

-- 3) packets hypertable -- the heavy step --------------------------------------
--    network is the compression segmentby key, so relabelling rewrites segments.
--    Pause the compression policy, then per compressed chunk: decompress ->
--    update -> recompress. This bounds peak disk to ~one chunk (vs decompressing
--    all 8GB at once, which won't fit in 30GB free).
--
--    Run the procedural block below (psql). It is idempotent and resumable.

-- 3a) Per-chunk update.
--     Do NOT run this as a PL/pgSQL DO loop: a DO block is one transaction, so
--     dead tuples/WAL accumulate across all chunks and can exhaust disk.
--     Use the shell wrapper instead; it commits and checkpoints each chunk:
--
--       scripts/relabel-packets-per-chunk.sh

-- The shell wrapper preserves and restores the pre-cutover compression-policy
-- state, including after an interrupted run.

-- 4) Verify -------------------------------------------------------------------
SELECT 'nodes'   AS tbl, network, count(*) FROM nodes GROUP BY network
UNION ALL SELECT 'status', network, count(*) FROM node_status_samples GROUP BY network
UNION ALL SELECT 'packets', network, count(*) FROM packets GROUP BY network
ORDER BY 1,2;
-- Expect: only 'ukmesh' across all three.
