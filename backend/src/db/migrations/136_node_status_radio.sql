-- 136_node_status_radio.sql
-- Persist the observer's radio settings from the MQTT status envelope.
--
-- Every status publisher already sends a top-level `radio` string, either the
-- CoreScope-canonical "MHz,kHz,SF,CR" (MCRS) or a vendor spelling such as
-- "SX1262 869.618/62/8/8" (mqtt-observer firmware), but ingest dropped it. The
-- field was therefore visible on the wire and invisible on every ukmesh
-- surface. Store it verbatim per sample; canonicalisation stays a consumer
-- concern so no information is lost at the ingest boundary.
--
-- Both projections are recreated to expose the column. node_identity_status_samples
-- carries the production alias join (migration 036 creates the table and the
-- unaliased view); appending columns keeps CREATE OR REPLACE VIEW valid on both
-- a fresh database and the deployed one. The public shims are frozen
-- projections, not SELECT *, so they must be refreshed explicitly.
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE core.node_status_samples
  ADD COLUMN IF NOT EXISTS radio TEXT;

CREATE OR REPLACE VIEW core.node_identity_status_samples AS
SELECT s."time",
       COALESCE(la.canonical_node_id, upper(btrim(s.node_id))) AS node_id,
       s.network,
       s.battery_mv,
       s.uptime_secs,
       s.tx_air_secs,
       s.rx_air_secs,
       s.channel_utilization,
       s.air_util_tx,
       s.stats,
       s.node_id AS node_id_raw,
       s.radio
  FROM core.node_status_samples s
  LEFT JOIN core.node_identity_aliases la
    ON la.source_node_id = upper(btrim(s.node_id));

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_views
     WHERE schemaname = 'public' AND viewname = 'node_identity_status_samples'
  ) THEN
    EXECUTE 'CREATE OR REPLACE VIEW public.node_identity_status_samples AS SELECT * FROM core.node_identity_status_samples';
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_views
     WHERE schemaname = 'public' AND viewname = 'node_status_samples'
  ) THEN
    EXECUTE $view$CREATE OR REPLACE VIEW public.node_status_samples AS
      SELECT "time", node_id, network, battery_mv, uptime_secs, tx_air_secs,
             rx_air_secs, channel_utilization, air_util_tx, stats, status, radio
        FROM core.node_status_samples$view$;
  END IF;
END $$;
