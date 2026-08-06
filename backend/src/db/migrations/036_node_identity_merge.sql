-- Reversible identity projection for MQTT observer keys and decoded advert keys.
-- Raw node, packet, and telemetry rows remain intact; the data layer exposes
-- canonical views and the backend periodically refreshes accepted aliases.

CREATE TABLE IF NOT EXISTS node_identity_aliases (
  source_node_id   TEXT PRIMARY KEY
    CHECK (source_node_id ~ '^[0-9A-F]{64}$'),
  canonical_node_id TEXT NOT NULL
    CHECK (canonical_node_id ~ '^[0-9A-F]{64}$'),
  confidence       TEXT NOT NULL
    CHECK (confidence IN ('high', 'medium')),
  reason           TEXT NOT NULL,
  evidence         JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_kind      TEXT NOT NULL DEFAULT 'automatic'
    CHECK (source_kind IN ('automatic', 'manual')),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (source_node_id <> canonical_node_id)
);

CREATE INDEX IF NOT EXISTS node_identity_aliases_canonical_idx
  ON node_identity_aliases (canonical_node_id);

CREATE TABLE IF NOT EXISTS node_identity_match_evidence (
  node_a_id        TEXT NOT NULL
    CHECK (node_a_id ~ '^[0-9A-F]{64}$'),
  node_b_id        TEXT NOT NULL
    CHECK (node_b_id ~ '^[0-9A-F]{64}$'),
  decision         TEXT NOT NULL
    CHECK (decision IN ('accepted', 'ambiguous')),
  confidence      TEXT NOT NULL
    CHECK (confidence IN ('high', 'medium', 'low')),
  score           INTEGER NOT NULL DEFAULT 0
    CHECK (score >= 0),
  reason           TEXT NOT NULL,
  evidence         JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_kind      TEXT NOT NULL DEFAULT 'automatic'
    CHECK (source_kind IN ('automatic', 'manual')),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (node_a_id, node_b_id),
  CHECK (node_a_id < node_b_id)
);

ALTER TABLE node_identity_match_evidence
  ADD COLUMN IF NOT EXISTS score INTEGER NOT NULL DEFAULT 0;
ALTER TABLE node_identity_match_evidence
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'automatic';

CREATE INDEX IF NOT EXISTS node_identity_match_evidence_decision_idx
  ON node_identity_match_evidence (decision, updated_at DESC);

CREATE OR REPLACE FUNCTION meshcore_canonical_node_id(input_node_id TEXT)
RETURNS TEXT
LANGUAGE SQL
STABLE
PARALLEL SAFE
SET search_path = public
AS $$
  SELECT CASE
    WHEN input_node_id IS NULL OR btrim(input_node_id) = '' THEN input_node_id
    ELSE COALESCE(
      (
        SELECT alias.canonical_node_id
        FROM node_identity_aliases alias
        WHERE alias.source_node_id = upper(btrim(input_node_id))
      ),
      upper(btrim(input_node_id))
    )
  END
$$;

-- One row per canonical identity. The representative is the best positioned
-- repeater row, then the row with the strongest advert evidence. Aggregate
-- liveness, telemetry timestamps, and advert counts across all members.
CREATE VIEW node_identity_nodes AS
WITH mapped AS (
  SELECT n.*,
         meshcore_canonical_node_id(n.node_id) AS canonical_node_id
    FROM nodes n
), ranked AS (
  SELECT m.*,
         row_number() OVER (
           PARTITION BY m.canonical_node_id
           ORDER BY
             CASE
               WHEN m.role = 2
                AND m.lat BETWEEN -90 AND 90
                AND m.lon BETWEEN -180 AND 180
                AND NOT (ABS(m.lat) < 1e-9 AND ABS(m.lon) < 1e-9)
                 THEN 0
               WHEN m.role = 2 THEN 1
               WHEN COALESCE(m.advert_count, 0) > 0 THEN 2
               ELSE 3
             END,
             COALESCE(m.advert_count, 0) DESC,
             m.last_seen DESC NULLS LAST,
             m.node_id
         ) AS representative_rank
    FROM mapped m
), aggregated AS (
  SELECT canonical_node_id,
         bool_or(COALESCE(is_online, FALSE)) AS is_online,
         max(last_seen) AS last_seen,
         max(last_predicted_online_at) AS last_predicted_online_at,
         max(last_path_evidence_at) AS last_path_evidence_at,
         max(last_mqtt_observer_seen_at) AS last_mqtt_observer_seen_at,
         max(last_rx_at) AS last_rx_at,
         max(last_status_at) AS last_status_at,
         min(created_at) AS created_at,
         sum(COALESCE(advert_count, 0))::integer AS advert_count,
         array_agg(node_id ORDER BY node_id) AS identity_source_ids,
         count(*)::integer AS identity_member_count
    FROM mapped
   GROUP BY canonical_node_id
)
SELECT r.canonical_node_id AS node_id,
       r.name,
       r.lat,
       r.lon,
       r.last_seen,
       a.is_online,
       r.hardware_model,
       r.firmware_version,
       r.public_key,
       a.created_at,
       r.iata,
       r.role,
       a.advert_count,
       r.elevation_m,
       r.network,
       a.last_predicted_online_at,
       a.last_path_evidence_at,
       a.last_mqtt_observer_seen_at,
       a.last_rx_at,
       a.last_status_at,
       r.observer_iata,
       a.identity_source_ids,
       a.identity_member_count
  FROM ranked r
  JOIN aggregated a ON a.canonical_node_id = r.canonical_node_id
 WHERE r.representative_rank = 1;

CREATE VIEW node_identity_sightings AS
SELECT meshcore_canonical_node_id(node_id) AS node_id,
       network,
       min(first_seen_at) AS first_seen_at,
       max(last_seen_at) AS last_seen_at
  FROM node_network_sightings
 GROUP BY meshcore_canonical_node_id(node_id), network;

CREATE VIEW node_identity_status_samples AS
SELECT time,
       meshcore_canonical_node_id(node_id) AS node_id,
       network,
       battery_mv,
       uptime_secs,
       tx_air_secs,
       rx_air_secs,
       channel_utilization,
       air_util_tx,
       stats
  FROM node_status_samples;

-- Packet observations retain all raw columns but expose canonical observer and
-- source keys. Public privacy queries may continue to use raw packets when
-- they need the original private-prefix evidence.
CREATE VIEW node_identity_packets AS
SELECT time,
       packet_hash,
       meshcore_canonical_node_id(rx_node_id) AS rx_node_id,
       meshcore_canonical_node_id(src_node_id) AS src_node_id,
       topic,
       packet_type,
       route_type,
       hop_count,
       rssi,
       snr,
       payload,
       raw_hex,
       advert_count,
       path_hashes,
       network,
       path_hash_size_bytes,
       transport_codes,
       region_scope,
       companion_sender,
       topic_prefix,
       iata,
       is_private,
       visibility_ok
  FROM packets;

-- Link rows are collapsed by the canonical unordered endpoint pair. Direction
-- counters are rotated when the raw key order differs from the canonical order.
CREATE VIEW node_identity_links AS
WITH mapped AS (
  SELECT meshcore_canonical_node_id(node_a_id) AS raw_a,
         meshcore_canonical_node_id(node_b_id) AS raw_b,
         observed_count,
         last_observed,
         itm_path_loss_db,
         itm_viable,
         itm_computed_at,
         count_a_to_b,
         count_b_to_a,
         force_viable,
         multibyte_observed_count,
         terrain_profile_json
    FROM node_links
), oriented AS (
  SELECT LEAST(raw_a, raw_b) AS node_a_id,
         GREATEST(raw_a, raw_b) AS node_b_id,
         observed_count,
         last_observed,
         itm_path_loss_db,
         itm_viable,
         itm_computed_at,
         CASE WHEN raw_a <= raw_b THEN count_a_to_b ELSE count_b_to_a END AS count_a_to_b,
         CASE WHEN raw_a <= raw_b THEN count_b_to_a ELSE count_a_to_b END AS count_b_to_a,
         force_viable,
         multibyte_observed_count,
         terrain_profile_json
    FROM mapped
   WHERE raw_a <> raw_b
)
SELECT node_a_id,
       node_b_id,
       sum(observed_count)::integer AS observed_count,
       max(last_observed) AS last_observed,
       min(itm_path_loss_db) AS itm_path_loss_db,
       bool_or(itm_viable) AS itm_viable,
       max(itm_computed_at) AS itm_computed_at,
       sum(count_a_to_b)::integer AS count_a_to_b,
       sum(count_b_to_a)::integer AS count_b_to_a,
       bool_or(force_viable) AS force_viable,
       sum(multibyte_observed_count)::integer AS multibyte_observed_count,
       (array_agg(terrain_profile_json ORDER BY last_observed DESC NULLS LAST)
         FILTER (WHERE terrain_profile_json IS NOT NULL))[1] AS terrain_profile_json
  FROM oriented
 GROUP BY node_a_id, node_b_id;

CREATE VIEW node_identity_link_radio_reports AS
WITH mapped AS (
  SELECT meshcore_canonical_node_id(node_a_id) AS raw_a,
         meshcore_canonical_node_id(node_b_id) AS raw_b,
         meshcore_canonical_node_id(reporter_node_id) AS reporter_node_id,
         meshcore_canonical_node_id(peer_node_id) AS peer_node_id,
         last_snr_db,
         best_snr_db,
         last_seen,
         sample_count
    FROM node_link_radio_reports
), oriented AS (
  SELECT LEAST(raw_a, raw_b) AS node_a_id,
         GREATEST(raw_a, raw_b) AS node_b_id,
         reporter_node_id,
         peer_node_id,
         last_snr_db,
         best_snr_db,
         last_seen,
         sample_count
    FROM mapped
   WHERE raw_a <> raw_b
)
SELECT node_a_id,
       node_b_id,
       reporter_node_id,
       peer_node_id,
       (array_agg(last_snr_db ORDER BY last_seen DESC NULLS LAST))[1] AS last_snr_db,
       max(best_snr_db) AS best_snr_db,
       max(last_seen) AS last_seen,
       sum(sample_count)::integer AS sample_count
  FROM oriented
 GROUP BY node_a_id, node_b_id, reporter_node_id, peer_node_id;
