-- Remove dead terrain_profile_json column from node_identity_links (no readers:
-- frontend terrain comes from DEM tiles; backend has zero references).
-- Transactional DROP+CREATE so the view is never missing mid-transaction.
BEGIN;
DROP VIEW IF EXISTS node_identity_links;
-- Optimize canonical node-identity views: replace per-row STABLE function
-- calls (meshcore_canonical_node_id — 5 calls/row on node_identity_links,
-- ~38s for a full scan) with set-based LEFT JOINs against
-- node_identity_aliases. Identical output columns/semantics.

CREATE VIEW node_identity_links AS
 WITH v AS MATERIALIZED (
 WITH mapped AS (
         SELECT COALESCE(la.canonical_node_id, upper(btrim(node_links.node_a_id))) AS raw_a,
            COALESCE(lb.canonical_node_id, upper(btrim(node_links.node_b_id))) AS raw_b,
            node_links.observed_count,
            node_links.last_observed,
            node_links.itm_path_loss_db,
            node_links.itm_viable,
            node_links.itm_computed_at,
            node_links.count_a_to_b,
            node_links.count_b_to_a,
            node_links.force_viable,
            node_links.multibyte_observed_count
           FROM node_links
           LEFT JOIN node_identity_aliases la ON la.source_node_id = upper(btrim(node_links.node_a_id))
           LEFT JOIN node_identity_aliases lb ON lb.source_node_id = upper(btrim(node_links.node_b_id))
          WHERE node_links.node_a_id IS NOT NULL AND node_links.node_b_id IS NOT NULL
        ), oriented AS (
         SELECT LEAST(mapped.raw_a, mapped.raw_b) AS node_a_id,
            GREATEST(mapped.raw_a, mapped.raw_b) AS node_b_id,
            mapped.observed_count,
            mapped.last_observed,
            mapped.itm_path_loss_db,
            mapped.itm_viable,
            mapped.itm_computed_at,
                CASE
                    WHEN mapped.raw_a <= mapped.raw_b THEN mapped.count_a_to_b
                    ELSE mapped.count_b_to_a
                END AS count_a_to_b,
                CASE
                    WHEN mapped.raw_a <= mapped.raw_b THEN mapped.count_b_to_a
                    ELSE mapped.count_a_to_b
                END AS count_b_to_a,
            mapped.force_viable,
            mapped.multibyte_observed_count
           FROM mapped
          WHERE mapped.raw_a <> mapped.raw_b
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
    sum(multibyte_observed_count)::integer AS multibyte_observed_count
   FROM oriented
  GROUP BY node_a_id, node_b_id
) SELECT * FROM v;
CREATE OR REPLACE VIEW node_identity_link_radio_reports AS
 WITH v AS MATERIALIZED (
 WITH mapped AS (
         SELECT COALESCE(la.canonical_node_id, upper(btrim(node_link_radio_reports.node_a_id))) AS raw_a,
            COALESCE(lb.canonical_node_id, upper(btrim(node_link_radio_reports.node_b_id))) AS raw_b,
            COALESCE(lr.canonical_node_id, upper(btrim(node_link_radio_reports.reporter_node_id))) AS reporter_node_id,
            COALESCE(lp.canonical_node_id, upper(btrim(node_link_radio_reports.peer_node_id))) AS peer_node_id,
            node_link_radio_reports.last_snr_db,
            node_link_radio_reports.best_snr_db,
            node_link_radio_reports.last_seen,
            node_link_radio_reports.sample_count
           FROM node_link_radio_reports
           LEFT JOIN node_identity_aliases la ON la.source_node_id = upper(btrim(node_link_radio_reports.node_a_id))
           LEFT JOIN node_identity_aliases lb ON lb.source_node_id = upper(btrim(node_link_radio_reports.node_b_id))
           LEFT JOIN node_identity_aliases lr ON lr.source_node_id = upper(btrim(node_link_radio_reports.reporter_node_id))
           LEFT JOIN node_identity_aliases lp ON lp.source_node_id = upper(btrim(node_link_radio_reports.peer_node_id))
          WHERE node_link_radio_reports.node_a_id IS NOT NULL AND node_link_radio_reports.node_b_id IS NOT NULL
        ), oriented AS (
         SELECT LEAST(mapped.raw_a, mapped.raw_b) AS node_a_id,
            GREATEST(mapped.raw_a, mapped.raw_b) AS node_b_id,
            mapped.reporter_node_id,
            mapped.peer_node_id,
            mapped.last_snr_db,
            mapped.best_snr_db,
            mapped.last_seen,
            mapped.sample_count
           FROM mapped
          WHERE mapped.raw_a <> mapped.raw_b
        )
 SELECT node_a_id,
    node_b_id,
    reporter_node_id,
    peer_node_id,
    (array_agg(last_snr_db ORDER BY oriented.last_seen DESC NULLS LAST))[1] AS last_snr_db,
    max(best_snr_db) AS best_snr_db,
    max(last_seen) AS last_seen,
    sum(sample_count)::integer AS sample_count
   FROM oriented
  GROUP BY node_a_id, node_b_id, reporter_node_id, peer_node_id
) SELECT * FROM v;
CREATE OR REPLACE VIEW node_identity_nodes AS
 WITH v AS MATERIALIZED (
 WITH mapped AS (
         SELECT n.node_id,
            n.name,
            n.lat,
            n.lon,
            n.last_seen,
            n.is_online,
            n.hardware_model,
            n.firmware_version,
            n.public_key,
            n.created_at,
            n.iata,
            n.role,
            n.advert_count,
            n.elevation_m,
            n.network,
            n.last_predicted_online_at,
            n.last_path_evidence_at,
            n.last_mqtt_observer_seen_at,
            n.last_rx_at,
            n.last_status_at,
            n.observer_iata,
            COALESCE(la.canonical_node_id, upper(btrim(n.node_id))) AS canonical_node_id
           FROM nodes n
           LEFT JOIN node_identity_aliases la ON la.source_node_id = upper(btrim(n.node_id))
        ), ranked AS (
         SELECT m.node_id,
            m.name,
            m.lat,
            m.lon,
            m.last_seen,
            m.is_online,
            m.hardware_model,
            m.firmware_version,
            m.public_key,
            m.created_at,
            m.iata,
            m.role,
            m.advert_count,
            m.elevation_m,
            m.network,
            m.last_predicted_online_at,
            m.last_path_evidence_at,
            m.last_mqtt_observer_seen_at,
            m.last_rx_at,
            m.last_status_at,
            m.observer_iata,
            m.canonical_node_id,
            row_number() OVER (PARTITION BY m.canonical_node_id ORDER BY (
                CASE
                    WHEN m.role = 2 AND m.lat >= '-90'::integer::double precision AND m.lat <= 90::double precision AND m.lon >= '-180'::integer::double precision AND m.lon <= 180::double precision AND NOT (abs(m.lat) < 0.000000001::double precision AND abs(m.lon) < 0.000000001::double precision) THEN 0
                    WHEN m.role = 2 THEN 1
                    WHEN COALESCE(m.advert_count, 0) > 0 THEN 2
                    ELSE 3
                END), (COALESCE(m.advert_count, 0)) DESC, m.last_seen DESC NULLS LAST, m.node_id) AS representative_rank
           FROM mapped m
        ), aggregated AS (
         SELECT mapped.canonical_node_id,
            bool_or(COALESCE(mapped.is_online, false)) AS is_online,
            max(mapped.last_seen) AS last_seen,
            max(mapped.last_predicted_online_at) AS last_predicted_online_at,
            max(mapped.last_path_evidence_at) AS last_path_evidence_at,
            max(mapped.last_mqtt_observer_seen_at) AS last_mqtt_observer_seen_at,
            max(mapped.last_rx_at) AS last_rx_at,
            max(mapped.last_status_at) AS last_status_at,
            min(mapped.created_at) AS created_at,
            sum(COALESCE(mapped.advert_count, 0))::integer AS advert_count,
            array_agg(mapped.node_id ORDER BY mapped.node_id) AS identity_source_ids,
            count(*)::integer AS identity_member_count
           FROM mapped
          GROUP BY mapped.canonical_node_id
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
  WHERE r.representative_rank = 1
) SELECT * FROM v;COMMIT;
