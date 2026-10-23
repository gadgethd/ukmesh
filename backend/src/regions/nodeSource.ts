import { regionMemberIdsSql } from './filter.js';

/** Match node_identity_nodes' representative/aggregate semantics on just the
 * region's identities. The global view materializes every node before filtering;
 * this region-first path instead probes nodes_pkey and the alias indexes. */
export function regionNodeSourceSql(regionParam: string): string {
  return `(
    SELECT identity.*
    FROM (SELECT DISTINCT member.node_id
          FROM (${regionMemberIdsSql(regionParam)}) AS member(node_id)) members
    CROSS JOIN LATERAL (
      SELECT members.node_id, n.name, n.lat, n.lon, n.last_seen,
        bool_or(COALESCE(n.is_online, FALSE)) OVER () AS is_online,
        n.hardware_model, n.firmware_version, n.public_key,
        min(n.created_at) OVER () AS created_at,
        n.iata, n.role, (sum(COALESCE(n.advert_count, 0)) OVER ())::integer AS advert_count,
        n.elevation_m, n.network,
        max(n.last_predicted_online_at) OVER () AS last_predicted_online_at,
        max(n.last_path_evidence_at) OVER () AS last_path_evidence_at,
        max(n.last_mqtt_observer_seen_at) OVER () AS last_mqtt_observer_seen_at,
        max(n.last_rx_at) OVER () AS last_rx_at,
        max(n.last_status_at) OVER () AS last_status_at,
        n.observer_iata
      FROM nodes n
      WHERE n.node_id = ANY(ARRAY(
        SELECT members.node_id
        UNION
        SELECT a.source_node_id FROM node_identity_aliases a
        WHERE a.canonical_node_id = members.node_id
      ))
      ORDER BY CASE
        WHEN n.role = 2 AND n.lat BETWEEN -90 AND 90 AND n.lon BETWEEN -180 AND 180
          AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9) THEN 0
        WHEN n.role = 2 THEN 1
        WHEN COALESCE(n.advert_count, 0) > 0 THEN 2
        ELSE 3 END,
        COALESCE(n.advert_count, 0) DESC, n.last_seen DESC NULLS LAST, n.node_id
      LIMIT 1
    ) identity
  )`;
}
