import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type OwnerRepositoryDeps = {
  query: QueryFn;
};

export type OwnerRepository = ReturnType<typeof createOwnerRepository>;

export function createOwnerRepository(deps: OwnerRepositoryDeps) {
  const { query } = deps;

  async function fetchLastHopStrength(rxNodeIds: string[], allOwnerNodeIds: string[], since?: string) {
    // $1 = rxNodeIds (the selected node to filter received packets)
    // $2 = allOwnerNodeIds (all owned nodes to exclude from last-hop analysis)
    // $3 = since (optional time filter)
    const params: unknown[] = [rxNodeIds, allOwnerNodeIds];
    const timeFilter = since
      ? `p.time >= $3::timestamptz`
      : `p.time > NOW() - INTERVAL '7 days'`;
    if (since) params.push(since);

    return query<{
      bucket: string;
      last_hop_node_id: string | null;
      last_hop_name: string;
      resolution: 'direct' | 'resolved' | 'inferred' | 'unresolved';
      avg_snr: number | null;
      avg_rssi: number | null;
      sample_count: number;
    }>(
      `WITH clashing_prefixes AS (
         SELECT LEFT(node_id, 2) AS prefix
         FROM node_identity_nodes
         WHERE role IN (1, 3)
         INTERSECT
         SELECT LEFT(node_id, 2) AS prefix
         FROM node_identity_nodes
         WHERE role = 2
       ),
       owner_packets AS (
         SELECT DISTINCT ON (p.rx_node_id, COALESCE(p.packet_hash, p.time::text))
           p.time,
           p.packet_hash,
           p.rx_node_id,
           p.src_node_id,
           p.hop_count,
           p.rssi,
           p.snr,
           CASE
             WHEN p.hop_count = 0
               AND p.src_node_id IS NOT NULL
               AND p.src_node_id != p.rx_node_id
             THEN UPPER(LEFT(p.src_node_id, 2))
             WHEN p.src_node_id = p.rx_node_id
               AND COALESCE(array_length(p.path_hashes, 1), 0) >= 2
             THEN UPPER(p.path_hashes[array_length(p.path_hashes, 1) - 1])
             WHEN p.src_node_id IS NOT NULL
               AND COALESCE(array_length(p.path_hashes, 1), 0) >= 1
             THEN UPPER(p.path_hashes[array_length(p.path_hashes, 1)])
             ELSE NULL
           END AS receiver_side_hash
         FROM node_identity_packets p
         WHERE p.rx_node_id_raw IN (
           SELECT meshcore_canonical_node_id(id)
           FROM unnest($1::text[]) AS selected(id)
           UNION
           SELECT source_node_id
           FROM node_identity_aliases
           WHERE canonical_node_id = ANY(
             SELECT meshcore_canonical_node_id(id)
             FROM unnest($1::text[]) AS selected(id)
           )
         )
           AND ${timeFilter}
           AND p.packet_type NOT IN (8, 9)
           AND p.src_node_id != p.rx_node_id
           AND (p.snr IS NOT NULL OR p.rssi IS NOT NULL)
           AND NOT (
             p.hop_count <= 1
             AND COALESCE(array_length(p.path_hashes, 1), 0) >= 1
             AND UPPER(p.path_hashes[array_length(p.path_hashes, 1)]) IN (SELECT prefix FROM clashing_prefixes)
           )
         ORDER BY p.rx_node_id, COALESCE(p.packet_hash, p.time::text), p.time ASC
       ),
       unique_receiver_targets AS (
         SELECT DISTINCT rx_node_id, receiver_side_hash
         FROM owner_packets
         WHERE receiver_side_hash IS NOT NULL
           AND hop_count IS NOT NULL
           AND hop_count > 0
       ),
       resolved_last_hop AS (
         SELECT
           uh.rx_node_id,
           uh.receiver_side_hash,
           n.node_id,
           COALESCE(n.name, n.node_id) AS name,
           COUNT(*) OVER (PARTITION BY uh.rx_node_id, uh.receiver_side_hash) AS match_count,
           ROW_NUMBER() OVER (PARTITION BY uh.rx_node_id, uh.receiver_side_hash ORDER BY n.node_id) AS rn
         FROM unique_receiver_targets uh
         JOIN node_identity_nodes n
           ON (n.role IS NULL OR n.role NOT IN (1, 3))
          AND UPPER(n.node_id) LIKE uh.receiver_side_hash || '%'
       ),
       inferred_last_hop AS (
         SELECT
           uh.rx_node_id,
           uh.receiver_side_hash,
           n.node_id,
           COALESCE(n.name, n.node_id) AS name,
           ROW_NUMBER() OVER (
             PARTITION BY uh.rx_node_id, uh.receiver_side_hash
             ORDER BY
               CASE
                 WHEN nl.force_viable = true OR nl.itm_viable = true THEN 0
                 WHEN nl.itm_path_loss_db IS NOT NULL AND nl.itm_path_loss_db <= 137.5 THEN 1
                 ELSE 2
               END,
               ((COALESCE(n.lat, 0) - COALESCE(rx.lat, 0)) * (COALESCE(n.lat, 0) - COALESCE(rx.lat, 0)))
               + ((COALESCE(n.lon, 0) - COALESCE(rx.lon, 0)) * (COALESCE(n.lon, 0) - COALESCE(rx.lon, 0))),
               n.node_id
           ) AS rn
         FROM unique_receiver_targets uh
         JOIN node_identity_nodes rx ON rx.node_id = uh.rx_node_id
         JOIN node_identity_nodes n
           ON (n.role IS NULL OR n.role NOT IN (1, 3))
          AND UPPER(LEFT(n.node_id, 2)) = UPPER(LEFT(uh.receiver_side_hash, 2))
         LEFT JOIN node_identity_links nl
           ON (
             (nl.node_a_id = uh.rx_node_id AND nl.node_b_id = n.node_id)
             OR (nl.node_b_id = uh.rx_node_id AND nl.node_a_id = n.node_id)
           )
         WHERE (
           nl.force_viable = true
           OR nl.itm_viable = true
           OR (nl.itm_path_loss_db IS NOT NULL AND nl.itm_path_loss_db <= 137.5)
         )
       ),
       classified AS (
         SELECT
           time_bucket('1 hour', op.time)::text AS bucket,
           CASE
             WHEN op.hop_count = 0
               AND op.src_node_id IS NOT NULL
               AND NOT (op.src_node_id = ANY(
                 SELECT meshcore_canonical_node_id(id)
                 FROM unnest($2::text[]) AS owned(id)
               ))
               AND src.node_id IS NOT NULL
               AND (src.role IS NULL OR src.role NOT IN (1, 3))
             THEN op.src_node_id
             WHEN rl.match_count = 1 AND rl.rn = 1 THEN rl.node_id
             WHEN ilh.rn = 1 THEN ilh.node_id
             ELSE NULL
           END AS last_hop_node_id,
           CASE
             WHEN op.hop_count = 0
               AND op.src_node_id IS NOT NULL
               AND NOT (op.src_node_id = ANY(
                 SELECT meshcore_canonical_node_id(id)
                 FROM unnest($2::text[]) AS owned(id)
               ))
               AND src.node_id IS NOT NULL
               AND (src.role IS NULL OR src.role NOT IN (1, 3))
             THEN COALESCE(src.name, op.src_node_id)
             WHEN rl.match_count = 1 AND rl.rn = 1 THEN rl.name
             WHEN ilh.rn = 1 THEN ilh.name
             ELSE 'Unresolved'
           END AS last_hop_name,
           CASE
             WHEN op.hop_count = 0
               AND op.src_node_id IS NOT NULL
               AND NOT (op.src_node_id = ANY(
                 SELECT meshcore_canonical_node_id(id)
                 FROM unnest($2::text[]) AS owned(id)
               ))
               AND src.node_id IS NOT NULL
               AND (src.role IS NULL OR src.role NOT IN (1, 3))
             THEN 'direct'
             WHEN rl.match_count = 1 AND rl.rn = 1 THEN 'resolved'
             WHEN ilh.rn = 1 THEN 'inferred'
             ELSE 'unresolved'
           END AS resolution,
           op.snr,
           op.rssi
         FROM owner_packets op
         LEFT JOIN node_identity_nodes src ON src.node_id = op.src_node_id
         LEFT JOIN resolved_last_hop rl
           ON rl.rx_node_id = op.rx_node_id
          AND rl.receiver_side_hash = op.receiver_side_hash
          AND rl.rn = 1
         LEFT JOIN inferred_last_hop ilh
           ON ilh.rx_node_id = op.rx_node_id
          AND ilh.receiver_side_hash = op.receiver_side_hash
          AND ilh.rn = 1
       )
       SELECT
         bucket,
         last_hop_node_id,
         last_hop_name,
         resolution,
         AVG(snr)::double precision AS avg_snr,
         AVG(rssi)::double precision AS avg_rssi,
         COUNT(*)::int AS sample_count
       FROM classified
       GROUP BY bucket, last_hop_node_id, last_hop_name, resolution
       ORDER BY bucket ASC, sample_count DESC, last_hop_name ASC`,
      params,
    );
  }

  async function fetchOwnerLiveData(selectedNodeId: string) {
    const [
      ownerNodeResult,
      incomingResult,
      packetResult,
      heardByResult,
      linkHealthResult,
      advertTrendResult,
      telemetryResult,
      heardNeighborsResult,
      packetsSentResult,
      packetsReceivedResult,
    ] = await Promise.all([
      query<{
        node_id: string;
        name: string | null;
        network: string;
        iata: string | null;
        advert_count: number | null;
        last_seen: string | null;
        lat: number | null;
        lon: number | null;
        role: number | null;
        members: string[];
      }>(
        `SELECT node_id, name, network, iata, advert_count, last_seen, lat, lon, role,
                identity_source_ids AS members
         FROM node_identity_nodes
         WHERE node_id = meshcore_canonical_node_id($1)
         LIMIT 1`,
        [selectedNodeId],
      ),
      query<{
        node_id: string;
        name: string | null;
        network: string | null;
        iata: string | null;
        lat: number | null;
        lon: number | null;
        packets_24h: number;
        last_seen: string | null;
      }>(
        `SELECT
           p.src_node_id AS node_id,
           n.name,
           n.network,
           n.iata,
           n.lat,
           n.lon,
           COUNT(*)::int AS packets_24h,
           MAX(p.time)::text AS last_seen
         FROM node_identity_packets p
         LEFT JOIN node_identity_nodes n ON n.node_id = p.src_node_id
         WHERE p.rx_node_id_raw IN (
           SELECT meshcore_canonical_node_id($1)
           UNION
           SELECT source_node_id FROM node_identity_aliases
           WHERE canonical_node_id = meshcore_canonical_node_id($1)
         )
           AND p.hop_count = 0
           AND p.src_node_id IS NOT NULL
           AND p.src_node_id <> meshcore_canonical_node_id($1)
           AND p.time > NOW() - INTERVAL '24 hours'
         GROUP BY p.src_node_id, n.name, n.network, n.iata, n.lat, n.lon
         ORDER BY packets_24h DESC
         LIMIT 250`,
        [selectedNodeId],
      ),
      query<{
        time: string;
        packet_type: number | null;
        route_type: number | null;
        hop_count: number | null;
        packet_hash: string | null;
        src_node_id: string | null;
        src_node_name: string | null;
        sender: string | null;
        body: string | null;
      }>(
        `WITH recent AS (
           SELECT
             p.time,
             p.packet_type,
             p.route_type,
             p.hop_count,
             p.packet_hash,
             p.src_node_id,
             p.payload
           FROM node_identity_packets p
           WHERE p.rx_node_id_raw IN (
             SELECT meshcore_canonical_node_id($1)
             UNION
             SELECT source_node_id FROM node_identity_aliases
             WHERE canonical_node_id = meshcore_canonical_node_id($1)
           )
             AND p.time > NOW() - INTERVAL '30 days'
           ORDER BY p.time DESC
           LIMIT 5000
         ),
         ranked AS (
           SELECT
             recent.time,
             recent.packet_type,
             recent.route_type,
             recent.hop_count,
             recent.packet_hash,
             recent.src_node_id,
             recent.payload,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(
                 recent.packet_hash,
                 CONCAT_WS(':',
                   COALESCE(recent.src_node_id, ''),
                   COALESCE(recent.packet_type::text, ''),
                   COALESCE(recent.route_type::text, ''),
                   COALESCE(recent.hop_count::text, ''),
                   COALESCE(recent.payload->'decrypted'->>'sender', ''),
                   COALESCE(recent.payload->'decrypted'->>'text', ''),
                   DATE_TRUNC('second', recent.time)::text
                 )
               )
               ORDER BY recent.time DESC
             ) AS rn
           FROM recent
         )
         SELECT
           r.time::text AS time,
           r.packet_type,
           r.route_type,
           r.hop_count,
           r.packet_hash,
           r.src_node_id,
           src.name AS src_node_name,
           COALESCE(r.payload->'decrypted'->>'sender', src.name, r.src_node_id) AS sender,
           COALESCE(
             r.payload->'decrypted'->>'text',
             r.payload->'decrypted'->>'message',
             r.payload->'decrypted'->>'body',
             r.payload->'decoded'->>'text',
             r.payload->>'message'
           ) AS body
         FROM ranked r
         LEFT JOIN node_identity_nodes src ON src.node_id = r.src_node_id
         WHERE r.rn = 1
         ORDER BY r.time DESC
         LIMIT 9`,
        [selectedNodeId],
      ),
      query<{
        node_id: string;
        name: string | null;
        network: string | null;
        iata: string | null;
        lat: number | null;
        lon: number | null;
        packets_24h: number;
        packets_7d: number;
        last_seen: string | null;
        best_hops: number | null;
      }>(
        `SELECT
           p.rx_node_id AS node_id,
           n.name,
           n.network,
           n.iata,
           n.lat,
           n.lon,
           COUNT(DISTINCT CASE WHEN p.time > NOW() - INTERVAL '24 hours' THEN p.packet_hash END)::int AS packets_24h,
           COUNT(DISTINCT p.packet_hash)::int AS packets_7d,
           MAX(p.time)::text AS last_seen,
           MIN(p.hop_count) AS best_hops
         FROM node_identity_packets p
         LEFT JOIN node_identity_nodes n ON n.node_id = p.rx_node_id
         WHERE p.src_node_id_raw IN (
           SELECT meshcore_canonical_node_id($1)
           UNION
           SELECT source_node_id FROM node_identity_aliases
           WHERE canonical_node_id = meshcore_canonical_node_id($1)
         )
           AND p.rx_node_id IS NOT NULL
           AND p.rx_node_id <> meshcore_canonical_node_id($1)
           AND p.time > NOW() - INTERVAL '7 days'
         GROUP BY p.rx_node_id, n.name, n.network, n.iata, n.lat, n.lon
         ORDER BY packets_24h DESC, packets_7d DESC, last_seen DESC
         LIMIT 20`,
        [selectedNodeId],
      ),
      query<{
        peer_node_id: string;
        peer_name: string | null;
        peer_network: string | null;
        owner_to_peer: number;
        peer_to_owner: number;
        observed_count: number;
        itm_path_loss_db: number | null;
        itm_viable: boolean | null;
        force_viable: boolean;
        last_observed: string | null;
      }>(
        `SELECT
           CASE WHEN nl.node_a_id = meshcore_canonical_node_id($1)
                THEN nl.node_b_id ELSE nl.node_a_id END AS peer_node_id,
           peer.name AS peer_name,
           peer.network AS peer_network,
           CASE WHEN nl.node_a_id = meshcore_canonical_node_id($1)
                THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS owner_to_peer,
           CASE WHEN nl.node_a_id = meshcore_canonical_node_id($1)
                THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS peer_to_owner,
           nl.observed_count,
           nl.itm_path_loss_db,
           nl.itm_viable,
           nl.force_viable,
           nl.last_observed::text AS last_observed
         FROM node_identity_links nl
         JOIN node_identity_nodes peer ON peer.node_id = CASE
           WHEN nl.node_a_id = meshcore_canonical_node_id($1) THEN nl.node_b_id
           ELSE nl.node_a_id
         END
         WHERE (nl.node_a_id = meshcore_canonical_node_id($1)
                OR nl.node_b_id = meshcore_canonical_node_id($1))
           AND (
             nl.force_viable = true
             OR nl.itm_viable = true
             OR (nl.itm_path_loss_db IS NOT NULL AND nl.itm_path_loss_db <= 137.5)
           )
         ORDER BY
           COALESCE(nl.itm_viable, false) DESC,
           nl.force_viable DESC,
           nl.observed_count DESC,
           nl.itm_path_loss_db ASC NULLS LAST
         LIMIT 12`,
        [selectedNodeId],
      ),
      query<{ bucket: string; adverts: number }>(
        `SELECT
           time_bucket('1 hour', time)::text AS bucket,
           COUNT(DISTINCT packet_hash)::int AS adverts
         FROM node_identity_packets
         WHERE src_node_id_raw IN (
           SELECT meshcore_canonical_node_id($1)
           UNION
           SELECT source_node_id FROM node_identity_aliases
           WHERE canonical_node_id = meshcore_canonical_node_id($1)
         )
           AND packet_type = 4
           AND time > NOW() - INTERVAL '24 hours'
         GROUP BY bucket
         ORDER BY bucket`,
        [selectedNodeId],
      ),
      query<{
        time: string;
        battery_mv: number | null;
        uptime_secs: string | null;
        tx_air_secs: string | null;
        rx_air_secs: string | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        solar_mv: number | null;
        board_temp_c: number | null;
        wifi_rssi: number | null;
        wifi_ssid: string | null;
        wifi_uptime_ms: number | null;
        ntp_synced: boolean | null;
        ntp_sync_age_ms: number | null;
        boot_count: number | null;
        reset_reason: string | null;
        max_loop_ms: number | null;
        max_loop_at_ms: number | null;
        nodes_heard_24h: number | null;
        air_util_rx: number | null;
        last_rx_rssi: number | null;
        last_rx_snr: number | null;
        tx_power_dbm: number | null;
        config_version: string | null;
        config_crc32: string | null;
        fs_free_bytes: number | null;
        fs_total_bytes: number | null;
        nvs_free_entries: number | null;
        channel_id: number | null;
        git_commit: string | null;
        boot_epoch: number | null;
        mqtt_broker_uri: string | null;
        mqtt_broker_username: string | null;
        mqtt_uptime_ms: number | null;
        mqtt_reconnect_attempts_1h: number | null;
        mqtt_session_status_publishes: number | null;
        mqtt_session_packet_publishes: number | null;
        mqtt_last_offline_epoch: number | null;
        uptime_ms: number | null;
        rx_publish_calls: number | null;
        tx_publish_calls: number | null;
      }>(
        `SELECT
           time::text AS time,
           CASE
             WHEN jsonb_typeof(stats->'battery_mv') = 'number' THEN (stats->>'battery_mv')::double precision
             ELSE battery_mv::double precision
           END AS battery_mv,
           uptime_secs::text AS uptime_secs,
           tx_air_secs::text AS tx_air_secs,
           rx_air_secs::text AS rx_air_secs,
           CASE
             WHEN jsonb_typeof(stats->'channel_utilization') = 'number' THEN (stats->>'channel_utilization')::double precision
             ELSE channel_utilization
           END AS channel_utilization,
           CASE
             WHEN jsonb_typeof(stats->'air_util_tx') = 'number' THEN (stats->>'air_util_tx')::double precision
             ELSE air_util_tx
           END AS air_util_tx,
           CASE WHEN jsonb_typeof(stats->'solar_mv') = 'number' THEN (stats->>'solar_mv')::double precision ELSE NULL END AS solar_mv,
           CASE WHEN jsonb_typeof(stats->'board_temp_c') = 'number' THEN (stats->>'board_temp_c')::double precision ELSE NULL END AS board_temp_c,
           CASE WHEN jsonb_typeof(stats->'wifi_rssi') = 'number' THEN (stats->>'wifi_rssi')::double precision ELSE NULL END AS wifi_rssi,
           CASE WHEN jsonb_typeof(stats->'wifi_ssid') = 'string' THEN stats->>'wifi_ssid' ELSE NULL END AS wifi_ssid,
           CASE WHEN jsonb_typeof(stats->'wifi_uptime_ms') = 'number' THEN (stats->>'wifi_uptime_ms')::double precision ELSE NULL END AS wifi_uptime_ms,
           CASE WHEN jsonb_typeof(stats->'ntp_synced') = 'boolean' THEN (stats->>'ntp_synced')::boolean ELSE NULL END AS ntp_synced,
           CASE WHEN jsonb_typeof(stats->'ntp_sync_age_ms') = 'number' THEN (stats->>'ntp_sync_age_ms')::double precision ELSE NULL END AS ntp_sync_age_ms,
           CASE WHEN jsonb_typeof(stats->'boot_count') = 'number' THEN (stats->>'boot_count')::double precision ELSE NULL END AS boot_count,
           CASE WHEN jsonb_typeof(stats->'reset_reason') = 'string' THEN stats->>'reset_reason' ELSE NULL END AS reset_reason,
           CASE WHEN jsonb_typeof(stats->'max_loop_ms') = 'number' THEN (stats->>'max_loop_ms')::double precision ELSE NULL END AS max_loop_ms,
           CASE WHEN jsonb_typeof(stats->'max_loop_at_ms') = 'number' THEN (stats->>'max_loop_at_ms')::double precision ELSE NULL END AS max_loop_at_ms,
           CASE WHEN jsonb_typeof(stats->'nodes_heard_24h') = 'number' THEN (stats->>'nodes_heard_24h')::double precision ELSE NULL END AS nodes_heard_24h,
           CASE WHEN jsonb_typeof(stats->'air_util_rx') = 'number' THEN (stats->>'air_util_rx')::double precision ELSE NULL END AS air_util_rx,
           CASE WHEN jsonb_typeof(stats->'last_rx_rssi') = 'number' THEN (stats->>'last_rx_rssi')::double precision ELSE NULL END AS last_rx_rssi,
           CASE WHEN jsonb_typeof(stats->'last_rx_snr') = 'number' THEN (stats->>'last_rx_snr')::double precision ELSE NULL END AS last_rx_snr,
           CASE WHEN jsonb_typeof(stats->'tx_power_dbm') = 'number' THEN (stats->>'tx_power_dbm')::double precision ELSE NULL END AS tx_power_dbm,
           CASE WHEN jsonb_typeof(stats->'config_version') = 'string' THEN stats->>'config_version' ELSE NULL END AS config_version,
           CASE WHEN jsonb_typeof(stats->'config_crc32') IN ('string', 'number') THEN stats->>'config_crc32' ELSE NULL END AS config_crc32,
           CASE WHEN jsonb_typeof(stats->'fs_free_bytes') = 'number' THEN (stats->>'fs_free_bytes')::double precision ELSE NULL END AS fs_free_bytes,
           CASE WHEN jsonb_typeof(stats->'fs_total_bytes') = 'number' THEN (stats->>'fs_total_bytes')::double precision ELSE NULL END AS fs_total_bytes,
           CASE WHEN jsonb_typeof(stats->'nvs_free_entries') = 'number' THEN (stats->>'nvs_free_entries')::double precision ELSE NULL END AS nvs_free_entries,
           CASE WHEN jsonb_typeof(stats->'channel_id') = 'number' THEN (stats->>'channel_id')::double precision ELSE NULL END AS channel_id,
           CASE WHEN jsonb_typeof(stats->'git_commit') = 'string' THEN stats->>'git_commit' ELSE NULL END AS git_commit,
           CASE WHEN jsonb_typeof(stats->'boot_epoch') = 'number' THEN (stats->>'boot_epoch')::double precision ELSE NULL END AS boot_epoch,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'broker_uri') = 'string'
               THEN stats->'mqtt'->>'broker_uri'
             ELSE NULL
           END AS mqtt_broker_uri,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'broker_username') = 'string'
               THEN stats->'mqtt'->>'broker_username'
             ELSE NULL
           END AS mqtt_broker_username,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'uptime_ms') = 'number'
               THEN (stats->'mqtt'->>'uptime_ms')::double precision
             ELSE NULL
           END AS mqtt_uptime_ms,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'reconnect_attempts_1h') = 'number'
               THEN (stats->'mqtt'->>'reconnect_attempts_1h')::double precision
             ELSE NULL
           END AS mqtt_reconnect_attempts_1h,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'session_status_publishes') = 'number'
               THEN (stats->'mqtt'->>'session_status_publishes')::double precision
             ELSE NULL
           END AS mqtt_session_status_publishes,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'session_packet_publishes') = 'number'
               THEN (stats->'mqtt'->>'session_packet_publishes')::double precision
             ELSE NULL
           END AS mqtt_session_packet_publishes,
           CASE
             WHEN jsonb_typeof(stats->'mqtt') = 'object' AND jsonb_typeof(stats->'mqtt'->'last_offline_epoch') = 'number'
               THEN (stats->'mqtt'->>'last_offline_epoch')::double precision
             ELSE NULL
           END AS mqtt_last_offline_epoch,
           CASE
             WHEN jsonb_typeof(stats->'uptime_ms') = 'number' THEN (stats->>'uptime_ms')::double precision
             ELSE NULL
           END AS uptime_ms,
           CASE
             WHEN jsonb_typeof(stats->'rx_publish_calls') = 'number' THEN (stats->>'rx_publish_calls')::double precision
             ELSE NULL
           END AS rx_publish_calls,
           CASE
             WHEN jsonb_typeof(stats->'tx_publish_calls') = 'number' THEN (stats->>'tx_publish_calls')::double precision
             ELSE NULL
           END AS tx_publish_calls
         FROM node_identity_status_samples
         WHERE node_id = meshcore_canonical_node_id($1)
           AND network = COALESCE(
             (SELECT network FROM node_identity_nodes
              WHERE node_id = meshcore_canonical_node_id($1)
              LIMIT 1),
             'ukmesh'
           )
           AND time > NOW() - INTERVAL '24 hours'
        ORDER BY time ASC`,
        [selectedNodeId],
      ),
      query<{
        id: string;
        rssi: number | null;
        snr: number | null;
        last_seen: string | null;
        sample_time: string;
      }>(
        `WITH latest AS (
           SELECT time, neighbors
           FROM node_neighbor_samples
           WHERE meshcore_canonical_node_id(node_id) = meshcore_canonical_node_id($1)
             AND network = COALESCE(
               (SELECT network FROM node_identity_nodes
                WHERE node_id = meshcore_canonical_node_id($1)
                LIMIT 1),
               'ukmesh'
             )
           ORDER BY time DESC
           LIMIT 1
         )
         SELECT
           COALESCE(item->>'id', item->>'node_id', item->>'pubkey', item->>'public_key') AS id,
           CASE
             WHEN jsonb_typeof(item->'rssi') = 'number' THEN (item->>'rssi')::double precision
             WHEN jsonb_typeof(item->'RSSI') = 'number' THEN (item->>'RSSI')::double precision
             ELSE NULL
           END AS rssi,
           CASE
             WHEN jsonb_typeof(item->'snr') = 'number' THEN (item->>'snr')::double precision
             WHEN jsonb_typeof(item->'SNR') = 'number' THEN (item->>'SNR')::double precision
             ELSE NULL
           END AS snr,
           COALESCE(
             CASE WHEN jsonb_typeof(item->'last_seen') IN ('string', 'number') THEN item->>'last_seen' ELSE NULL END,
             CASE WHEN jsonb_typeof(item->'lastSeen') IN ('string', 'number') THEN item->>'lastSeen' ELSE NULL END
           ) AS last_seen,
           latest.time::text AS sample_time
         FROM latest
         CROSS JOIN LATERAL jsonb_array_elements(
           CASE WHEN jsonb_typeof(latest.neighbors) = 'array' THEN latest.neighbors ELSE '[]'::jsonb END
         ) AS neighbor(item)
         WHERE COALESCE(item->>'id', item->>'node_id', item->>'pubkey', item->>'public_key') IS NOT NULL
         ORDER BY last_seen DESC NULLS LAST
         LIMIT 32`,
        [selectedNodeId],
      ),
      query<{ packets_24h: number }>(
        `SELECT COUNT(*)::int AS packets_24h
         FROM node_identity_packets
         WHERE src_node_id_raw IN (
           SELECT meshcore_canonical_node_id($1)
           UNION
           SELECT source_node_id FROM node_identity_aliases
           WHERE canonical_node_id = meshcore_canonical_node_id($1)
         )
           AND time > NOW() - INTERVAL '24 hours'`,
        [selectedNodeId],
      ),
      query<{ packets_24h: number }>(
        `SELECT COUNT(*)::int AS packets_24h
         FROM node_identity_packets
         WHERE rx_node_id_raw IN (
           SELECT meshcore_canonical_node_id($1)
           UNION
           SELECT source_node_id FROM node_identity_aliases
           WHERE canonical_node_id = meshcore_canonical_node_id($1)
         )
           AND time > NOW() - INTERVAL '24 hours'`,
        [selectedNodeId],
      ),
    ]);

    return {
      ownerNodeResult,
      incomingResult,
      packetResult,
      heardByResult,
      linkHealthResult,
      advertTrendResult,
      telemetryResult,
      heardNeighborsResult,
      packetsSentResult,
      packetsReceivedResult,
    };
  }

  return {
    fetchOwnerLiveData,
    fetchLastHopStrength,
  };
}
