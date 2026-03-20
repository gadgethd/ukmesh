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

  async function fetchLastHopStrength(ownerNodeIds: string[]) {
    return query<{
      bucket: string;
      last_hop_node_id: string | null;
      last_hop_name: string;
      resolution: 'direct' | 'resolved' | 'inferred' | 'unresolved';
      avg_snr: number | null;
      avg_rssi: number | null;
      sample_count: number;
    }>(
      `WITH owner_packets AS (
         SELECT
           p.time,
           p.packet_hash,
           p.rx_node_id,
           p.src_node_id,
           p.hop_count,
           p.rssi,
           p.snr,
           CASE
             WHEN COALESCE(array_length(p.path_hashes, 1), 0) > 0
           THEN UPPER(p.path_hashes[array_length(p.path_hashes, 1)])
           ELSE NULL
           END AS receiver_side_hash
         FROM packets p
         WHERE p.rx_node_id = ANY($1::text[])
           AND p.time > NOW() - INTERVAL '7 days'
           AND (p.snr IS NOT NULL OR p.rssi IS NOT NULL)
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
         JOIN nodes n
           ON (n.role IS NULL OR n.role = 2)
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
         JOIN nodes rx ON rx.node_id = uh.rx_node_id
         JOIN nodes n
           ON (n.role IS NULL OR n.role = 2)
          AND UPPER(LEFT(n.node_id, 2)) = UPPER(LEFT(uh.receiver_side_hash, 2))
         LEFT JOIN node_links nl
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
             WHEN op.hop_count = 0 AND op.src_node_id IS NOT NULL AND NOT (op.src_node_id = ANY($1::text[])) THEN op.src_node_id
             WHEN rl.match_count = 1 AND rl.rn = 1 THEN rl.node_id
             WHEN ilh.rn = 1 THEN ilh.node_id
             ELSE NULL
           END AS last_hop_node_id,
           CASE
             WHEN op.hop_count = 0 AND op.src_node_id IS NOT NULL AND NOT (op.src_node_id = ANY($1::text[])) THEN COALESCE(src.name, op.src_node_id)
             WHEN rl.match_count = 1 AND rl.rn = 1 THEN rl.name
             WHEN ilh.rn = 1 THEN ilh.name
             ELSE 'Unresolved'
           END AS last_hop_name,
           CASE
             WHEN op.hop_count = 0 AND op.src_node_id IS NOT NULL AND NOT (op.src_node_id = ANY($1::text[])) THEN 'direct'
             WHEN rl.match_count = 1 AND rl.rn = 1 THEN 'resolved'
             WHEN ilh.rn = 1 THEN 'inferred'
             ELSE 'unresolved'
           END AS resolution,
           op.snr,
           op.rssi
         FROM owner_packets op
         LEFT JOIN nodes src ON src.node_id = op.src_node_id
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
      [ownerNodeIds],
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
      }>(
        `SELECT node_id, name, network, iata, advert_count, last_seen, lat, lon, role
         FROM nodes
         WHERE node_id = $1
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
         FROM packets p
         LEFT JOIN nodes n ON n.node_id = p.src_node_id
         WHERE p.rx_node_id = $1
           AND p.hop_count = 0
           AND p.src_node_id IS NOT NULL
           AND p.src_node_id <> $1
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
        `WITH ranked AS (
           SELECT
             p.time,
             p.packet_type,
             p.route_type,
             p.hop_count,
             p.packet_hash,
             p.src_node_id,
             p.payload,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(
                 p.packet_hash,
                 CONCAT_WS(':',
                   COALESCE(p.src_node_id, ''),
                   COALESCE(p.packet_type::text, ''),
                   COALESCE(p.route_type::text, ''),
                   COALESCE(p.hop_count::text, ''),
                   COALESCE(p.payload->'decrypted'->>'sender', ''),
                   COALESCE(p.payload->'decrypted'->>'text', ''),
                   DATE_TRUNC('second', p.time)::text
                 )
               )
               ORDER BY p.time DESC
             ) AS rn
           FROM packets p
           WHERE p.rx_node_id = $1
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
         LEFT JOIN nodes src ON src.node_id = r.src_node_id
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
         FROM packets p
         LEFT JOIN nodes n ON n.node_id = p.rx_node_id
         WHERE p.src_node_id = $1
           AND p.rx_node_id IS NOT NULL
           AND p.rx_node_id <> $1
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
           CASE WHEN nl.node_a_id = $1 THEN nl.node_b_id ELSE nl.node_a_id END AS peer_node_id,
           peer.name AS peer_name,
           peer.network AS peer_network,
           CASE WHEN nl.node_a_id = $1 THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS owner_to_peer,
           CASE WHEN nl.node_a_id = $1 THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS peer_to_owner,
           nl.observed_count,
           nl.itm_path_loss_db,
           nl.itm_viable,
           nl.force_viable,
           nl.last_observed::text AS last_observed
         FROM node_links nl
         JOIN nodes peer ON peer.node_id = CASE WHEN nl.node_a_id = $1 THEN nl.node_b_id ELSE nl.node_a_id END
         WHERE (nl.node_a_id = $1 OR nl.node_b_id = $1)
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
         FROM packets
         WHERE src_node_id = $1
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
        uptime_ms: number | null;
        rx_publish_calls: number | null;
        tx_publish_calls: number | null;
      }>(
        `SELECT
           time::text AS time,
           battery_mv,
           uptime_secs::text AS uptime_secs,
           tx_air_secs::text AS tx_air_secs,
           rx_air_secs::text AS rx_air_secs,
           channel_utilization,
           air_util_tx,
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
         FROM node_status_samples
         WHERE node_id = $1
           AND time > NOW() - INTERVAL '24 hours'
        ORDER BY time ASC`,
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
    };
  }

  return {
    fetchOwnerLiveData,
    fetchLastHopStrength,
  };
}
