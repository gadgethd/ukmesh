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
  };
}
