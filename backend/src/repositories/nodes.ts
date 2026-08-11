import type { QueryResultRow } from 'pg';
import type { NetworkFilters } from '../api/utils/networkFilters.js';
import { publicMapFreshPredicate } from '../nodes/publicMap.js';
import {
  nodeEffectiveLastSeenSql,
  nodeEffectiveOnlineSql,
} from '../nodes/presence.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type InferredPacketRow = {
  packet_hash: string;
  time: string;
  path_hashes: string[] | null;
  path_hash_size_bytes: number | null;
};

export type NodeLinkRow = {
  peer_id: string;
  peer_name: string | null;
  observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number;
  count_peer_to_this: number;
};

export type TestDiagnosticsData = {
  packets: Array<Record<string, unknown>>;
  latestStatuses: Array<Record<string, unknown>>;
  statusSamples: Array<Record<string, unknown>>;
  latestStatus: Record<string, unknown> | null;
  history: Array<Record<string, unknown>>;
};

export type NodeRepository = {
  loadTestDiagnostics: () => Promise<TestDiagnosticsData>;
  listPublicMapRows: (
    fields: readonly string[],
    filters: NetworkFilters,
    snapshot: string,
    cursor: string | null,
    limit: number,
  ) => Promise<Array<Record<string, unknown>>>;
  listAllNodeIds: () => Promise<Array<{ node_id: string }>>;
  listInferredPackets: (
    scope: NetworkFilters,
    limit: number,
  ) => Promise<InferredPacketRow[]>;
  listNodeLinks: (
    nodeId: string,
    filters: NetworkFilters,
  ) => Promise<NodeLinkRow[]>;
};

export function createNodeRepository(query: QueryFn): NodeRepository {
  return {
    async loadTestDiagnostics() {
      const [packetsResult, latestStatusRows, statusSamplesResult] = await Promise.all([
        query<Record<string, unknown>>(
          `SELECT
             time::text,
             topic,
             packet_hash,
             packet_type,
             route_type,
             hop_count,
             src_node_id,
             rx_node_id,
             rssi,
             snr,
             payload,
             raw_hex,
             path_hash_size_bytes,
             path_hashes
           FROM packets
           WHERE network = 'test'
           ORDER BY time DESC
           LIMIT 2000`,
          [],
        ),
        query<Record<string, unknown>>(
          `SELECT * FROM (
             SELECT DISTINCT ON (nss.node_id)
               nss.time::text,
               nss.node_id,
               nss.network,
               nss.battery_mv,
               nss.uptime_secs,
               nss.tx_air_secs,
               nss.rx_air_secs,
               nss.channel_utilization,
               nss.air_util_tx,
               nss.stats,
               n.name,
               n.iata,
               n.hardware_model,
               n.firmware_version
             FROM node_identity_status_samples nss
             LEFT JOIN node_identity_nodes n ON n.node_id = nss.node_id
             WHERE nss.network = 'test'
             ORDER BY nss.node_id, nss.time DESC
           ) latest
           ORDER BY time DESC`,
          [],
        ),
        query<Record<string, unknown>>(
          `SELECT
             time::text,
             node_id,
             network,
             battery_mv,
             uptime_secs,
             tx_air_secs,
             rx_air_secs,
             channel_utilization,
             air_util_tx,
             stats
           FROM node_identity_status_samples
           WHERE network = 'test'
           ORDER BY time DESC`,
          [],
        ),
      ]);
      const latestStatus = latestStatusRows.rows[0] ?? null;
      const latestNodeId = typeof latestStatus?.['node_id'] === 'string'
        ? latestStatus['node_id']
        : null;
      const history = latestNodeId
        ? (await query<Record<string, unknown>>(
          `SELECT
             time::text,
             battery_mv,
             uptime_secs,
             channel_utilization,
             air_util_tx,
             CASE
               WHEN jsonb_typeof(stats->'heap_free') = 'number' THEN (stats->>'heap_free')::double precision
               ELSE NULL
             END AS heap_free,
             CASE
               WHEN jsonb_typeof(stats->'heap_min_free') = 'number' THEN (stats->>'heap_min_free')::double precision
               ELSE NULL
             END AS heap_min_free,
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
             END AS tx_publish_calls,
             CASE
               WHEN jsonb_typeof(stats->'tx_queue_depth') = 'number' THEN (stats->>'tx_queue_depth')::double precision
               ELSE NULL
             END AS tx_queue_depth,
             CASE
               WHEN jsonb_typeof(stats->'tx_queue_depth_peak') = 'number' THEN (stats->>'tx_queue_depth_peak')::double precision
               ELSE NULL
             END AS tx_queue_depth_peak
           FROM node_identity_status_samples
           WHERE node_id = $1
             AND network = 'test'
             AND time > NOW() - INTERVAL '24 hours'
           ORDER BY time ASC`,
          [latestNodeId],
        )).rows
        : [];
      return {
        packets: packetsResult.rows,
        latestStatuses: latestStatusRows.rows,
        statusSamples: statusSamplesResult.rows,
        latestStatus,
        history,
      };
    },

    async listPublicMapRows(fields, filters, snapshot, cursor, limit) {
      const snapshotParameter = filters.params.length + 1;
      const cursorParameter = snapshotParameter + 1;
      const limitParameter = cursorParameter + 1;
      const selectedFields = fields
        .map((field) => {
          if (field === 'last_seen') {
            return `${nodeEffectiveLastSeenSql('n')}::text AS last_seen`;
          }
          if (field === 'is_online') {
            return `${nodeEffectiveOnlineSql('n', `$${snapshotParameter}::timestamptz`)} AS is_online`;
          }
          return `n.${field}`;
        })
        .join(', ');
      const result = await query<Record<string, unknown>>(
        `SELECT ${selectedFields}
           FROM node_identity_nodes n
          WHERE ${publicMapFreshPredicate('n', `$${snapshotParameter}::timestamptz`)}
            ${filters.nodesAlias('n')}
            AND ($${cursorParameter}::text IS NULL OR n.node_id > $${cursorParameter})
          ORDER BY n.node_id
          LIMIT $${limitParameter}`,
        [...filters.params, snapshot, cursor, limit + 1],
      );
      return result.rows;
    },

    async listAllNodeIds() {
      return (await query<{ node_id: string }>('SELECT node_id FROM node_identity_nodes')).rows;
    },

    async listInferredPackets(scope, limit) {
      return (await query<InferredPacketRow>(
        `SELECT p.packet_hash, p.time::text, p.path_hashes, p.path_hash_size_bytes
           FROM packets p
          WHERE p.time > NOW() - INTERVAL '7 days'
            ${scope.packetsAlias('p')}
            AND p.path_hash_size_bytes > 1
            AND p.path_hashes IS NOT NULL
            AND array_length(p.path_hashes, 1) > 0
          ORDER BY p.time DESC
          LIMIT $${scope.params.length + 1}`,
        [...scope.params, limit],
      )).rows;
    },

    async listNodeLinks(nodeId, filters) {
      const idParam = `$${filters.params.length + 1}`;
      return (await query<NodeLinkRow>(
        `WITH source_node AS MATERIALIZED (
           SELECT node_id
             FROM node_identity_nodes sn
            WHERE node_id = meshcore_canonical_node_id(${idParam})
              AND (name IS NULL OR name NOT LIKE '%🚫%')
              ${filters.nodesAlias('sn')}
         ),
         relevant_links AS MATERIALIZED (
           SELECT
             CASE WHEN nl.node_a_id = meshcore_canonical_node_id(${idParam})
                  THEN nl.node_b_id ELSE nl.node_a_id END AS peer_id,
             nl.observed_count,
             nl.itm_path_loss_db,
             CASE WHEN nl.node_a_id = meshcore_canonical_node_id(${idParam})
                  THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS count_this_to_peer,
             CASE WHEN nl.node_a_id = meshcore_canonical_node_id(${idParam})
                  THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS count_peer_to_this
             FROM node_identity_links nl
            WHERE (nl.node_a_id = meshcore_canonical_node_id(${idParam})
                   OR nl.node_b_id = meshcore_canonical_node_id(${idParam}))
              AND (nl.itm_viable = TRUE OR nl.force_viable = TRUE)
              AND EXISTS (SELECT 1 FROM source_node)
         )
         SELECT
           rl.peer_id, peer.name AS peer_name, rl.observed_count,
           rl.itm_path_loss_db, rl.count_this_to_peer, rl.count_peer_to_this
           FROM relevant_links rl
           JOIN node_identity_nodes peer ON peer.node_id = rl.peer_id
          WHERE (peer.name IS NULL OR peer.name NOT LIKE '%🚫%')
            ${filters.nodesAlias('peer')}
          ORDER BY rl.observed_count DESC`,
        [...filters.params, nodeId],
      )).rows;
    },
  };
}
