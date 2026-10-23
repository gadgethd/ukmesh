import type { QueryResultRow } from 'pg';
import { nodeAliasArraySql, type NetworkFilters } from '../api/utils/networkFilters.js';
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
  last_observed: string | null;
  recent_count_72h: number;
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
    search?: string,
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

    async listPublicMapRows(fields, filters, snapshot, cursor, limit, search) {
      const snapshotParameter = filters.params.length + 1;
      const cursorParameter = snapshotParameter + 1;
      const limitParameter = cursorParameter + 1;
      const searchParameter = `$${limitParameter + 1}`;
      const searchCondition = search ? `AND (
        STRPOS(LOWER(COALESCE(n.name, '')), LOWER(${searchParameter})) > 0
        OR STRPOS(LOWER(n.node_id), LOWER(${searchParameter})) > 0
        OR STRPOS(LOWER(COALESCE(n.iata, '')), LOWER(${searchParameter})) > 0
        OR EXISTS (
          SELECT 1 FROM node_name_history history
          JOIN nodes name_source ON name_source.node_id = history.node_id
          WHERE history.node_id = ANY(n.identity_source_ids)
            AND history.name NOT LIKE '%🚫%'
            AND (name_source.name IS NULL OR name_source.name NOT LIKE '%🚫%')
            AND (name_source.network IS DISTINCT FROM 'test' OR n.network = 'test')
            AND STRPOS(LOWER(history.name), LOWER(${searchParameter})) > 0
        )
      )` : '';
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
            ${searchCondition}
            AND ($${cursorParameter}::text IS NULL OR n.node_id > $${cursorParameter})
          ORDER BY n.node_id
          LIMIT $${limitParameter}`,
        [...filters.params, snapshot, cursor, limit + 1, ...(search ? [search] : [])],
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
      const links = (await query<Omit<NodeLinkRow, 'recent_count_72h'>>(
        `WITH requested_identity AS MATERIALIZED (
           SELECT COALESCE(
             (SELECT canonical_node_id FROM node_identity_aliases
               WHERE source_node_id = UPPER(BTRIM(${idParam}))),
             UPPER(BTRIM(${idParam}))
           ) AS node_id
         ), link_nodes AS MATERIALIZED (
           SELECT node_id, name, network FROM node_identity_nodes
         ), source_node AS MATERIALIZED (
           SELECT node_id
             FROM link_nodes sn
            WHERE node_id = (SELECT node_id FROM requested_identity)
              AND (name IS NULL OR name NOT LIKE '%🚫%')
              ${filters.nodesAlias('sn')}
         ),
         relevant_links AS MATERIALIZED (
           SELECT
             CASE WHEN nl.node_a_id = (SELECT node_id FROM requested_identity)
                  THEN nl.node_b_id ELSE nl.node_a_id END AS peer_id,
             nl.observed_count,
             nl.last_observed,
             nl.itm_path_loss_db,
             CASE WHEN nl.node_a_id = (SELECT node_id FROM requested_identity)
                  THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS count_this_to_peer,
             CASE WHEN nl.node_a_id = (SELECT node_id FROM requested_identity)
                  THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS count_peer_to_this
             FROM node_identity_links nl
            WHERE (nl.node_a_id = (SELECT node_id FROM requested_identity)
                   OR nl.node_b_id = (SELECT node_id FROM requested_identity))
              AND (nl.itm_viable = TRUE OR nl.force_viable = TRUE)
              AND EXISTS (SELECT 1 FROM source_node)
         )
         SELECT
           rl.peer_id, peer.name AS peer_name, rl.observed_count,
           rl.itm_path_loss_db, rl.count_this_to_peer, rl.count_peer_to_this,
           to_char(rl.last_observed AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS last_observed
           FROM relevant_links rl
           JOIN link_nodes peer ON peer.node_id = rl.peer_id
          WHERE (peer.name IS NULL OR peer.name NOT LIKE '%🚫%')
            ${filters.nodesAlias('peer')}
          ORDER BY rl.observed_count DESC`,
        [...filters.params, nodeId],
      )).rows;
      if (links.length === 0) return [];

      // Resolve the visible link set first. The materialized identity views are
      // expensive; share their node projection above, then keep this one packet
      // aggregate indexed and bounded independently of those view plans.
      // last_observed is the worker's latest observation in either direction,
      // not necessarily direct. Count all public packet rows for canonical src/rx
      // pairs (including relayed packets), not distinct hashes or lifetime hits.
      // The sparkline uses the same pairing, but additionally needs signal data
      // and caps its points; neither restriction applies to this 72-hour count.
      const linksParam = `$${filters.params.length + 2}`;
      return (await query<NodeLinkRow>(
        `WITH source_ids AS MATERIALIZED (
           SELECT ${nodeAliasArraySql(idParam)} AS ids
         ), visible_links AS MATERIALIZED (
           SELECT * FROM jsonb_to_recordset(${linksParam}::jsonb) AS link(
             peer_id text, peer_name text, observed_count integer,
             itm_path_loss_db double precision, count_this_to_peer integer,
             count_peer_to_this integer, last_observed text
           )
         ), peer_aliases AS MATERIALIZED (
           SELECT vl.peer_id, UNNEST(${nodeAliasArraySql('vl.peer_id')}) AS source_node_id
             FROM visible_links vl
         ), peer_ids AS MATERIALIZED (
           SELECT ARRAY(SELECT source_node_id FROM peer_aliases) AS ids
         ), recent_counts AS (
           SELECT pa.peer_id, COUNT(*)::integer AS recent_count_72h
             FROM packets p
             JOIN peer_aliases pa ON pa.source_node_id = CASE
               WHEN p.rx_node_id = ANY((SELECT ids FROM source_ids)::text[])
                 THEN p.src_node_id ELSE p.rx_node_id END
            WHERE p.time > NOW() - INTERVAL '72 hours'
              AND p.time <= NOW()
              AND p.src_node_id IS NOT NULL
              AND (
                (p.rx_node_id = ANY((SELECT ids FROM source_ids)::text[])
                  AND p.src_node_id = ANY((SELECT ids FROM peer_ids)::text[]))
                OR (p.src_node_id = ANY((SELECT ids FROM source_ids)::text[])
                  AND p.rx_node_id = ANY((SELECT ids FROM peer_ids)::text[]))
              )
              ${filters.packetsAlias('p')}
            GROUP BY pa.peer_id
         )
         SELECT vl.peer_id, vl.peer_name, vl.observed_count,
           vl.itm_path_loss_db, vl.count_this_to_peer, vl.count_peer_to_this,
           vl.last_observed,
           COALESCE(rc.recent_count_72h, 0)::integer AS recent_count_72h
           FROM visible_links vl
           LEFT JOIN recent_counts rc ON rc.peer_id = vl.peer_id
          ORDER BY vl.observed_count DESC`,
        [...filters.params, nodeId, JSON.stringify(links)],
      )).rows;
    },
  };
}
