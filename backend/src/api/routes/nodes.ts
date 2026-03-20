import type { Request, Response, Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import { normalizeObserverQuery } from '../utils/observer.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type NodeRecord = {
  node_id: string;
  lat?: number | null;
  lon?: number | null;
  role?: number | null;
  name?: string | null;
};

type GetNodesFn = (network?: string, observer?: string) => Promise<NodeRecord[]>;
type GetNodeHistoryFn = (nodeId: string, hours: number) => Promise<unknown>;
type GetNodeAdvertsFn = (publicKey: string, hours: number) => Promise<unknown>;
type RequireLocalOnlyFn = (req: Request, res: Response) => boolean;

type NetworkFilters = {
  params: string[];
  packetsAlias: (alias: string) => string;
};

type InferredMultibyteNode = {
  node_id: string;
  name: string;
  lat: number;
  lon: number;
  last_seen: string;
  is_online: boolean;
  role: number;
  inferred_prefix: string;
  inferred_hash_size_bytes: number;
  inferred_observations: number;
  inferred_packet_count: number;
  inferred_prev_name?: string | null;
  inferred_next_name?: string | null;
};

type InferredActiveResponse = {
  inferredNodes: InferredMultibyteNode[];
  inferredActiveNodeIds: string[];
};

type NodesRouteDeps = {
  getNodes: GetNodesFn;
  getNodeHistory: GetNodeHistoryFn;
  getNodeAdverts: GetNodeAdvertsFn;
  query: QueryFn;
  requireLocalOnly: RequireLocalOnlyFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  inferredNodesCache: Map<string, { ts: number; data: unknown }>;
  inferredNodesCacheTtlMs: number;
};

export function registerNodeRoutes(router: Router, deps: NodesRouteDeps): void {
  const {
    getNodes,
    getNodeHistory,
    getNodeAdverts,
    query,
    requireLocalOnly,
    networkFilters,
    inferredNodesCache,
    inferredNodesCacheTtlMs,
  } = deps;

  router.get('/local/test-diagnostics', async (req, res) => {
    try {
      if (!requireLocalOnly(req, res)) return;

      const [nodes, packetsResult, latestStatusRows, statusSamplesResult] = await Promise.all([
        getNodes('test'),
        query<{
          time: string;
          topic: string;
          packet_hash: string | null;
          packet_type: number | null;
          route_type: number | null;
          hop_count: number | null;
          src_node_id: string | null;
          rx_node_id: string | null;
          rssi: number | null;
          snr: number | null;
          payload: Record<string, unknown> | null;
          raw_hex: string | null;
          path_hash_size_bytes: number | null;
          path_hashes: string[] | null;
        }>(
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
           ORDER BY time DESC`,
          [],
        ),
        query<{
          time: string;
          node_id: string;
          network: string | null;
          battery_mv: number | null;
          uptime_secs: number | null;
          tx_air_secs: number | null;
          rx_air_secs: number | null;
          channel_utilization: number | null;
          air_util_tx: number | null;
          stats: Record<string, unknown> | null;
          name: string | null;
          iata: string | null;
          hardware_model: string | null;
          firmware_version: string | null;
        }>(
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
             FROM node_status_samples nss
             LEFT JOIN nodes n ON n.node_id = nss.node_id
             WHERE nss.network = 'test'
             ORDER BY nss.node_id, nss.time DESC
           ) latest
           ORDER BY time DESC`,
          [],
        ),
        query<{
          time: string;
          node_id: string;
          network: string | null;
          battery_mv: number | null;
          uptime_secs: number | null;
          tx_air_secs: number | null;
          rx_air_secs: number | null;
          channel_utilization: number | null;
          air_util_tx: number | null;
          stats: Record<string, unknown> | null;
        }>(
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
           FROM node_status_samples
           WHERE network = 'test'
           ORDER BY time DESC`,
          [],
        ),
      ]);

      const packets = packetsResult.rows;
      const latestStatuses = latestStatusRows.rows;
      const statusSamples = statusSamplesResult.rows;
      const latestStatus = latestStatusRows.rows[0] ?? null;
      let history: unknown[] = [];
      if (latestStatus?.node_id) {
        const historyRows = await query<{
          time: string;
          battery_mv: number | null;
          uptime_secs: number | null;
          channel_utilization: number | null;
          air_util_tx: number | null;
          heap_free: number | null;
          heap_min_free: number | null;
          uptime_ms: number | null;
          rx_publish_calls: number | null;
          tx_publish_calls: number | null;
          tx_queue_depth: number | null;
          tx_queue_depth_peak: number | null;
        }>(
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
           FROM node_status_samples
           WHERE node_id = $1
             AND network = 'test'
             AND time > NOW() - INTERVAL '24 hours'
           ORDER BY time ASC`,
          [latestStatus.node_id],
        );
        history = historyRows.rows;
      }

      res.json({
        network: 'test',
        nodes,
        packets,
        latestStatuses,
        statusSamples,
        latestStatus,
        history,
      });
    } catch (err) {
      console.error('[api] GET /local/test-diagnostics', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes', async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const observer = normalizeObserverQuery(req.query['observer']);
      const nodes = await getNodes(network, observer);
      res.json(nodes);
    } catch (err) {
      console.error('[api] GET /nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/inferred-nodes', async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const observer = normalizeObserverQuery(req.query['observer']);
      const scope = networkFilters(network, observer);

      const inferredCacheKey = `${network ?? 'all'}:${observer ?? ''}`;
      const inferredCached = inferredNodesCache.get(inferredCacheKey);
      if (inferredCached && Date.now() - inferredCached.ts < inferredNodesCacheTtlMs) {
        res.json(inferredCached.data);
        return;
      }

      const [visibleNodes, allNodeIds, packetsResult] = await Promise.all([
        getNodes(network, observer),
        query<{ node_id: string }>('SELECT node_id FROM nodes'),
        query<{
          packet_hash: string;
          time: string;
          path_hashes: string[] | null;
          path_hash_size_bytes: number | null;
        }>(
          `SELECT p.packet_hash, p.time::text, p.path_hashes, p.path_hash_size_bytes
           FROM packets p
           WHERE p.time > NOW() - INTERVAL '7 days'
             ${scope.packetsAlias('p')}
             AND p.path_hash_size_bytes > 1
             AND p.path_hashes IS NOT NULL
             AND array_length(p.path_hashes, 1) > 0
           ORDER BY p.time DESC`,
          scope.params,
        ),
      ]);

      const exactNodes = visibleNodes.filter((node) =>
        node.role === undefined || node.role === 2,
      );

      const inferredUnknowns = new Map<string, {
        prefix: string;
        hashSizeBytes: number;
        packetHashes: Set<string>;
        observations: number;
        latestSeen: string;
        sumLat: number;
        sumLon: number;
        prevNameCounts: Map<string, number>;
        nextNameCounts: Map<string, number>;
      }>();
      const inferredKnowns = new Map<string, {
        nodeId: string;
        packetHashes: Set<string>;
        observations: number;
        latestSeen: string;
      }>();

      const exactMatch = (pathHash: string) => {
        const normalized = pathHash.toUpperCase();
        const matches = exactNodes.filter((node) =>
          typeof node.lat === 'number'
          && typeof node.lon === 'number'
          && node.node_id.toUpperCase().startsWith(normalized),
        );
        return matches.length === 1 ? matches[0] : null;
      };

      for (const row of packetsResult.rows) {
        const pathHashes = Array.isArray(row.path_hashes) ? row.path_hashes : [];
        if (pathHashes.length < 3) continue;
        const hashSizeBytes = Number(row.path_hash_size_bytes ?? 0);
        if (hashSizeBytes < 2 || hashSizeBytes > 3) continue;

        for (let idx = 1; idx < pathHashes.length - 1; idx += 1) {
          const current = pathHashes[idx];
          const prev = pathHashes[idx - 1];
          const next = pathHashes[idx + 1];
          if (!current || !prev || !next) continue;
          if (current.length !== hashSizeBytes * 2) continue;

          const currentMatch = exactMatch(current);
          if (currentMatch) {
            const key = currentMatch.node_id;
            const existing = inferredKnowns.get(key) ?? {
              nodeId: currentMatch.node_id,
              packetHashes: new Set<string>(),
              observations: 0,
              latestSeen: row.time,
            };
            existing.packetHashes.add(row.packet_hash);
            existing.observations += 1;
            existing.latestSeen = existing.latestSeen > row.time ? existing.latestSeen : row.time;
            inferredKnowns.set(key, existing);
            continue;
          }

          const prevMatch = exactMatch(prev);
          const nextMatch = exactMatch(next);
          if (!prevMatch || !nextMatch) continue;

          const estimateLat = (Number(prevMatch.lat) + Number(nextMatch.lat)) / 2;
          const estimateLon = (Number(prevMatch.lon) + Number(nextMatch.lon)) / 2;
          const key = `${hashSizeBytes}:${current.toUpperCase()}`;
          const existing = inferredUnknowns.get(key) ?? {
            prefix: current.toUpperCase(),
            hashSizeBytes,
            packetHashes: new Set<string>(),
            observations: 0,
            latestSeen: row.time,
            sumLat: 0,
            sumLon: 0,
            prevNameCounts: new Map<string, number>(),
            nextNameCounts: new Map<string, number>(),
          };

          existing.packetHashes.add(row.packet_hash);
          existing.observations += 1;
          existing.latestSeen = existing.latestSeen > row.time ? existing.latestSeen : row.time;
          existing.sumLat += estimateLat;
          existing.sumLon += estimateLon;
          const prevLabel = prevMatch.name?.trim() || prevMatch.node_id.slice(0, 8);
          const nextLabel = nextMatch.name?.trim() || nextMatch.node_id.slice(0, 8);
          existing.prevNameCounts.set(prevLabel, (existing.prevNameCounts.get(prevLabel) ?? 0) + 1);
          existing.nextNameCounts.set(nextLabel, (existing.nextNameCounts.get(nextLabel) ?? 0) + 1);
          inferredUnknowns.set(key, existing);
        }
      }

      const bestLabel = (counts: Map<string, number>): string | null => {
        let best: string | null = null;
        let bestCount = -1;
        for (const [label, count] of counts) {
          if (count > bestCount) {
            best = label;
            bestCount = count;
          }
        }
        return best;
      };

      const knownNodeIdSet = new Set(allNodeIds.rows.map((n) => n.node_id.toUpperCase()));
      const inferredNodes: InferredMultibyteNode[] = Array.from(inferredUnknowns.values())
        .filter((entry) => entry.packetHashes.size >= 2
          && !Array.from(knownNodeIdSet).some((id) => id.startsWith(entry.prefix.toUpperCase())))
        .map((entry) => ({
          node_id: `inferred:${entry.hashSizeBytes}:${entry.prefix}`,
          name: `Inferred ${entry.prefix}`,
          lat: entry.sumLat / entry.observations,
          lon: entry.sumLon / entry.observations,
          last_seen: new Date(entry.latestSeen).toISOString(),
          is_online: true,
          role: 2,
          inferred_prefix: entry.prefix,
          inferred_hash_size_bytes: entry.hashSizeBytes,
          inferred_observations: entry.observations,
          inferred_packet_count: entry.packetHashes.size,
          inferred_prev_name: bestLabel(entry.prevNameCounts),
          inferred_next_name: bestLabel(entry.nextNameCounts),
        }))
        .sort((a, b) => (
          b.inferred_packet_count - a.inferred_packet_count
          || b.inferred_observations - a.inferred_observations
          || b.last_seen.localeCompare(a.last_seen)
        ));
      const inferredActiveNodeIds = Array.from(inferredKnowns.values())
        .filter((entry) => entry.packetHashes.size >= 2)
        .sort((a, b) => (
          b.packetHashes.size - a.packetHashes.size
          || b.observations - a.observations
          || b.latestSeen.localeCompare(a.latestSeen)
        ))
        .map((entry) => entry.nodeId);

      const payload: InferredActiveResponse = {
        inferredNodes,
        inferredActiveNodeIds,
      };
      inferredNodesCache.set(inferredCacheKey, { ts: Date.now(), data: payload });
      res.json(payload);
    } catch (err) {
      console.error('[api] GET /inferred-nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes/:id/links', async (req, res) => {
    try {
      const id = req.params['id']!;
      if (!/^[0-9a-fA-F]{64}$/.test(id)) {
        res.status(400).json({ error: 'Invalid node ID format' });
        return;
      }
      const result = await query<{
        peer_id: string;
        peer_name: string | null;
        observed_count: number;
        itm_path_loss_db: number | null;
        count_this_to_peer: number;
        count_peer_to_this: number;
      }>(
        `SELECT
           CASE WHEN node_a_id = $1 THEN node_b_id ELSE node_a_id END AS peer_id,
           n.name AS peer_name,
           observed_count,
           itm_path_loss_db,
           CASE WHEN node_a_id = $1 THEN count_a_to_b ELSE count_b_to_a END AS count_this_to_peer,
           CASE WHEN node_a_id = $1 THEN count_b_to_a ELSE count_a_to_b END AS count_peer_to_this
         FROM node_links
         LEFT JOIN nodes n ON n.node_id = CASE WHEN node_a_id = $1 THEN node_b_id ELSE node_a_id END
         WHERE (node_a_id = $1 OR node_b_id = $1)
           AND (itm_viable = true OR force_viable = true)
         ORDER BY observed_count DESC`,
        [id],
      );
      res.json(result.rows);
    } catch (err) {
      console.error('[api] GET /nodes/:id/links', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes/:id/history', async (req, res) => {
    try {
      const id = req.params['id']!;
      if (!/^[0-9a-fA-F]{64}$/.test(id)) {
        res.status(400).json({ error: 'Invalid node ID format' });
        return;
      }
      const hours = Math.min(Number(req.query['hours'] ?? 24), 672);
      const history = await getNodeHistory(id, hours);
      res.json(history);
    } catch (err) {
      console.error('[api] GET /nodes/:id/history', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes/:id/adverts', async (req, res) => {
    try {
      const publicKey = req.params['id']!;
      if (!/^[0-9a-fA-F]{64}$/.test(publicKey)) {
        res.status(400).json({ error: 'Invalid public key format' });
        return;
      }
      const hours = Math.min(Number(req.query['hours'] ?? 24), 672);
      const adverts = await getNodeAdverts(publicKey, hours);
      res.json(adverts);
    } catch (err) {
      console.error('[api] GET /nodes/:id/adverts', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
