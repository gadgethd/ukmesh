import type { Request, Response, Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import { isPrivateNode, redactPrivateNode } from '../utils/privateNode.js';

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

type GetNodesFn = (
  network?: string,
  observer?: string,
  fields?: 'full' | 'slim',
) => Promise<NodeRecord[]>;
type GetNodeHistoryFn = (nodeId: string, hours: number, network: string) => Promise<unknown>;
type GetNodeAdvertsFn = (publicKey: string, hours: number, limit: number, network: string) => Promise<unknown>;
type RequireLocalOnlyFn = (req: Request, res: Response) => boolean;

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
  inferredNodesInflight: Map<string, Promise<unknown>>;
  inferredNodesCacheTtlMs: number;
  nodeLinksCache: Map<string, { ts: number; data: unknown }>;
  nodeLinksInflight: Map<string, Promise<unknown>>;
  nodeLinksCacheTtlMs: number;
  nodesLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
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
    inferredNodesInflight,
    inferredNodesCacheTtlMs,
    nodeLinksCache,
    nodeLinksInflight,
    nodeLinksCacheTtlMs,
    nodesLimiter,
  } = deps;

  router.get('/nodes/map', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const scope = networkFilters(network, normalizeObserverQuery(req.query['observer']));
      const allowed = ['node_id', 'name', 'lat', 'lon', 'role', 'iata', 'last_seen', 'is_online', 'hardware_model', 'firmware_version'] as const;
      const requested = String(req.query['fields'] ?? '')
        .split(',')
        .map((field) => field.trim())
        .filter((field): field is typeof allowed[number] => allowed.includes(field as typeof allowed[number]));
      const fields = requested.length > 0 ? [...new Set(['node_id', ...requested])] : [...allowed];
      const result = await query<Record<string, unknown>>(
        `SELECT ${fields.map((field) => `n.${field}`).join(', ')}
         FROM nodes n
         WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
           AND (n.role IS NULL OR n.role IN (1, 2, 3, 4))
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           ${scope.nodesAlias('n')}
         ORDER BY n.last_seen DESC NULLS LAST`,
        scope.params,
      );
      res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
      res.json(result.rows);
    } catch (err) {
      console.error('[api] GET /nodes/map', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

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
           ORDER BY time DESC
           LIMIT 2000`,
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

  router.get('/nodes', nodesLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const fields = req.query['fields'] === 'slim' ? 'slim' : 'full';
      const nodes = await getNodes(network, observer, fields);
      res.json(nodes.filter((node) => !isPrivateNode(node.name)).map(redactPrivateNode));
    } catch (err) {
      console.error('[api] GET /nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes/map', nodesLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const filters = networkFilters(network, observer);
      const result = await query(
        `SELECT
           n.node_id, n.name, n.lat, n.lon, n.role, n.iata,
           n.last_seen::text, n.is_online, n.hardware_model,
           n.firmware_version, n.advert_count
         FROM nodes n
         WHERE n.role = 2
           AND n.lat IS NOT NULL
           AND n.lon IS NOT NULL
           AND n.lat BETWEEN -90 AND 90
           AND n.lon BETWEEN -180 AND 180
           AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           ${filters.nodesAlias('n')}
         ORDER BY n.node_id`,
        filters.params,
      );
      res.setHeader('Cache-Control', 'public, max-age=30, stale-while-revalidate=60');
      res.json(result.rows);
    } catch (err) {
      console.error('[api] GET /nodes/map', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/inferred-nodes', nodesLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const scope = networkFilters(network, observer);

      const inferredCacheKey = `${network ?? 'all'}:${observer ?? ''}`;
      const inferredCached = inferredNodesCache.get(inferredCacheKey);
      if (inferredCached && Date.now() - inferredCached.ts < inferredNodesCacheTtlMs) {
        res.json(inferredCached.data);
        return;
      }
      const existingWork = inferredNodesInflight.get(inferredCacheKey);
      if (existingWork) {
        if (inferredCached) {
          res.json(inferredCached.data);
          return;
        }
        await existingWork;
        const refreshed = inferredNodesCache.get(inferredCacheKey);
        if (!refreshed) throw new Error('INFERRED_NODES_REFRESH_FAILED');
        res.json(refreshed.data);
        return;
      }

      let finishWork: () => void = () => {};
      const work = new Promise<void>((resolve) => {
        finishWork = resolve;
      });
      inferredNodesInflight.set(inferredCacheKey, work);
      const servingStale = Boolean(inferredCached);
      if (inferredCached) res.json(inferredCached.data);

      try {
        const [visibleNodes, allNodeIds, packetsResult] = await Promise.all([
          getNodes(network, observer, 'slim').then((nodes) =>
          nodes.filter((node) => !isPrivateNode(node.name)).map(redactPrivateNode)),
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
      const exactNodePrefixes = new Map<string, (typeof exactNodes)[number] | null>();
      for (const node of exactNodes) {
        if (typeof node.lat !== 'number' || typeof node.lon !== 'number') continue;
        const nodeId = node.node_id.toUpperCase();
        for (const prefixLength of [4, 6]) {
          const prefix = nodeId.slice(0, prefixLength);
          if (prefix.length !== prefixLength) continue;
          exactNodePrefixes.set(prefix, exactNodePrefixes.has(prefix) ? null : node);
        }
      }

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
        return exactNodePrefixes.get(normalized) ?? null;
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

      const knownNodePrefixes = new Set<string>();
      for (const node of allNodeIds.rows) {
        const nodeId = node.node_id.toUpperCase();
        if (nodeId.length >= 4) knownNodePrefixes.add(nodeId.slice(0, 4));
        if (nodeId.length >= 6) knownNodePrefixes.add(nodeId.slice(0, 6));
      }
      const inferredNodes: InferredMultibyteNode[] = Array.from(inferredUnknowns.values())
        .filter((entry) => entry.packetHashes.size >= 2
          && !knownNodePrefixes.has(entry.prefix.toUpperCase()))
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
        if (!servingStale) res.json(payload);
      } finally {
        inferredNodesInflight.delete(inferredCacheKey);
        finishWork();
      }
    } catch (err) {
      console.error('[api] GET /inferred-nodes', (err as Error).message);
      if (!res.headersSent) res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes/:id/links', async (req, res) => {
    try {
      const id = req.params['id']!;
      if (!/^[0-9a-fA-F]{64}$/.test(id)) {
        res.status(400).json({ error: 'Invalid node ID format' });
        return;
      }
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = networkFilters(network);
      const cacheKey = `${network}:${id.toUpperCase()}`;
      const cached = nodeLinksCache.get(cacheKey);
      if (cached && Date.now() - cached.ts < nodeLinksCacheTtlMs) {
        res.json(cached.data);
        return;
      }
      const idParam = `$${filters.params.length + 1}`;
      const loadLinks = async () => {
        const result = await query<{
          peer_id: string;
          peer_name: string | null;
          observed_count: number;
          itm_path_loss_db: number | null;
          count_this_to_peer: number;
          count_peer_to_this: number;
        }>(
          `WITH source_node AS MATERIALIZED (
             SELECT node_id
             FROM nodes
             WHERE node_id = ${idParam}
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               ${filters.nodes}
           ),
           relevant_links AS MATERIALIZED (
             SELECT
               CASE WHEN nl.node_a_id = ${idParam} THEN nl.node_b_id ELSE nl.node_a_id END AS peer_id,
               nl.observed_count,
               nl.itm_path_loss_db,
               CASE WHEN nl.node_a_id = ${idParam} THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS count_this_to_peer,
               CASE WHEN nl.node_a_id = ${idParam} THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS count_peer_to_this
             FROM node_links nl
             WHERE (nl.node_a_id = ${idParam} OR nl.node_b_id = ${idParam})
               AND (nl.itm_viable = TRUE OR nl.force_viable = TRUE)
               AND EXISTS (SELECT 1 FROM source_node)
           )
           SELECT
             rl.peer_id, peer.name AS peer_name, rl.observed_count,
             rl.itm_path_loss_db, rl.count_this_to_peer, rl.count_peer_to_this
           FROM relevant_links rl
           JOIN nodes peer ON peer.node_id = rl.peer_id
           WHERE (peer.name IS NULL OR peer.name NOT LIKE '%🚫%')
             ${filters.nodesAlias('peer')}
           ORDER BY rl.observed_count DESC`,
          [...filters.params, id],
        );
        return result.rows;
      };
      let inflight = nodeLinksInflight.get(cacheKey);
      if (!inflight) {
        const tracked = loadLinks()
          .then((rows) => {
            nodeLinksCache.set(cacheKey, { ts: Date.now(), data: rows });
            return rows;
          })
          .finally(() => {
            if (nodeLinksInflight.get(cacheKey) === tracked) nodeLinksInflight.delete(cacheKey);
          });
        inflight = tracked;
        nodeLinksInflight.set(cacheKey, tracked);
      }
      if (cached) {
        res.setHeader('Warning', '110 - "Response is stale"');
        res.json(cached.data);
        return;
      }
      res.json(await inflight);
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
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const history = await getNodeHistory(id, hours, network);
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
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const adverts = await getNodeAdverts(publicKey, hours, 100, network);
      res.json(adverts);
    } catch (err) {
      console.error('[api] GET /nodes/:id/adverts', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
