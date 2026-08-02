import type { Request, Response, Router } from 'express';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import { isPrivateNode, redactPrivateNode } from '../utils/privateNode.js';
import type { NodeRepository } from '../../repositories/nodes.js';
import {
  PublicMapInputError,
  encodePublicMapCursor,
  fitPublicMapRowsToByteBudget,
  parsePublicMapCursor,
  parsePublicMapFields,
  parsePublicMapLimit,
  parsePublicMapSnapshot,
} from '../../nodes/publicMap.js';
import {
  parseBoundedInteger,
  parseEnum,
  parseHexIdentifier,
} from '../utils/input.js';

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

const MAX_INFERRED_PACKET_ROWS = 100_000;
const MAX_INFERRED_NODES = 2_000;
const MAX_INFERRED_ACTIVE_NODE_IDS = 5_000;

type NodesRouteDeps = {
  getNodes: GetNodesFn;
  getNodeHistory: GetNodeHistoryFn;
  getNodeAdverts: GetNodeAdvertsFn;
  nodeRepository: NodeRepository;
  requireLocalOnly: RequireLocalOnlyFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  getPublicVisibilityGeneration: () => Promise<number>;
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
    nodeRepository,
    requireLocalOnly,
    networkFilters,
    getPublicVisibilityGeneration,
    inferredNodesCache,
    inferredNodesInflight,
    inferredNodesCacheTtlMs,
    nodeLinksCache,
    nodeLinksInflight,
    nodeLinksCacheTtlMs,
    nodesLimiter,
  } = deps;

  router.get('/local/test-diagnostics', async (req, res) => {
    try {
      if (!requireLocalOnly(req, res)) return;
      const [nodes, diagnostics] = await Promise.all([
        getNodes('test'),
        nodeRepository.loadTestDiagnostics(),
      ]);

      res.json({
        network: 'test',
        nodes,
        ...diagnostics,
      });
    } catch (err) {
      console.error('[api] GET /local/test-diagnostics', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes', nodesLimiter, async (req, res) => {
    const fields = parseEnum(req.query['fields'], {
      name: 'fields',
      values: ['slim', 'full'] as const,
      defaultValue: 'full',
    })!;
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
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
      const fields = parsePublicMapFields(req.query['fields']);
      const limit = parsePublicMapLimit(req.query['limit']);
      const snapshot = parsePublicMapSnapshot(req.query['snapshot']);
      const cursor = parsePublicMapCursor(req.query['cursor']);
      const result = await nodeRepository.listPublicMapRows(
        fields,
        filters,
        snapshot,
        cursor,
        limit,
      );
      const hasExtraRow = result.length > limit;
      const fitted = fitPublicMapRowsToByteBudget(result, limit);
      const rows = fitted.rows;
      const complete = !hasExtraRow && !fitted.truncatedByBytes;
      const nextCursor = complete || rows.length < 1
        ? null
        : encodePublicMapCursor(String(rows[rows.length - 1]!['node_id']));
      res.setHeader('Cache-Control', 'public, max-age=30, stale-while-revalidate=60');
      res.setHeader('X-Map-Snapshot', snapshot);
      res.setHeader('X-Map-Complete', complete ? 'true' : 'false');
      res.json({
        nodes: rows,
        page: {
          snapshot,
          nextCursor,
          complete,
          returned: rows.length,
          rowLimit: limit,
        },
      });
    } catch (err) {
      if (err instanceof PublicMapInputError) {
        res.status(400).json({ error: err.message });
        return;
      }
      console.error('[api] GET /nodes/map', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/inferred-nodes', nodesLimiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const scope = networkFilters(network, observer);
      const visibilityGeneration = await getPublicVisibilityGeneration();

      const inferredCacheKey = `${network ?? 'all'}:${observer ?? ''}:visibility-${visibilityGeneration}`;
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
          nodeRepository.listAllNodeIds(),
          nodeRepository.listInferredPackets(scope, MAX_INFERRED_PACKET_ROWS),
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

      for (const row of packetsResult) {
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
      for (const node of allNodeIds) {
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
        ))
        .slice(0, MAX_INFERRED_NODES);
      const inferredActiveNodeIds = Array.from(inferredKnowns.values())
        .filter((entry) => entry.packetHashes.size >= 2)
        .sort((a, b) => (
          b.packetHashes.size - a.packetHashes.size
          || b.observations - a.observations
          || b.latestSeen.localeCompare(a.latestSeen)
        ))
        .map((entry) => entry.nodeId)
        .slice(0, MAX_INFERRED_ACTIVE_NODE_IDS);

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
    const id = parseHexIdentifier(req.params['id'], {
      name: 'node ID',
      minLength: 64,
      maxLength: 64,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = networkFilters(network);
      const cacheKey = `${network}:${id.toUpperCase()}`;
      const cached = nodeLinksCache.get(cacheKey);
      if (cached && Date.now() - cached.ts < nodeLinksCacheTtlMs) {
        res.json(cached.data);
        return;
      }
      const loadLinks = () => nodeRepository.listNodeLinks(id, filters);
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
    const id = parseHexIdentifier(req.params['id'], {
      name: 'node ID',
      minLength: 64,
      maxLength: 64,
    });
    const hours = parseBoundedInteger(req.query['hours'], {
      name: 'hours',
      defaultValue: 24,
      min: 1,
      max: 672,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const history = await getNodeHistory(id, hours, network);
      res.json(history);
    } catch (err) {
      console.error('[api] GET /nodes/:id/history', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/nodes/:id/adverts', async (req, res) => {
    const publicKey = parseHexIdentifier(req.params['id'], {
      name: 'public key',
      minLength: 64,
      maxLength: 64,
    });
    const hours = parseBoundedInteger(req.query['hours'], {
      name: 'hours',
      defaultValue: 24,
      min: 1,
      max: 672,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const adverts = await getNodeAdverts(publicKey, hours, 100, network);
      res.json(adverts);
    } catch (err) {
      console.error('[api] GET /nodes/:id/adverts', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
