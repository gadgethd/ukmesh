import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { normalizeObserverQuery } from '../utils/observer.js';
import { expandResolverScope } from '../../networks.js';
import { networkFilters } from '../utils/networkFilters.js';
import { getPublicRuntimeFeatureConfig } from '../../features.js';
import {
  parseBoolean,
  parseBoundedInteger,
  parseEnum,
  parseHexIdentifier,
} from '../utils/input.js';
import {
  decodePlannedNodeCursor,
  listPublicPlannedNodes,
} from '../../repositories/plannedNodes.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type GetRecentPacketsFn = (
  limit: number,
  network?: string,
  observer?: string,
  fields?: 'full' | 'slim',
) => Promise<unknown>;
type GetRecentPacketEventsFn = (limit: number, network?: string, observer?: string) => Promise<unknown>;
type GetPacketDetailFn = (hash: string, network?: string) => Promise<unknown>;

type MiscRouteDeps = {
  query: QueryFn;
  getRecentPackets: GetRecentPacketsFn;
  getRecentPacketEvents: GetRecentPacketEventsFn;
  getPacketDetail: GetPacketDetailFn;
  getPublicVisibilityGeneration: () => Promise<number>;
  packetDetailLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export function registerMiscRoutes(router: Router, deps: MiscRouteDeps): void {
  const {
    query,
    getRecentPackets,
    getRecentPacketEvents,
    getPacketDetail,
    getPublicVisibilityGeneration,
    packetDetailLimiter,
  } = deps;

  router.get('/runtime-config', async (_req, res) => {
    try {
      const privacyGeneration = await getPublicVisibilityGeneration();
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Vary', 'Accept-Encoding');
      res.json(getPublicRuntimeFeatureConfig(process.env, privacyGeneration));
    } catch (error) {
      console.error('[api] GET /runtime-config', (error as Error).message);
      res.status(503).json({ error: 'Runtime configuration unavailable' });
    }
  });

  router.get('/packets/recent', async (req, res) => {
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 200,
      min: 1,
      max: 1000,
    });
    const raw = parseBoolean(req.query['raw'], { name: 'raw' });
    const fields = parseEnum(req.query['fields'], {
      name: 'fields',
      values: ['slim', 'full'] as const,
      defaultValue: 'full',
    })!;
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const observer = normalizeObserverQuery(req.query['observer']);
      const packets = raw
        ? await getRecentPacketEvents(limit, network, observer)
        : await getRecentPackets(limit, network, observer, fields);
      res.json(packets);
    } catch (err) {
      console.error('[api] GET /packets/recent', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/packets/:hash', packetDetailLimiter, async (req, res) => {
    const hash = parseHexIdentifier(req.params['hash'], {
      name: 'hash',
      maxLength: 128,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const detail = await getPacketDetail(hash, network);
      if (!detail) {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      res.json(detail);
    } catch (err) {
      console.error('[api] GET /packets/:hash', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/companion-activity', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = networkFilters(network);
      const result = await query<{
        sender: string;
        message_count: string;
        last_message_at: string;
      }>(
        `SELECT
          p.companion_sender AS sender,
          COUNT(DISTINCT p.packet_hash)::text AS message_count,
          MAX(p.time) AS last_message_at
        FROM packets p
        WHERE
          p.packet_type = 5
          AND p.companion_sender IS NOT NULL
          AND p.companion_sender != ''
          AND p.companion_sender NOT LIKE '%🚫%'
          AND p.time > NOW() - INTERVAL '24 hours'
          ${filters.packetsAlias('p')}
        GROUP BY p.companion_sender
        ORDER BY COUNT(DISTINCT p.packet_hash) DESC
        LIMIT 100`,
        filters.params,
      );
      res.json(result.rows.map(r => ({
        sender: r.sender,
        message_count: parseInt(r.message_count, 10),
        last_message_at: r.last_message_at,
      })));
    } catch (err) {
      console.error('[api] GET /companion-activity', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/planned-nodes', async (req, res) => {
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 50,
      min: 1,
      max: 100,
    });
    try {
      const page = await listPublicPlannedNodes(query, {
        limit,
        cursor: decodePlannedNodeCursor(req.query['cursor']),
      });
      res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
      res.json(page);
    } catch (err) {
      if ((err as Error).message === 'INVALID_PLANNED_NODE_CURSOR') {
        res.status(400).json({ error: 'Invalid planned-node cursor' });
        return;
      }
      console.error('[api] GET /planned-nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/mqtt-nodes', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const networkValues = expandResolverScope(network);
      const params: unknown[] = [networkValues, networkValues];
      const networkClause = 'AND nss.network = ANY($1::text[])';
      const packetNetworkClause = 'AND network = ANY($2::text[])';
      const result = await query<{
        node_id: string;
        name: string | null;
        last_seen: string;
        battery_mv: number | null;
        uptime_secs: number | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        rx_air_secs: number | null;
        tx_air_secs: number | null;
        stats: Record<string, unknown> | null;
        packets_24h: string;
      }>(

        `SELECT DISTINCT ON (nss.node_id)
           nss.node_id,
           n.name,
           nss.time AS last_seen,
           nss.battery_mv,
           nss.uptime_secs,
           nss.channel_utilization,
           nss.air_util_tx,
           nss.rx_air_secs,
           nss.tx_air_secs,
           nss.stats,
           COALESCE(pc.packet_count, 0) AS packets_24h
         FROM node_status_samples nss
         LEFT JOIN nodes n ON n.node_id = nss.node_id
         LEFT JOIN (
           SELECT rx_node_id, COUNT(*) AS packet_count
           FROM packets
           WHERE time > NOW() - INTERVAL '24 hours'
             AND rx_node_id IS NOT NULL
             ${packetNetworkClause}
           GROUP BY rx_node_id
         ) pc ON pc.rx_node_id = nss.node_id
         WHERE nss.time > NOW() - INTERVAL '15 minutes'
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           ${networkClause}
         ORDER BY nss.node_id, COALESCE(nss.uptime_secs, 0) DESC, nss.time DESC`,
        params,
      );
      res.json(result.rows.filter((r) => Number(r.packets_24h) > 0));
    } catch (err) {
      console.error('[api] GET /mqtt-nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
