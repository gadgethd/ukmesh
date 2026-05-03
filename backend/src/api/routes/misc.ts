import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import { normalizeObserverQuery } from '../utils/observer.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type GetRecentPacketsFn = (limit: number, network?: string, observer?: string) => Promise<unknown>;
type GetRecentPacketEventsFn = (limit: number, network?: string, observer?: string) => Promise<unknown>;
type GetPacketDetailFn = (hash: string, network?: string) => Promise<unknown>;

type MiscRouteDeps = {
  query: QueryFn;
  getRecentPackets: GetRecentPacketsFn;
  getRecentPacketEvents: GetRecentPacketEventsFn;
  getPacketDetail: GetPacketDetailFn;
};

export function registerMiscRoutes(router: Router, deps: MiscRouteDeps): void {
  const {
    query,
    getRecentPackets,
    getRecentPacketEvents,
    getPacketDetail,
  } = deps;

  router.get('/packets/recent', async (req, res) => {
    try {
      const limit = Math.min(Number(req.query['limit'] ?? 200), 1000);
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const observer = normalizeObserverQuery(req.query['observer']);
      const raw = String(req.query['raw'] ?? '').trim();
      const packets = raw === '1'
        ? await getRecentPacketEvents(limit, network, observer)
        : await getRecentPackets(limit, network, observer);
      res.json(packets);
    } catch (err) {
      console.error('[api] GET /packets/recent', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/packets/:hash', async (req, res) => {
    try {
      const hash = String(req.params['hash'] ?? '').trim();
      if (!hash || !/^[0-9a-fA-F]{1,128}$/.test(hash)) {
        res.status(400).json({ error: 'Invalid packet hash' });
        return;
      }
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
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
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const params: unknown[] = [];
      const networkClause = network ? `AND network = $${params.push(network)}` : '';
      const result = await query<{
        sender: string;
        message_count: string;
        last_message_at: string;
      }>(
        `SELECT
          payload->'decrypted'->>'sender' AS sender,
          COUNT(DISTINCT packet_hash)::text AS message_count,
          MAX(time) AS last_message_at
        FROM packets
        WHERE
          packet_type = 5
          AND payload->'decrypted' IS NOT NULL
          AND payload->'decrypted'->>'sender' IS NOT NULL
          AND payload->'decrypted'->>'sender' != ''
          AND time > NOW() - INTERVAL '24 hours'
          ${networkClause}
        GROUP BY payload->'decrypted'->>'sender'
        ORDER BY COUNT(DISTINCT packet_hash) DESC
        LIMIT 100`,
        params,
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

  router.get('/planned-nodes', async (_req, res) => {
    try {
      const result = await query(
        'SELECT id, owner_pubkey, name, lat, lon, height_m, notes, created_at FROM planned_nodes ORDER BY created_at DESC',
      );
      res.json(result.rows);
    } catch (err) {
      console.error('[api] GET /planned-nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/mqtt-nodes', async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
      const params: unknown[] = [];
      const networkClause = network ? `AND nss.network = $${params.push(network)}` : `AND nss.network IS DISTINCT FROM 'test'`;
      const packetNetworkClause = network ? `AND network = $${params.length + 1}` : `AND network IS DISTINCT FROM 'test'`;
      if (network) params.push(network);
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
