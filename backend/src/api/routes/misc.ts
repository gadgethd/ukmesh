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

type MiscRouteDeps = {
  query: QueryFn;
  getRecentPackets: GetRecentPacketsFn;
  getRecentPacketEvents: GetRecentPacketEventsFn;
};

export function registerMiscRoutes(router: Router, deps: MiscRouteDeps): void {
  const {
    query,
    getRecentPackets,
    getRecentPacketEvents,
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
}
