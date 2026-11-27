import type { Router } from 'express';
import { BoundedTtlMap } from '../../cache/boundedTtlMap.js';
import type { DatabaseQueryFn } from '../../db/contracts.js';
import { publicJson } from '../../http/publicJson.js';
import { resolvePublicVisibilityScope } from '../../http/requestScope.js';
import { networkFilters } from '../../platform/query/networkFilters.js';
import { API_CACHE_POLICIES } from '../cachePolicies.js';
import { normalizeObserverQuery } from '../utils/observer.js';

type RegionOptions = {
  network: 'ukmesh' | 'test';
  observer: string | null;
  visibilityGeneration: number;
  regions: string[];
};

export function registerFeedRegionRoute(
  router: Router,
  query: DatabaseQueryFn,
  readGeneration: () => Promise<number>,
): () => void {
  const cache = new BoundedTtlMap<string, RegionOptions>(API_CACHE_POLICIES.feedRegions);
  let inFlight: { key: string; result: Promise<RegionOptions>; controller: AbortController } | undefined;
  let closed = false;

  router.get('/feed/regions', async (req, res) => {
    const observer = normalizeObserverQuery(req.query['observer']);
    const { network } = resolvePublicVisibilityScope(req.query['network'], req.headers, observer);
    try {
      await publicJson(res, readGeneration, async visibilityGeneration => {
        if (closed) throw new Error('FEED_REGION_LOOKUP_CLOSED');
        const key = JSON.stringify([network, observer, visibilityGeneration]);
        const cached = cache.get(key);
        if (cached) return cached;
        if (inFlight?.key === key) return inFlight.result;
        if (inFlight) throw new Error('FEED_REGION_LOOKUP_BUSY');

        const filters = networkFilters(network, observer);
        const controller = new AbortController();
        const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]);
        const result = query<{ iata: string }>(`
          WITH observers AS MATERIALIZED (
            SELECT DISTINCT p.iata, p.rx_node_id
            FROM mesh.packets p
            WHERE p.time > NOW() - INTERVAL '7 days' AND p.rx_node_id IS NOT NULL
              ${filters.packetsAlias('p')}
          )
          SELECT DISTINCT COALESCE(NULLIF(BTRIM(UPPER(iata)), ''), 'UNK') AS iata
          FROM observers
          WHERE rx_node_id ~ '^[0-9A-Fa-f]{64}$'`, filters.params, signal)
          .then(({ rows }) => {
            const regions = [...new Set(rows.map(({ iata }) => iata.trim().toUpperCase())
              .filter(iata => /^[A-Z0-9]{2,8}$/.test(iata)))].sort();
            const payload = { network, observer: observer ?? null, visibilityGeneration, regions };
            if (!closed) cache.set(key, payload);
            return payload;
          })
          .finally(() => { inFlight = undefined; });
        inFlight = { key, result, controller };
        return result;
      });
    } catch (error) {
      console.error('[api] GET /feed/regions', (error as Error).message);
      res.status(503).json({ error: 'Observer regions are unavailable', retryable: true });
    }
  });

  return () => {
    closed = true;
    inFlight?.controller.abort();
    cache.shutdown();
    cache.clear();
  };
}
