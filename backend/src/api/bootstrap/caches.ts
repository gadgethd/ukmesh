import { OWNER_CACHE_POLICIES } from '../../owner/cachePolicies.js';
import { API_BOOTSTRAP_CACHE_POLICIES } from './cachePolicies.js';
import { BoundedTtlMap } from '../../cache/boundedTtlMap.js';

// Freshness/warm cadence for the dashboard stat aggregates. These queries scan
// the packets hypertable through the public privacy filter and take tens of
// seconds; a 60s cadence meant the ~80s recompute never fit inside its window
// and the warm loop pinned the database CPU. 5 minutes keeps the recompute well
// inside the interval while stale-while-revalidate (below) still serves reads
// instantly. Live data (nodes, packet feed) is realtime over WebSocket, not here.
export const STATS_CACHE_TTL_MS = 5 * 60_000;
export const STATS_CACHE_STALE_TTL_MS = API_BOOTSTRAP_CACHE_POLICIES.statsCache.ttlMs;
export const INFERRED_NODES_CACHE_TTL_MS = 5 * 60_000;
export const INFERRED_NODES_CACHE_STALE_TTL_MS = API_BOOTSTRAP_CACHE_POLICIES.inferredNodesCache.ttlMs;
export const NODE_LINKS_CACHE_TTL_MS = 60_000;
export const NODE_LINKS_CACHE_STALE_TTL_MS = API_BOOTSTRAP_CACHE_POLICIES.nodeLinksCache.ttlMs;
export const HEALTH_CACHE_TTL_MS = 60_000;
// Regenerate charts after 30 minutes, serving the previous complete snapshot
// immediately while a single background refresh runs, including after restart.
export const CHARTS_CACHE_TTL_MS = 30 * 60_000;
// Retain the complete snapshot for availability; this is not its freshness TTL.
export const CHARTS_CACHE_STALE_TTL_MS = API_BOOTSTRAP_CACHE_POLICIES.chartsCache.ttlMs;
// The owner dashboard polls /owner/live every 10s. A 5s TTL meant every poll (and
// every node switch) re-ran 9 DB queries cold. Keep it just above the poll interval
// so consecutive polls hit, and switching back to a recently viewed node is instant.
export const OWNER_LIVE_CACHE_TTL_MS = API_BOOTSTRAP_CACHE_POLICIES.ownerLiveCache.ttlMs;
// /owner/session polls every 15s and rebuilds the whole dashboard (resolve node IDs
// + 3 packets-hypertable queries). Cache the built dashboard per user for a short window.
export const OWNER_DASHBOARD_CACHE_TTL_MS = OWNER_CACHE_POLICIES.ownerDashboardCache.ttlMs;

export function createApiCaches() {
  const statsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>(API_BOOTSTRAP_CACHE_POLICIES.statsCache);
  const inferredNodesCache = new BoundedTtlMap<string, { ts: number; data: unknown }>(API_BOOTSTRAP_CACHE_POLICIES.inferredNodesCache);
  const inferredNodesInflight = new Map<string, Promise<unknown>>();
  const nodeLinksCache = new BoundedTtlMap<string, { ts: number; data: unknown }>(API_BOOTSTRAP_CACHE_POLICIES.nodeLinksCache);
  const nodeLinksInflight = new Map<string, Promise<unknown>>();
  const chartsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>(API_BOOTSTRAP_CACHE_POLICIES.chartsCache);
  const chartsInflight = new Map<string, Promise<unknown>>();
  const ownerLiveCache = new BoundedTtlMap<string, { ts: number; data: unknown }>(API_BOOTSTRAP_CACHE_POLICIES.ownerLiveCache);

  const caches = [statsCache, inferredNodesCache, nodeLinksCache, chartsCache, ownerLiveCache];
  return { statsCache, inferredNodesCache, inferredNodesInflight, nodeLinksCache, nodeLinksInflight, chartsCache, chartsInflight, ownerLiveCache,
    close() {
      for (const cache of caches) { cache.shutdown(); cache.clear(); }
      inferredNodesInflight.clear(); nodeLinksInflight.clear(); chartsInflight.clear();
    },
  };
}
