import { BoundedTtlMap } from '../../cache/boundedTtlMap.js';
import { getWorkerHealthOverview } from '../../health/status.js';
import { HealthSnapshotCache } from '../../health/snapshot.js';

// Freshness/warm cadence for the dashboard stat aggregates. These queries scan
// the packets hypertable through the public privacy filter and take tens of
// seconds; a 60s cadence meant the ~80s recompute never fit inside its window
// and the warm loop pinned the database CPU. 5 minutes keeps the recompute well
// inside the interval while stale-while-revalidate (below) still serves reads
// instantly. Live data (nodes, packet feed) is realtime over WebSocket, not here.
export const STATS_CACHE_TTL_MS = 5 * 60_000;
export const STATS_CACHE_STALE_TTL_MS = 15 * 60_000;
export const INFERRED_NODES_CACHE_TTL_MS = 5 * 60_000;
export const INFERRED_NODES_CACHE_STALE_TTL_MS = 30 * 60_000;
export const NODE_LINKS_CACHE_TTL_MS = 60_000;
export const NODE_LINKS_CACHE_STALE_TTL_MS = 5 * 60_000;
export const HEALTH_CACHE_TTL_MS = 60_000;
// Check durable chart freshness every 30 minutes. Regeneration is gated by the
// persisted snapshot max age below, so a process restart cannot turn an empty
// in-memory cache into an unnecessary analytical rebuild.
export const CHARTS_CACHE_TTL_MS = 30 * 60_000;
// A complete persisted snapshot remains current for six hours. Once expired it
// is still the availability fallback while exactly one refresh runs.
export const CHARTS_CACHE_STALE_TTL_MS = 6 * 60 * 60_000;
// The owner dashboard polls /owner/live every 10s. A 5s TTL meant every poll (and
// every node switch) re-ran 9 DB queries cold. Keep it just above the poll interval
// so consecutive polls hit, and switching back to a recently viewed node is instant.
export const OWNER_LIVE_CACHE_TTL_MS = Number(process.env['OWNER_LIVE_CACHE_TTL_MS'] ?? 15_000);
// /owner/session polls every 15s and rebuilds the whole dashboard (resolve node IDs
// + 3 packets-hypertable queries). Cache the built dashboard per user for a short window.
export const OWNER_DASHBOARD_CACHE_TTL_MS = Number(process.env['OWNER_DASHBOARD_CACHE_TTL_MS'] ?? 20_000);

export const statsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  name: 'api_stats',
  maxEntries: 256, maxWeight: 16 * 1024 * 1024, ttlMs: STATS_CACHE_STALE_TTL_MS,
});
export const inferredNodesCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  name: 'api_inferred_nodes',
  maxEntries: 128, maxWeight: 16 * 1024 * 1024, ttlMs: INFERRED_NODES_CACHE_STALE_TTL_MS,
});
export const inferredNodesInflight = new Map<string, Promise<unknown>>();
export const nodeLinksCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  name: 'api_node_links',
  maxEntries: 4096, maxWeight: 32 * 1024 * 1024, ttlMs: NODE_LINKS_CACHE_STALE_TTL_MS,
});
export const nodeLinksInflight = new Map<string, Promise<unknown>>();
export const chartsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  name: 'api_charts',
  maxEntries: 256, maxWeight: 32 * 1024 * 1024, ttlMs: CHARTS_CACHE_STALE_TTL_MS,
});
export const chartsInflight = new Map<string, Promise<unknown>>();
export const ownerLiveCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  name: 'api_owner_live',
  maxEntries: 512, maxWeight: 16 * 1024 * 1024, ttlMs: OWNER_LIVE_CACHE_TTL_MS,
});

export const healthSnapshot = new HealthSnapshotCache(
  getWorkerHealthOverview,
  Math.max(HEALTH_CACHE_TTL_MS * 5, 5 * 60_000),
);
