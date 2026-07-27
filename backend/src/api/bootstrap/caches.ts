import { BoundedTtlMap } from '../../cache/boundedTtlMap.js';

// Freshness/warm cadence for the dashboard stat aggregates. These queries scan
// the packets hypertable through the public privacy filter and take tens of
// seconds; a 60s cadence meant the ~80s recompute never fit inside its window
// and the warm loop pinned the database CPU. 5 minutes keeps the recompute well
// inside the interval while stale-while-revalidate (below) still serves reads
// instantly. Live data (nodes, packet feed) is realtime over WebSocket, not here.
export const STATS_CACHE_TTL_MS = 5 * 60_000;
export const STATS_CACHE_STALE_TTL_MS = 15 * 60_000;
export const INFERRED_NODES_CACHE_TTL_MS = 5 * 60_000;
export const PATH_HISTORY_CACHE_TTL_MS = 60_000;
export const CHARTS_CACHE_TTL_MS = 30 * 60_000;
// A chart snapshot is refreshed every 30 minutes, but remains usable for six
// hours if a refresh is slow or fails. Keeping the storage TTL longer than the
// freshness TTL lets callers receive the last complete, privacy-filtered
// snapshot while exactly one bounded refresh runs in the background.
export const CHARTS_CACHE_STALE_TTL_MS = 6 * 60 * 60_000;
// The owner dashboard polls /owner/live every 10s. A 5s TTL meant every poll (and
// every node switch) re-ran 9 DB queries cold. Keep it just above the poll interval
// so consecutive polls hit, and switching back to a recently viewed node is instant.
export const OWNER_LIVE_CACHE_TTL_MS = Number(process.env['OWNER_LIVE_CACHE_TTL_MS'] ?? 15_000);
// /owner/session polls every 15s and rebuilds the whole dashboard (resolve node IDs
// + 3 packets-hypertable queries). Cache the built dashboard per user for a short window.
export const OWNER_DASHBOARD_CACHE_TTL_MS = Number(process.env['OWNER_DASHBOARD_CACHE_TTL_MS'] ?? 20_000);

export const statsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  maxEntries: 256, maxWeight: 16 * 1024 * 1024, ttlMs: STATS_CACHE_STALE_TTL_MS,
});
export const inferredNodesCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  maxEntries: 128, maxWeight: 16 * 1024 * 1024, ttlMs: INFERRED_NODES_CACHE_TTL_MS,
});
export const pathHistoryCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  maxEntries: 8, maxWeight: 8 * 1024 * 1024, ttlMs: PATH_HISTORY_CACHE_TTL_MS,
});
export const chartsCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  maxEntries: 256, maxWeight: 32 * 1024 * 1024, ttlMs: CHARTS_CACHE_STALE_TTL_MS,
});
export const chartsInflight = new Map<string, Promise<unknown>>();
export const ownerLiveCache = new BoundedTtlMap<string, { ts: number; data: unknown }>({
  maxEntries: 512, maxWeight: 16 * 1024 * 1024, ttlMs: OWNER_LIVE_CACHE_TTL_MS,
});
