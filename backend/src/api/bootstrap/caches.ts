export const STATS_CACHE_TTL_MS = 60_000;
export const INFERRED_NODES_CACHE_TTL_MS = 60_000;
export const PATH_HISTORY_CACHE_TTL_MS = 60_000;
export const CHARTS_CACHE_TTL_MS = 30 * 60_000;
// The owner dashboard polls /owner/live every 10s. A 5s TTL meant every poll (and
// every node switch) re-ran 9 DB queries cold. Keep it just above the poll interval
// so consecutive polls hit, and switching back to a recently viewed node is instant.
export const OWNER_LIVE_CACHE_TTL_MS = Number(process.env['OWNER_LIVE_CACHE_TTL_MS'] ?? 15_000);
// /owner/session polls every 15s and rebuilds the whole dashboard (resolve node IDs
// + 3 packets-hypertable queries). Cache the built dashboard per user for a short window.
export const OWNER_DASHBOARD_CACHE_TTL_MS = Number(process.env['OWNER_DASHBOARD_CACHE_TTL_MS'] ?? 20_000);

export const statsCache = new Map<string, { ts: number; data: unknown }>();
export const inferredNodesCache = new Map<string, { ts: number; data: unknown }>();
export const pathHistoryCache = new Map<string, { ts: number; data: unknown }>();
export const chartsCache = new Map<string, { ts: number; data: unknown }>();
export const chartsInflight = new Map<string, Promise<unknown>>();
export const ownerLiveCache = new Map<string, { ts: number; data: unknown }>();
