export const STATS_CACHE_TTL_MS = 15_000;
export const INFERRED_NODES_CACHE_TTL_MS = 60_000;
export const PATH_HISTORY_CACHE_TTL_MS = 60_000;
export const COVERAGE_CACHE_TTL_MS = 30_000;
export const CHARTS_CACHE_TTL_MS = 30 * 60_000;
export const CROSS_NETWORK_CACHE_TTL_MS = 60_000;
export const OWNER_LIVE_CACHE_TTL_MS = 5_000;

export const statsCache = new Map<string, { ts: number; data: unknown }>();
export const inferredNodesCache = new Map<string, { ts: number; data: unknown }>();
export const pathHistoryCache = new Map<string, { ts: number; data: unknown }>();
export const coverageCache = new Map<string, { ts: number; data: unknown }>();
export const chartsCache = new Map<string, { ts: number; data: unknown }>();
export const crossNetworkCache = new Map<string, { ts: number; data: unknown }>();
export const chartsInflight = new Map<string, Promise<unknown>>();
export const ownerLiveCache = new Map<string, { ts: number; data: unknown }>();
