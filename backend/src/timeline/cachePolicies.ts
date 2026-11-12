import { defineCachePolicy } from '../cache/policy.js';

export const TIMELINE_CACHE_POLICIES = Object.freeze({
  completedSnapshot: defineCachePolicy({
    id: 'api_activity_timeline',
    source: 'src/timeline/snapshotCache.ts',
    disposition: 'bounded-cache',
    name: 'api_activity_timeline',
    maxEntries: 16,
    maxWeight: 8_388_608,
    ttlMs: 90_000,
    freshMs: 30_000,
    maxRefreshes: 4,
    scope: 'network + window minutes + bucket minutes + ready privacy generation',
    invalidation: 'fresh authority before/after delivery + generation-keyed 30s refresh/90s maximum age',
    negativeCaching: 'empty complete snapshot',
    singleFlight: 'at most four coalesced/background fills; other cold requests retain awaited SQL',
  }),
});
