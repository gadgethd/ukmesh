export type CachePolicyRecord = Readonly<{
  source: string;
  disposition: 'bounded-cache' | 'request-local-exclusion';
  maxEntries?: number;
  maxBytes?: number;
  ttlMs?: number;
  scope: string;
  invalidation: string;
  negativeCaching: string;
  singleFlight: string;
}>;

/**
 * Reviewed registry for process-lifetime data caches.
 *
 * Coordination-only in-flight maps are excluded because their admission caps
 * and resolve/reject cleanup live beside the work they coordinate. Algorithmic
 * maps are request/run-local. The static registry test discovers every
 * BoundedTtlMap and every Map identifier containing "cache", so a new cache
 * cannot land without an explicit policy or exclusion.
 */
export const CACHE_POLICY_REGISTRY: readonly CachePolicyRecord[] = Object.freeze([
  {
    source: 'src/api/bootstrap/caches.ts#statsCache',
    disposition: 'bounded-cache', maxEntries: 256, maxBytes: 16 << 20, ttlMs: 15 * 60_000,
    scope: 'network + observer + visibility generation', invalidation: 'generation check + TTL/stale-while-revalidate',
    negativeCaching: 'completed responses only', singleFlight: 'statsService statsInflight',
  },
  {
    source: 'src/api/bootstrap/caches.ts#inferredNodesCache',
    disposition: 'bounded-cache', maxEntries: 128, maxBytes: 16 << 20, ttlMs: 30 * 60_000,
    scope: 'network + observer + visibility generation', invalidation: 'TTL',
    negativeCaching: 'none', singleFlight: 'inferredNodesInflight',
  },
  {
    source: 'src/api/bootstrap/caches.ts#nodeLinksCache',
    disposition: 'bounded-cache', maxEntries: 4_096, maxBytes: 32 << 20, ttlMs: 5 * 60_000,
    scope: 'network + observer + node', invalidation: 'TTL',
    negativeCaching: 'empty link response allowed', singleFlight: 'nodeLinksInflight',
  },
  {
    source: 'src/api/bootstrap/caches.ts#pathHistoryCache',
    disposition: 'bounded-cache', maxEntries: 8, maxBytes: 8 << 20, ttlMs: 60_000,
    scope: 'public scope + visibility generation', invalidation: 'TTL + publication fence',
    negativeCaching: 'none', singleFlight: 'durable analysis lease',
  },
  {
    source: 'src/api/bootstrap/caches.ts#chartsCache',
    disposition: 'bounded-cache', maxEntries: 256, maxBytes: 32 << 20, ttlMs: 6 * 60 * 60_000,
    scope: 'network + visibility generation; observer responses are not cached', invalidation: 'generation check + stale-while-revalidate TTL',
    negativeCaching: 'last successful snapshot only', singleFlight: 'chartsInflight',
  },
  {
    source: 'src/api/bootstrap/caches.ts#ownerLiveCache',
    disposition: 'bounded-cache', maxEntries: 512, maxBytes: 16 << 20, ttlMs: 15_000,
    scope: 'authorized owner node', invalidation: 'TTL + logout',
    negativeCaching: 'none', singleFlight: 'request-bound repository work',
  },
  {
    source: 'src/stats/statsService.ts#channelTrafficCache',
    disposition: 'bounded-cache', maxEntries: 64, maxBytes: 2 << 20, ttlMs: 60 * 60_000,
    scope: 'network + visibility generation; observer responses are not cached', invalidation: 'generation check + TTL',
    negativeCaching: 'empty completed response', singleFlight: 'channelTrafficInflight',
  },
  {
    source: 'src/stats/statsService.ts#observerActivityCache',
    disposition: 'bounded-cache', maxEntries: 64, maxBytes: 16 << 20, ttlMs: 5 * 60_000,
    scope: 'network', invalidation: 'TTL/stale-while-revalidate',
    negativeCaching: 'empty completed response', singleFlight: 'observerActivityInflight',
  },
  {
    source: 'src/path-beta/resolveCache.ts#cache',
    disposition: 'bounded-cache', maxEntries: 4_096, maxBytes: 64 << 20, ttlMs: 10 * 60_000,
    scope: 'packet + network + observer + visibility generation', invalidation: 'packet generation token',
    negativeCaching: 'none', singleFlight: 'pathingService resolve maps',
  },
  {
    source: 'src/path-beta/resolveCache.ts#invalidationVersions',
    disposition: 'bounded-cache', maxEntries: 50_000, maxBytes: 4 << 20, ttlMs: 60 * 60_000,
    scope: 'packet', invalidation: 'TTL after cache result expiry',
    negativeCaching: 'zero generation is implicit', singleFlight: 'not applicable',
  },
  {
    source: 'src/path-beta/resolveCache.ts#stickyNodeCache',
    disposition: 'bounded-cache', maxEntries: 2_048, maxBytes: 16 << 20, ttlMs: 30 * 60_000,
    scope: 'packet + network; public projection remains visibility-fenced', invalidation: 'TTL',
    negativeCaching: 'none', singleFlight: 'worker pool',
  },
  {
    source: 'src/path-beta/resolver.ts#contextCache',
    disposition: 'bounded-cache', maxEntries: 16, maxBytes: 128 << 20, ttlMs: 15 * 60_000,
    scope: 'network + embedded visibility generation', invalidation: 'generation check + TTL',
    negativeCaching: 'none', singleFlight: 'bounded worker pool',
  },
  {
    source: 'src/owner/ownerAccess.ts#ownerNodeIdCache',
    disposition: 'bounded-cache', maxEntries: 2_048, maxBytes: 4 << 20, ttlMs: 30_000,
    scope: 'owner username', invalidation: 'authorization reconciliation + TTL',
    negativeCaching: 'empty verified grant list', singleFlight: 'ownerNodeIdInflight (cap 128)',
  },
  {
    source: 'src/owner/ownerAccess.ts#authCache',
    disposition: 'bounded-cache', maxEntries: 512, maxBytes: 2 << 20, ttlMs: 5 * 60_000,
    scope: 'owner username + credential digest', invalidation: 'credential mismatch + TTL',
    negativeCaching: 'positive results only', singleFlight: 'broker connection timeout',
  },
  {
    source: 'src/owner/ownerService.ts#ownerLastHopCache',
    disposition: 'bounded-cache', maxEntries: 1_024, maxBytes: 64 << 20, ttlMs: 60 * 60_000,
    scope: 'authorized node', invalidation: 'logout + TTL',
    negativeCaching: 'completed empty series', singleFlight: 'owner route limiter',
  },
  {
    source: 'src/owner/ownerService.ts#ownerDashboardCache',
    disposition: 'bounded-cache', maxEntries: 512, maxBytes: 16 << 20, ttlMs: 20_000,
    scope: 'owner username', invalidation: 'logout + TTL',
    negativeCaching: 'none', singleFlight: 'owner route limiter',
  },
  {
    source: 'src/ws/server.ts#viableLinksCache',
    disposition: 'bounded-cache', maxEntries: 50, maxBytes: 16 << 20, ttlMs: 5 * 60_000,
    scope: 'network + observer', invalidation: 'TTL',
    negativeCaching: 'completed empty link set', singleFlight: 'initialStateInflight',
  },
  {
    source: 'src/ws/server.ts#initialStateCache',
    disposition: 'bounded-cache', maxEntries: 128, maxBytes: 48 << 20, ttlMs: 60_000,
    scope: 'network + observer + privacy projection', invalidation: 'TTL + live updates',
    negativeCaching: 'completed empty state', singleFlight: 'initialStateInflight (cap 128)',
  },
  {
    source: 'src/mqtt/client.ts#channelCache',
    disposition: 'bounded-cache', maxEntries: 200, maxBytes: 2 << 20, ttlMs: 10 * 60_000,
    scope: 'raw packet within immutable configured channel set', invalidation: 'TTL/LRU capacity',
    negativeCaching: 'failed channel decode is cached', singleFlight: 'single MQTT ingest task',
  },
  {
    source: 'src/path-beta/resolver.ts#hopCache',
    disposition: 'request-local-exclusion',
    scope: 'one resolver invocation', invalidation: 'discarded with invocation',
    negativeCaching: 'local null memoization', singleFlight: 'not applicable',
  },
]);
