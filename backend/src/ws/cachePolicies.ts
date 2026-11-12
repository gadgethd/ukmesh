import { defineCachePolicy } from '../cache/policy.js';

export const WS_CACHE_POLICIES = Object.freeze({
  viableLinksCache: defineCachePolicy({
    id: 'ws_viable_links',
    source: 'src/ws/server.ts',
    disposition: 'bounded-cache',
    name: 'ws_viable_links',
    maxEntries: 50,
    maxWeight: 16_777_216,
    ttlMs: 300_000,
    scope: 'network + observer',
    invalidation: 'TTL',
    negativeCaching: 'completed empty link set',
    singleFlight: 'initialStateInflight',
  }),
  initialStateCache: defineCachePolicy({
    id: 'ws_initial_state',
    source: 'src/ws/server.ts',
    disposition: 'bounded-cache',
    name: 'ws_initial_state',
    maxEntries: 128,
    maxWeight: 50_331_648,
    ttlMs: 60_000,
    scope: 'network + observer + region + ready privacy generation in the completed snapshot',
    invalidation: 'fresh authority before loading and delivering + TTL + live updates',
    negativeCaching: 'completed empty state',
    singleFlight: 'initialStateInflight (cap 128)',
  }),
});
