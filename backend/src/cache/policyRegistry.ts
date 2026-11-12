import type { CachePolicyRecord } from './policy.js';
import { MQTT_CACHE_POLICIES } from '../mqtt/cachePolicies.js';
import { API_CACHE_POLICIES } from '../api/cachePolicies.js';
import { API_BOOTSTRAP_CACHE_POLICIES } from '../api/bootstrap/cachePolicies.js';
import { STATS_CACHE_POLICIES } from '../stats/cachePolicies.js';
import { PATH_CACHE_POLICIES } from '../path-beta/cachePolicies.js';
import { TIMELINE_CACHE_POLICIES } from '../timeline/cachePolicies.js';
import { OWNER_CACHE_POLICIES } from '../owner/cachePolicies.js';
import { WS_CACHE_POLICIES } from '../ws/cachePolicies.js';

export type { CachePolicyRecord } from './policy.js';

export const REQUEST_LOCAL_CACHE_EXCLUSIONS: readonly CachePolicyRecord[] = Object.freeze([
  {
    source: 'src/tools/benchmarkBoundedCacheAudit.ts',
    disposition: 'request-local-exclusion',
    scope: 'one benchmark repetition',
    invalidation: 'cleared and shut down after each repetition',
    negativeCaching: 'not applicable',
    singleFlight: 'not applicable',
    id: 'benchmarkBoundedCacheAudit:cache',
  },
  {
    source: 'src/tools/benchmarkBoundedCache.ts',
    disposition: 'request-local-exclusion',
    scope: 'one benchmark repetition',
    invalidation: 'cleared and shut down after each repetition',
    negativeCaching: 'not applicable',
    singleFlight: 'not applicable',
    id: 'benchmarkBoundedCache:cache',
  },
  {
    source: 'src/path-beta/resolver.ts',
    disposition: 'request-local-exclusion',
    scope: 'one resolver invocation',
    invalidation: 'discarded with invocation',
    negativeCaching: 'local null memoization',
    singleFlight: 'not applicable',
    id: 'resolver:hopCache',
  },
  {
    id: 'mqtt-sweep-benchmark',
    source: 'src/tools/benchmarkMqttCacheSweeps.ts',
    disposition: 'request-local-exclusion',
    scope: 'one benchmark trial',
    invalidation: 'cleared and shut down per trial',
    negativeCaching: 'not applicable',
    singleFlight: 'not applicable',
  },
]);

/** Inventory is composed from the same policies that configure storage. */
export const CACHE_POLICY_REGISTRY: readonly CachePolicyRecord[] = Object.freeze([
  ...Object.values(MQTT_CACHE_POLICIES),
  ...Object.values(API_CACHE_POLICIES),
  ...Object.values(API_BOOTSTRAP_CACHE_POLICIES),
  ...Object.values(STATS_CACHE_POLICIES),
  ...Object.values(PATH_CACHE_POLICIES),
  ...Object.values(TIMELINE_CACHE_POLICIES),
  ...Object.values(OWNER_CACHE_POLICIES),
  ...Object.values(WS_CACHE_POLICIES),
  ...REQUEST_LOCAL_CACHE_EXCLUSIONS,
]);
