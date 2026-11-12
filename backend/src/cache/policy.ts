export type CachePolicyRecord = Readonly<{
  id: string;
  source: string;
  disposition: 'bounded-cache' | 'request-local-exclusion';
  name?: string;
  maxEntries?: number;
  maxWeight?: number;
  ttlMs?: number;
  scope: string;
  invalidation: string;
  negativeCaching: string;
  singleFlight: string;
}>;

export type BoundedCachePolicy = CachePolicyRecord & Readonly<{
  disposition: 'bounded-cache'; maxEntries: number; maxWeight: number; ttlMs: number;
}>;

export function defineCachePolicy<T extends BoundedCachePolicy>(policy: T): Readonly<T> {
  return Object.freeze(policy);
}
