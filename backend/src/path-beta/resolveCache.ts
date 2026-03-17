/**
 * Short-lived in-process cache for path-beta resolve results.
 * Entries are kept until a new MQTT observation arrives for that packet hash,
 * at which point the hash is invalidated so the next request re-resolves with
 * fresh (potentially multi-observer) data.
 *
 * Entries also expire after RESOLVE_CACHE_TTL_MS to prevent unbounded growth
 * over multi-day uptime.
 */

const RESOLVE_CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes

type CacheEntry = { data: unknown; cachedAt: number };
const cache = new Map<string, CacheEntry>();

export function getResolveCache(key: string): unknown | undefined {
  const entry = cache.get(key);
  if (!entry) return undefined;
  if (Date.now() - entry.cachedAt > RESOLVE_CACHE_TTL_MS) {
    cache.delete(key);
    return undefined;
  }
  return entry.data;
}

export function setResolveCache(key: string, result: unknown): void {
  cache.set(key, { data: result, cachedAt: Date.now() });
}

/** Invalidate all cached results for a given packet hash (all networks/observers). */
export function invalidateResolveCache(packetHash: string): void {
  for (const key of cache.keys()) {
    if (key.includes(`|${packetHash}|`) || key.endsWith(`|${packetHash}`)) {
      cache.delete(key);
    }
  }
}
