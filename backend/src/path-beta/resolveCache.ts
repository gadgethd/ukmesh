/**
 * Short-lived in-process cache for path-beta resolve results.
 * Entries are kept until a new MQTT observation arrives for that packet hash,
 * at which point the hash is invalidated so the next request re-resolves with
 * fresh (potentially multi-observer) data.
 */

const cache = new Map<string, unknown>();

export function getResolveCache(key: string): unknown | undefined {
  return cache.get(key);
}

export function setResolveCache(key: string, result: unknown): void {
  cache.set(key, result);
}

/** Invalidate all cached results for a given packet hash (all networks/observers). */
export function invalidateResolveCache(packetHash: string): void {
  for (const key of cache.keys()) {
    if (key.includes(`|${packetHash}|`) || key.endsWith(`|${packetHash}`)) {
      cache.delete(key);
    }
  }
}
