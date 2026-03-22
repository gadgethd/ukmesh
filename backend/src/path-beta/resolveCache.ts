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

/**
 * Sticky node anchors — persists across resolve cache invalidations so that
 * re-resolutions triggered by new observations reuse the same high-confidence
 * hop assignments instead of picking different nodes each time.
 *
 * Keyed by `${packetHash}|${network}`, value is a map of normalizedHash → nodeId
 * for every hop that was resolved with confidence >= the purple threshold.
 * New high-confidence assignments are merged in (never overwritten with lower ones).
 */
const STICKY_NODE_TTL_MS = 30 * 60 * 1000; // 30 minutes
type StickyEntry = { hashToNodeId: Map<string, string>; updatedAt: number };
const stickyNodeCache = new Map<string, StickyEntry>();

export function getStickyNodeMap(
  packetHash: string,
  network: string,
): { hashToNodeId: Map<string, string>; ageFraction: number } | undefined {
  const key = `${packetHash}|${network}`;
  const entry = stickyNodeCache.get(key);
  if (!entry) return undefined;
  const ageMs = Date.now() - entry.updatedAt;
  if (ageMs > STICKY_NODE_TTL_MS) {
    stickyNodeCache.delete(key);
    return undefined;
  }
  // ageFraction: 0 = brand new, 1 = at TTL boundary
  const ageFraction = ageMs / STICKY_NODE_TTL_MS;
  return { hashToNodeId: entry.hashToNodeId, ageFraction };
}

/** Save a pre-filtered hash→nodeId map of confident hops for this packet. */
export function mergeStickyNodes(
  packetHash: string,
  network: string,
  updates: Record<string, string>,
): void {
  if (Object.keys(updates).length === 0) return;
  const key = `${packetHash}|${network}`;
  let entry = stickyNodeCache.get(key);
  if (!entry) {
    entry = { hashToNodeId: new Map(), updatedAt: Date.now() };
    stickyNodeCache.set(key, entry);
  }
  for (const [hash, nodeId] of Object.entries(updates)) {
    if (hash && nodeId) entry.hashToNodeId.set(hash, nodeId);
  }
  entry.updatedAt = Date.now();
}
