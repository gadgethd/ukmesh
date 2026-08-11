/**
 * Short-lived in-process cache for path-beta resolve results.
 * Entries are kept until a new MQTT observation arrives for that packet hash,
 * at which point the hash is invalidated so the next request re-resolves with
 * fresh (potentially multi-observer) data.
 *
 * Entries also expire after RESOLVE_CACHE_TTL_MS to prevent unbounded growth
 * over multi-day uptime.
 */

import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

const RESOLVE_CACHE_TTL_MS = 10 * 60 * 1000;
const cache = new BoundedTtlMap<string, {
  data: unknown;
  packetHash: string;
  invalidationVersion: number;
}>({
  name: 'path_resolution',
  maxEntries: 4_096,
  maxWeight: 64 * 1024 * 1024,
  ttlMs: RESOLVE_CACHE_TTL_MS,
});
const invalidationVersions = new BoundedTtlMap<string, number>({
  name: 'path_invalidation',
  maxEntries: 50_000,
  maxWeight: 4 * 1024 * 1024,
  ttlMs: 60 * 60_000,
});

function packetHashFromKey(key: string): string {
  return key.split('|')[1]?.trim() ?? '';
}

export function getResolveCache(key: string): unknown | undefined {
  const entry = cache.get(key);
  if (!entry) return undefined;
  if (
    entry.invalidationVersion
    !== (invalidationVersions.get(entry.packetHash) ?? 0)
  ) {
    cache.delete(key);
    return undefined;
  }
  return entry.data;
}

export function setResolveCache(key: string, result: unknown): void {
  const packetHash = packetHashFromKey(key);
  if (!packetHash) return;
  cache.set(key, {
    data: result,
    packetHash,
    invalidationVersion: invalidationVersions.get(packetHash) ?? 0,
  });
}

/** Invalidate all cached results for a given packet hash (all networks/observers). */
export function invalidateResolveCache(packetHash: string): void {
  invalidationVersions.set(
    packetHash,
    (invalidationVersions.get(packetHash) ?? 0) + 1,
  );
}

export function resolveCacheMetrics() {
  return {
    results: cache.metrics(),
    invalidations: invalidationVersions.metrics(),
    sticky: stickyNodeCache.metrics(),
    held: heldPathCache.metrics(),
  };
}

export type HeldPathEntry = {
  path: string[];
  resolvedAt: number;
  physical: boolean;
};

const HELD_PATH_TTL_MS = 30 * 60 * 1000;
const heldPathCache = new BoundedTtlMap<string, HeldPathEntry>({
  name: 'path_held_paths',
  maxEntries: 2_048,
  maxWeight: 16 * 1024 * 1024,
  ttlMs: HELD_PATH_TTL_MS,
  weightOf: (key, value) => key.length * 2 + value.path.length * 128,
});

export function getHeldPath(packetHash: string, network: string): HeldPathEntry | undefined {
  const entry = heldPathCache.get(`${packetHash}|${network}`);
  return entry ? { ...entry, path: [...entry.path] } : undefined;
}

export function setHeldPath(
  packetHash: string,
  network: string,
  value: HeldPathEntry,
): void {
  if (!packetHash || !network || value.path.length === 0 || value.path.some((nodeId) => !nodeId)) return;
  heldPathCache.set(`${packetHash}|${network}`, { ...value, path: [...value.path] });
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
const stickyNodeCache = new BoundedTtlMap<string, StickyEntry>({
  name: 'path_sticky_nodes',
  maxEntries: 2_048,
  maxWeight: 16 * 1024 * 1024,
  ttlMs: STICKY_NODE_TTL_MS,
  weightOf: (key, value) => key.length * 2 + value.hashToNodeId.size * 256,
});
const STICKY_NODES_PER_PACKET_MAX = 64;

export function getStickyNodeMap(
  packetHash: string,
  network: string,
): { hashToNodeId: Map<string, string>; ageFraction: number } | undefined {
  const key = `${packetHash}|${network}`;
  const entry = stickyNodeCache.get(key);
  if (!entry) return undefined;
  const ageMs = Date.now() - entry.updatedAt;
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
    if (
      !entry.hashToNodeId.has(hash)
      && entry.hashToNodeId.size >= STICKY_NODES_PER_PACKET_MAX
    ) break;
    if (hash && nodeId) entry.hashToNodeId.set(hash, nodeId);
  }
  entry.updatedAt = Date.now();
  stickyNodeCache.set(key, entry);
}
