/**
 * Slow-mode resolution scheduler for path-beta.
 *
 * When a multibyte packet first arrives, the eager path resolves it
 * immediately with whatever observers have heard it so far. But the packet
 * keeps propagating through the mesh for tens of seconds after the first
 * hearing (real data: spread between first and last observer p50 11s,
 * p90 28s, p95 34s, p99 50s; median 17 observers per packet) — and
 * multi-observer consensus is stronger than a partial observer set.
 *
 * Slow mode waits out the propagation window from the FIRST observation,
 * then runs ONE final multi-observer resolution over the complete observer
 * set. The final result is pushed into the resolve cache (after bumping the
 * invalidation generation) so the next API read — fast or slow — serves the
 * full-propagation answer; the `mode=slow` variant of /path-beta/resolve-multi
 * additionally reports `{ status: 'pending', remainingMs }` while inside the
 * window so callers can block for the final answer.
 *
 * Best-effort by design: loss of the in-process schedule on restart is
 * harmless (the eager path still resolves, and mode=slow falls through to a
 * fresh resolve once the window has passed).
 */
import { pathingConfig } from '../platform/config/pathing.js';
import {
  invalidateResolveCache,
  setResolveCache,
  mergeStickyNodes,
  getStickyNodeMap,
  getHeldPath,
  setHeldPath,
  resolveCacheKey,
} from './resolveCache.js';

type SlowModePool = { runBackground<T>(job: unknown): Promise<T | null> };
type SlowRecorder = (
  packetHash: string,
  network: string,
  result: { observers?: unknown; canonicalPath?: unknown },
) => Promise<void>;
export function createSlowResolutionScheduler(deps: { pool: SlowModePool; record: SlowRecorder }) {
  let closed = false;
  const active = new Set<Promise<void>>();
  type PendingEntry = {
    timer: NodeJS.Timeout;
    network: string;
    scheduledAt: number;
  };

  const pending = new Map<string, PendingEntry>();

  function pendingMax(): number {
    const v = Number(process.env['PATH_SLOW_MODE_PENDING_MAX']);
    if (Number.isFinite(v) && v > 0) return Math.min(50_000, Math.floor(v));
    return 20_000;
  }

  function slowModeEnabled(): boolean {
    const v = process.env['PATH_SLOW_MODE_ENABLED'];
    if (v !== undefined) return v !== 'false';
    return pathingConfig.slowModeEnabled;
  }

  function slowModeWindowMs(): number {
    const v = Number(process.env['PATH_SLOW_MODE_WINDOW_MS']);
    if (Number.isFinite(v) && v > 0) return Math.min(300_000, Math.max(50, v));
    return pathingConfig.slowModeWindowMs;
  }

  /** Register the first sighting of a path-bearing packet: schedules exactly
   * one final multi-observer resolution after the propagation window closes.
   * Idempotent per (packetHash, network); bounded in-flight set. */
  function scheduleSlowResolution(packetHash: string, network: string): void {
    if (closed || !slowModeEnabled()) return;
    if (!packetHash || !network) return;
    const key = `${packetHash}|${network}`;
    if (pending.has(key)) return;
    const max = pendingMax();
    if (pending.size >= max) {
      // evict the oldest entry (Map insertion order) to bound memory
      const oldest = pending.keys().next().value as string | undefined;
      if (oldest !== undefined) {
        const entry = pending.get(oldest);
        if (entry) clearTimeout(entry.timer);
        pending.delete(oldest);
      }
    }
    const windowMs = slowModeWindowMs();
    const timer = setTimeout(() => {
      pending.delete(key);
      const work = finalizeSlowResolution(packetHash, network).finally(() => active.delete(work));
      active.add(work);
    }, windowMs);
    timer.unref?.();
    pending.set(key, { timer, network, scheduledAt: Date.now() });
  }

  /** Milliseconds until the window closes for this packet, or 0 if not pending. */
  function slowModeRemainingMs(packetHash: string, network: string): number {
    const entry = pending.get(`${packetHash}|${network}`);
    if (!entry) return 0;
    const remaining = entry.scheduledAt + slowModeWindowMs() - Date.now();
    return Math.max(0, remaining);
  }

  function slowModePendingCount(): number {
    return pending.size;
  }

  function slowModeStatus(): {
    enabled: boolean;
    windowMs: number;
    pending: number;
    pendingMax: number;
  } {
    return {
      enabled: slowModeEnabled(),
      windowMs: slowModeWindowMs(),
      pending: slowModePendingCount(),
      pendingMax: pendingMax(),
    };
  }

  async function finalizeSlowResolution(packetHash: string, network: string): Promise<void> {
    try {
      // Force the next API read to re-resolve with the complete observer set.
      invalidateResolveCache(packetHash);
      const stickyEntry = getStickyNodeMap(packetHash, network);
      const stickyMap = stickyEntry
        ? Object.fromEntries(stickyEntry.hashToNodeId)
        : undefined;
      const stickyAgeFraction = stickyEntry?.ageFraction;
      const heldPath = getHeldPath(packetHash, network);
      const result = await deps.pool.runBackground<{
        stickyUpdates?: Record<string, string>;
        observers?: unknown;
        canonicalPath?: unknown;
      }>({
        type: 'resolveMulti',
        packetHash,
        network,
        ...(stickyMap ? { stickyMap, stickyAgeFraction } : {}),
        ...(heldPath ? { heldPath } : {}),
      });
      if (!result || closed) return;
      const { stickyUpdates, ...cacheableResult } = result;
      setResolveCache(resolveCacheKey('multi', packetHash, network), cacheableResult);
      if (stickyUpdates && Object.keys(stickyUpdates).length > 0) {
        mergeStickyNodes(packetHash, network, stickyUpdates);
      }
      const canonicalPath = Array.isArray(result.canonicalPath) ? result.canonicalPath : [];
      const heldNodes = canonicalPath
        .map((hop) => hop && typeof hop === 'object' && 'nodeId' in hop
          ? String((hop as { nodeId?: unknown }).nodeId ?? '')
          : '')
        .filter(Boolean);
      if (heldNodes.length > 0 && heldNodes.length === canonicalPath.length) {
        setHeldPath(packetHash, network, { path: heldNodes, resolvedAt: Date.now(), physical: true });
      }
      await deps.record(packetHash, network, result).catch(() => {
        /* observability is best-effort */
      });
    } catch (err) {
      console.error('[slow-mode] finalize failed', (err as Error).message);
    }
  }

  return {
    scheduleSlowResolution, slowModeRemainingMs, slowModePendingCount,
    slowModeStatus, slowModeWindowMs,
    async close() {
      if (closed) return;
      closed = true;
      for (const entry of pending.values()) clearTimeout(entry.timer);
      pending.clear();
      await Promise.allSettled([...active]);
    },
  };
}
export type SlowResolutionScheduler = ReturnType<typeof createSlowResolutionScheduler>;
