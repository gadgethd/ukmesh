import { boundedEnvInteger } from '../platform/config/boundedNumber.js';
import { reactivateHistoricPathNodes } from '../repositories/pathEvidence.js';
import { pathingConfig } from '../platform/config/pathing.js';
import { getStickyNodeMap, getHeldPath, setResolveCache, resolveCacheKey, mergeStickyNodes, setHeldPath } from '../path-beta/resolveCache.js';
import { mqttPreResolveHistoryEntries, mqttPreResolveInflight, postPersistDroppedTotal, postPersistInFlightGauge } from '../metrics/ingest.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import type { WorkerPool } from '../path-beta/workerPool.js';
import type { IngestEvents } from './events.js';

export type PostPersistFollowUpArgs = {
  finalHash: string;
  decodedPathHashSizeBytes: number | undefined;
  path: string[] | undefined;
  decodedRouteType: number | undefined;
  network: string;
  resolvedPacketType: number | undefined;
};

export function createCommittedFollowUps(deps: {
  query: DatabaseQueryFn;
  resolvePool: Pick<WorkerPool, 'runBackground'>;
  scheduleSlowResolution: (packetHash: string, network: string) => void;
  emitNodeUpsert: IngestEvents['emitNodeUpsert'];
}) {
  const { query, resolvePool, scheduleSlowResolution, emitNodeUpsert } = deps;
  let closed = false;
  let sweepTimer: ReturnType<typeof setInterval> | undefined;
  const active = new Set<Promise<void>>();
  const preResolveTasks = new Set<Promise<void>>();
  /** In-flight pre-resolve tracking — prevents duplicate concurrent resolutions for the same hash/network. */
  const preResolveInFlight = new Set<string>();
  const preResolveLastStarted = new Map<string, number>();
  const PRE_RESOLVE_HISTORY_MAX = 50_000;
  const PRE_RESOLVE_ENABLED = process.env['PATH_BETA_PRERESOLVE_ENABLED'] === '1';
  const PRE_RESOLVE_MIN_INTERVAL_MS = boundedEnvInteger(
    'PATH_BETA_PRERESOLVE_MIN_INTERVAL_MS',
    10_000,
    1_000,
    24 * 60 * 60_000,
  );

  // Best-effort post-persist follow-ups are bounded: a stalled database must
  // not grow the in-flight set without limit. Over the bound they are dropped
  // (safe: everything here is explicitly best-effort) and counted.
  const POST_PERSIST_MAX_INFLIGHT = 64;
  let postPersistInFlight = 0;

  /**
   * Run the best-effort live-map follow-ups under the global in-flight bound.
   * Shared by the durable ingest path (after the committed write) and by the
   * RAM writer's post-commit hook, so a stall cannot grow the in-flight set
   * without limit in either mode; over the bound they are dropped (best-effort)
   * and counted.
   */
  function schedulePostPersistFollowUps(args: PostPersistFollowUpArgs): void {
    if (closed) return;
    if (postPersistInFlight >= POST_PERSIST_MAX_INFLIGHT) {
      postPersistDroppedTotal.inc();
      return;
    }
    postPersistInFlight++;
    postPersistInFlightGauge.set(postPersistInFlight);
    const work = runPostPersistFollowUps(args)
      .catch((err: unknown) => {
        console.error(
          '[mqtt] post-persist follow-up failed:',
          err instanceof Error ? err.message : String(err),
        );
      })
      .finally(() => {
        active.delete(work);
        postPersistInFlight--;
        postPersistInFlightGauge.set(postPersistInFlight);
      });
    active.add(work);
  }

  async function runPostPersistFollowUps(args: PostPersistFollowUpArgs): Promise<void> {
    const { finalHash, decodedPathHashSizeBytes, path, decodedRouteType, network, resolvedPacketType } = args;

    // A repeater appearing in a multibyte (2–3 byte) path hash almost certainly
    // relayed this packet. Resolve only a unique historic row with stored
    // coordinates, then broadcast the full row so clients which no longer hold
    // the stale node can restore it at its original location. Best-effort.
    if (decodedPathHashSizeBytes != null && decodedPathHashSizeBytes >= 2 && path && path.length > 0) {
      try {
        const historicNodes = await reactivateHistoricPathNodes(query, {
          pathHashes: path,
          sizeBytes: decodedPathHashSizeBytes,
          seenAt: new Date(),
          routeType: decodedRouteType,
          network,
        });
        for (const node of historicNodes) emitNodeUpsert({ ...node });
      } catch (err) {
        console.error('[mqtt] path evidence error:', (err as Error).message);
      }
      // Slow mode: schedule ONE final multi-observer resolution after the
      // propagation window closes so the complete observer set is included
      // (idempotent per packet hash; best-effort).
      if (path.length >= pathingConfig.slowModeMinPathHops) {
        scheduleSlowResolution(finalHash, network);
      }
    }

    // Pre-resolve path for ADV/GRP packets so the cache is warm before the browser requests it.
    // Bursty multi-observer packets can arrive repeatedly for the same hash; throttle those
    // warmups so live ingest cannot monopolise the shared path resolver pool.
    if (closed || !PRE_RESOLVE_ENABLED || (resolvedPacketType !== 4 && resolvedPacketType !== 5)) return;
    const preResolveKey = `${finalHash}|${network}`;
    const lastPreResolve = preResolveLastStarted.get(preResolveKey) ?? 0;
    if (preResolveInFlight.has(preResolveKey) || Date.now() - lastPreResolve < PRE_RESOLVE_MIN_INTERVAL_MS) {
      return;
    }
    preResolveInFlight.add(preResolveKey);
    mqttPreResolveInflight.set(preResolveInFlight.size);
    if (
      !preResolveLastStarted.has(preResolveKey)
      && preResolveLastStarted.size >= PRE_RESOLVE_HISTORY_MAX
    ) {
      const oldest = preResolveLastStarted.keys().next().value;
      if (oldest !== undefined) preResolveLastStarted.delete(oldest);
    }
    preResolveLastStarted.set(preResolveKey, Date.now());
    mqttPreResolveHistoryEntries.set(preResolveLastStarted.size);
    // Pass any previously resolved high-confidence hop assignments so the solver reuses them
    const stickyEntry = getStickyNodeMap(finalHash, network);
    const stickyMap = stickyEntry ? Object.fromEntries(stickyEntry.hashToNodeId) : undefined;
    const stickyAgeFraction = stickyEntry?.ageFraction;
    const heldPath = getHeldPath(finalHash, network);
    const pending = resolvePool.runBackground<{
      stickyUpdates?: Record<string, string>;
      canonicalPath?: Array<{ nodeId?: string | null }>;
    }>({ type: 'resolveMulti', packetHash: finalHash, network, ...(stickyMap ? { stickyMap, stickyAgeFraction } : {}), ...(heldPath ? { heldPath } : {}) })
      .then((result) => {
        if (result && !closed) {
          const { stickyUpdates, ...cacheableResult } = result;
          setResolveCache(resolveCacheKey('multi', finalHash, network), cacheableResult);
          if (stickyUpdates && Object.keys(stickyUpdates).length > 0) {
            mergeStickyNodes(finalHash, network, stickyUpdates);
          }
          const heldNodes = result.canonicalPath?.map((hop) => hop.nodeId ?? '').filter(Boolean) ?? [];
          if (heldNodes.length > 0 && heldNodes.length === result.canonicalPath?.length) {
            setHeldPath(finalHash, network, { path: heldNodes, resolvedAt: Date.now(), physical: true });
          }
        }
      })
      .catch(() => { /* best-effort */ })
      .finally(() => {
        preResolveTasks.delete(pending);
        preResolveInFlight.delete(preResolveKey);
        mqttPreResolveInflight.set(preResolveInFlight.size);
      });
    preResolveTasks.add(pending);
  }

  return {
    schedule: schedulePostPersistFollowUps,
    start() {
      if (closed || sweepTimer) return;
      sweepTimer = setInterval(() => {
        const cutoff = Date.now() - PRE_RESOLVE_MIN_INTERVAL_MS * 6;
        for (const [key, ts] of preResolveLastStarted) {
          if (ts < cutoff) preResolveLastStarted.delete(key);
        }
        mqttPreResolveHistoryEntries.set(preResolveLastStarted.size);
      }, 60_000).unref();
    },
    async close() {
      if (closed) return;
      closed = true;
      if (sweepTimer) clearInterval(sweepTimer);
      await Promise.allSettled([...active, ...preResolveTasks]);
      preResolveLastStarted.clear(); preResolveInFlight.clear();
      mqttPreResolveHistoryEntries.set(0); mqttPreResolveInflight.set(0);
    },
  };
}
export type CommittedFollowUps = ReturnType<typeof createCommittedFollowUps>;
