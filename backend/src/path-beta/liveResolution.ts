import { query } from '../db/runtime.js';
import { boundedEnvInteger } from '../platform/config/boundedNumber.js';
import { mqttPreResolveHistoryEntries, mqttPreResolveInflight } from '../metrics/ingest.js';
import { createSlowResolutionRecorder } from '../pathing/slowResolutionRepository.js';
import { createResolvePool } from './resolvePool.js';
import { createSlowResolutionScheduler } from './slowMode.js';
import {
  clearResolvedPaths,
  getHeldPath,
  getStickyNodeMap,
  invalidateResolveCache,
  mergeStickyNodes,
  resolveCacheKey,
  setHeldPath,
  setResolveCache,
} from './resolveCache.js';
import type { PathObservation } from './observation.js';
import type { WorkerPool } from './workerPool.js';

type ResolvePool = Pick<WorkerPool, 'run' | 'runBackground' | 'close'>;

export function createPathResolutionRuntime(resolvePool: ResolvePool = createResolvePool()) {
  const record = createSlowResolutionRecorder(query, () => slowMode.slowModeWindowMs());
  const slowMode = createSlowResolutionScheduler({ pool: resolvePool, record });
  const preResolveTasks = new Set<Promise<void>>();
  const preResolveInFlight = new Set<string>();
  const preResolveLastStarted = new Map<string, number>();
  const enabled = process.env['PATH_BETA_PRERESOLVE_ENABLED'] === '1';
  const intervalMs = boundedEnvInteger('PATH_BETA_PRERESOLVE_MIN_INTERVAL_MS', 10_000, 1_000, 24 * 60 * 60_000);
  const historyMax = 50_000;
  let streamGeneration = 0;
  let sweepTimer: NodeJS.Timeout | undefined;
  let closed = false;

  function acceptObservation({ packetHash, network, slow, warm }: PathObservation): void {
    if (closed) return;
    invalidateResolveCache(packetHash);
    if (slow) slowMode.scheduleSlowResolution(packetHash, network);
    if (!warm || !enabled) return;
    const preResolveKey = `${packetHash}|${network}`;
    const lastPreResolve = preResolveLastStarted.get(preResolveKey) ?? 0;
    if (preResolveInFlight.has(preResolveKey) || Date.now() - lastPreResolve < intervalMs) {
      return;
    }
    preResolveInFlight.add(preResolveKey);
    mqttPreResolveInflight.set(preResolveInFlight.size);
    if (
      !preResolveLastStarted.has(preResolveKey)
      && preResolveLastStarted.size >= historyMax
    ) {
      const oldest = preResolveLastStarted.keys().next().value;
      if (oldest !== undefined) preResolveLastStarted.delete(oldest);
    }
    preResolveLastStarted.set(preResolveKey, Date.now());
    mqttPreResolveHistoryEntries.set(preResolveLastStarted.size);
    const stickyEntry = getStickyNodeMap(packetHash, network);
    const stickyMap = stickyEntry ? Object.fromEntries(stickyEntry.hashToNodeId) : undefined;
    const stickyAgeFraction = stickyEntry?.ageFraction;
    const heldPath = getHeldPath(packetHash, network);
    const cacheKey = resolveCacheKey('multi', packetHash, network);
    const generation = streamGeneration;
    const pending = resolvePool.runBackground<{
      stickyUpdates?: Record<string, string>;
      canonicalPath?: Array<{ nodeId?: string | null }>;
    }>({ type: 'resolveMulti', packetHash, network,
      ...(stickyMap ? { stickyMap, stickyAgeFraction } : {}), ...(heldPath ? { heldPath } : {}) })
      .then((result) => {
        if (result && !closed && generation === streamGeneration) {
          const { stickyUpdates, ...cacheableResult } = result;
          setResolveCache(cacheKey, cacheableResult);
          if (stickyUpdates && Object.keys(stickyUpdates).length > 0) {
            mergeStickyNodes(packetHash, network, stickyUpdates);
          }
          const heldNodes = result.canonicalPath?.map((hop) => hop.nodeId ?? '').filter(Boolean) ?? [];
          if (heldNodes.length > 0 && heldNodes.length === result.canonicalPath?.length) {
            setHeldPath(packetHash, network, { path: heldNodes, resolvedAt: Date.now(), physical: true });
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
    resolvePool,
    slowMode,
    acceptObservation,
    clearResolvedPaths() {
      streamGeneration++;
      clearResolvedPaths();
    },
    start() {
      if (closed || sweepTimer || !enabled) return;
      sweepTimer = setInterval(() => {
        const cutoff = Date.now() - intervalMs * 6;
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
      await Promise.allSettled([slowMode.close(), ...preResolveTasks]);
      preResolveLastStarted.clear();
      preResolveInFlight.clear();
      mqttPreResolveHistoryEntries.set(0);
      mqttPreResolveInflight.set(0);
      await resolvePool.close();
    },
  };
}
