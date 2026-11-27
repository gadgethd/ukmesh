import { query } from '../db/runtime.js';
import { boundedEnvInteger } from '../platform/config/boundedNumber.js';
import { mqttPreResolveHistoryEntries, mqttPreResolveInflight } from '../metrics/ingest.js';
import { createSlowResolutionRecorder } from '../pathing/slowResolutionRepository.js';
import { readReadyVisibilityGeneration } from '../privacy/authority.js';
import { createResolvePool } from './resolvePool.js';
import { createResolveCoordinator, StalePathResolutionError } from './resolveCoordinator.js';
import { createSlowResolutionScheduler } from './slowMode.js';
import { clearResolvedPaths, invalidateResolveCache } from './resolveCache.js';
import type { PathObservation } from './observation.js';
import type { WorkerPool } from './workerPool.js';

type ResolvePool = Pick<WorkerPool, 'run' | 'runBackground' | 'close'>;

export function createPathResolutionRuntime(
  resolvePool: ResolvePool = createResolvePool(),
  readVisibilityGeneration = readReadyVisibilityGeneration,
) {
  const resolution = createResolveCoordinator(resolvePool, readVisibilityGeneration);
  const record = createSlowResolutionRecorder(query, () => slowMode.slowModeWindowMs());
  const slowMode = createSlowResolutionScheduler({
    async finalize(packetHash, network) {
      // The propagation deadline requires a final solve even if no observer
      // arrived after the last interactive or warm result was cached.
      invalidateResolveCache(packetHash);
      try {
        return (await resolution.resolve({ kind: 'multi', packetHash, network }, 'background'))?.payload ?? null;
      } catch (error) {
        if (error instanceof StalePathResolutionError) return null;
        throw error;
      }
    },
    record,
  });
  const preResolveTasks = new Set<Promise<void>>();
  const preResolveInFlight = new Set<string>();
  const preResolveLastStarted = new Map<string, number>();
  const enabled = process.env['PATH_BETA_PRERESOLVE_ENABLED'] === '1';
  const intervalMs = boundedEnvInteger('PATH_BETA_PRERESOLVE_MIN_INTERVAL_MS', 10_000, 1_000, 24 * 60 * 60_000);
  const historyMax = 50_000;
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
    const pending = resolution.resolve({ kind: 'multi', packetHash, network }, 'background')
      .then(() => undefined, () => { /* best-effort */ })
      .finally(() => {
        preResolveTasks.delete(pending);
        preResolveInFlight.delete(preResolveKey);
        mqttPreResolveInflight.set(preResolveInFlight.size);
      });
    preResolveTasks.add(pending);
  }

  return {
    resolve: resolution.resolve,
    slowMode,
    acceptObservation,
    clearResolvedPaths,
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
      resolution.close();
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
