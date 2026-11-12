import { TIMELINE_CACHE_POLICIES } from './cachePolicies.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import { activityTimelineRefreshInflight } from '../metrics/stats.js';

const FRESH_MS = TIMELINE_CACHE_POLICIES.completedSnapshot.freshMs;
const MAX_REFRESHES = TIMELINE_CACHE_POLICIES.completedSnapshot.maxRefreshes;

/** Completed snapshots only. Delivery still passes through publicJson's fresh
 * authority reads; callers include the generation and every query scope in key. */
export function createActivityTimelineCache() {
  const cache = new BoundedTtlMap<string, { ts: number; data: unknown }>(TIMELINE_CACHE_POLICIES.completedSnapshot);
  const inflight = new Map<string, Promise<unknown>>();
  let closed = false;

  function refresh(key: string, generation: number, readGeneration: () => Promise<number>, load: () => Promise<unknown>) {
    let pending!: Promise<unknown>;
    activityTimelineRefreshInflight.inc();
    pending = (async () => {
      const data = await load();
      if (await readGeneration() !== generation) throw new Error('PUBLIC_VISIBILITY_CHANGED');
      if (!closed) cache.set(key, { ts: Date.now(), data });
      return data;
    })().finally(() => {
      activityTimelineRefreshInflight.dec();
      if (inflight.get(key) === pending) inflight.delete(key);
    });
    inflight.set(key, pending);
    return pending;
  }

  return {
    async get(key: string, generation: number, readGeneration: () => Promise<number>, load: () => Promise<unknown>) {
      if (closed) return load();
      const cached = cache.get(key);
      if (cached && Date.now() - cached.ts < FRESH_MS) return cached.data;
      const pending = inflight.get(key);
      if (cached) {
        if (!pending && inflight.size < MAX_REFRESHES) {
          void refresh(key, generation, readGeneration, load).catch(error => {
            console.error('[api] activity timeline refresh failed', (error as Error).message);
          });
        }
        return cached.data;
      }
      if (pending) return pending;
      // Keep detached refresh work bounded. At capacity an uncached request
      // retains the original awaited SQL path and publicJson delivery fences.
      if (inflight.size >= MAX_REFRESHES) return load();
      return refresh(key, generation, readGeneration, load);
    },
    metrics: () => ({ ...cache.metrics(), inflight: inflight.size }),
    shutdown() { closed = true; cache.shutdown(); cache.clear(); inflight.clear(); },
  };
}
