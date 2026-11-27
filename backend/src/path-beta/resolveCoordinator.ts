import { positiveIntegerSetting } from '../platform/config/boundedNumber.js';
import {
  captureResolveVersion,
  getHeldPath,
  getResolveCache,
  getStickyNodeMap,
  isCurrentResolveVersion,
  mergeStickyNodes,
  resolveCacheKey,
  setHeldPath,
  setResolveCache,
} from './resolveCache.js';
import { resolvePoolBackgroundLimits, resolveWorkTimeoutMs } from './resolvePool.js';
import { WorkerPoolTimeoutError, type WorkerPool } from './workerPool.js';

type ResolutionRequest = {
  kind: 'single' | 'multi' | 'lazy';
  packetHash: string;
  network: string;
  observer?: string | null;
};
type WorkerResult = {
  stickyUpdates?: Record<string, string>;
  canonicalPath?: Array<{ nodeId?: string | null }>;
  observers?: unknown;
} & Record<string, unknown>;
type Resolution = { payload: WorkerResult; explain: boolean };
type PendingResolution = Promise<Resolution | null>;

export class StalePathResolutionError extends Error {
  constructor() { super('PATH_RESOLVE_STALE'); }
}

/** Coordinates worker admission, cache use and publication for one API runtime. */
export function createResolveCoordinator(
  pool: Pick<WorkerPool, 'run' | 'runBackground'>,
  readVisibilityGeneration: (signal?: AbortSignal) => Promise<number>,
) {
  const interactive = new Map<string, PendingResolution>();
  const background = new Map<string, PendingResolution>();
  const observerVariants = new Map<string, Map<string, number>>();
  const interactiveLimit = positiveIntegerSetting('PATH_RESOLVE_UNIQUE_INFLIGHT_MAX', 34, 256);
  const observerLimit = positiveIntegerSetting('PATH_RESOLVE_OBSERVER_VARIANTS_PER_PACKET_MAX', 4, 16);
  const { workers, queuedBackground } = resolvePoolBackgroundLimits();
  const workTimeoutMs = resolveWorkTimeoutMs();
  let backgroundCallers = 0;
  let interactivePathTasks = 0;
  let initialAuthority: Promise<number> | undefined;
  let closed = false;

  async function readyGeneration(signal?: AbortSignal): Promise<number> {
    try {
      return await readVisibilityGeneration(signal);
    } catch (error) {
      if (signal?.aborted) throw new WorkerPoolTimeoutError();
      if (error instanceof Error && error.message === 'PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE') {
        throw new StalePathResolutionError();
      }
      throw error;
    }
  }

  function releaseObserver(packet: string, observer: string): void {
    const variants = observerVariants.get(packet);
    const count = variants?.get(observer) ?? 0;
    if (!variants) return;
    if (count <= 1) variants.delete(observer);
    else variants.set(observer, count - 1);
    if (variants.size === 0) observerVariants.delete(packet);
  }

  async function resolve(request: ResolutionRequest, urgency: 'interactive' | 'background' = 'interactive'): PendingResolution {
    if (closed) throw new Error('WORKER_POOL_CLOSED');
    const isBackground = urgency === 'background';
    if (isBackground && backgroundCallers >= workers + queuedBackground) return null;
    if (isBackground) backgroundCallers++;
    try {
      // Collapse overlapping preflight reads. The publication probe below must
      // start after the worker completes, even if a preflight is still active.
      initialAuthority ??= readyGeneration().finally(() => { initialAuthority = undefined; });
      const generation = await initialAuthority;
      if (closed) throw new Error('WORKER_POOL_CLOSED');
      const { kind, packetHash, network, observer } = request;
      const cacheKey = resolveCacheKey(kind, packetHash, network, generation, observer);
      if (kind !== 'lazy') {
        const cached = getResolveCache(cacheKey) as Resolution | undefined;
        if (cached) return cached;
      }
      const version = captureResolveVersion(packetHash);
      const key = `${cacheKey}|${version}`;
      // Background jobs can use sticky anchors. They must not consume an
      // interactive queue slot or make an interactive reader wait behind it.
      const inflight = isBackground ? background : interactive;
      const existing = inflight.get(key);
      if (existing) return await existing;
      const isInteractivePath = !isBackground && kind !== 'lazy';
      if (isInteractivePath && interactivePathTasks >= interactiveLimit) throw new Error('PATH_RESOLVE_OVERLOADED');

      const packetKey = `${packetHash}|${network}`;
      if (observer) {
        const variants = observerVariants.get(packetKey) ?? new Map<string, number>();
        if (!variants.has(observer) && variants.size >= observerLimit) throw new Error('PATH_RESOLVE_OVERLOADED');
        variants.set(observer, (variants.get(observer) ?? 0) + 1);
        observerVariants.set(packetKey, variants);
      }
      if (isInteractivePath) interactivePathTasks++;
      const task = (async () => {
        const deadline = performance.now() + workTimeoutMs;
        const sticky = isBackground ? getStickyNodeMap(packetHash, network) : undefined;
        const heldPath = kind === 'lazy' ? undefined : getHeldPath(packetHash, network);
        const job = {
          type: kind === 'single' ? 'resolve' : kind === 'multi' ? 'resolveMulti' : 'resolveLazy',
          packetHash,
          network,
          requiredVisibilityGeneration: generation,
          ...(observer ? { observer } : {}),
          ...(heldPath ? { heldPath } : {}),
          ...(sticky ? { stickyMap: Object.fromEntries(sticky.hashToNodeId), stickyAgeFraction: sticky.ageFraction } : {}),
        };
        const result = await (isBackground
          ? pool.runBackground<WorkerResult>(job)
          : pool.run<WorkerResult | null>(job)).catch((error: Error) => {
          if (error.message.startsWith('PUBLIC_VISIBILITY_CHANGED_DURING_')) throw new StalePathResolutionError();
          throw error;
        });
        if (!result) return null;
        if (closed || !isCurrentResolveVersion(packetHash, version)) throw new StalePathResolutionError();
        const remaining = Math.ceil(deadline - performance.now());
        if (remaining < 1) throw new WorkerPoolTimeoutError();
        const signal = AbortSignal.timeout(remaining);
        const current = await readyGeneration(signal);
        if (signal.aborted) throw new WorkerPoolTimeoutError();
        if (current !== generation || closed || !isCurrentResolveVersion(packetHash, version)) throw new StalePathResolutionError();

        const { stickyUpdates, ...payload } = result;
        const resolution = { payload, explain: !isBackground };
        if (kind !== 'lazy') {
          setResolveCache(cacheKey, resolution, version);
          if (isBackground && stickyUpdates) mergeStickyNodes(packetHash, network, stickyUpdates);
          const path = result.canonicalPath?.map(hop => hop.nodeId ?? '') ?? [];
          if (path.length > 0 && path.every(Boolean)) {
            setHeldPath(packetHash, network, { path, resolvedAt: Date.now(), physical: true });
          }
        }
        return resolution;
      })().finally(() => {
        inflight.delete(key);
        if (isInteractivePath) interactivePathTasks--;
        if (observer) releaseObserver(packetKey, observer);
      });
      inflight.set(key, task);
      return await task;
    } finally {
      if (isBackground) backgroundCallers--;
    }
  }

  return { resolve, close() { closed = true; } };
}
