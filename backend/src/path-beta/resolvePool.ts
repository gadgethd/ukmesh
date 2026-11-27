/**
 * API-owned worker pool shared by HTTP and background path resolution.
 * One worker keeps computation off the main event loop while avoiding a
 * duplicate worker-owned path context. Larger hosts can raise PATH_RESOLVE_WORKERS.
 */
import { WorkerPool } from './workerPool.js';

export function createResolvePool() {
  const { workers, queuedBackground } = resolvePoolBackgroundLimits();
  return new WorkerPool(
    new URL('./resolveWorker.js', import.meta.url),
    workers,
    queuedBackground,
    Math.min(256, Math.max(1, Number(process.env['PATH_RESOLVE_INTERACTIVE_QUEUE_MAX'] ?? 32) || 32)),
    resolveWorkTimeoutMs(),
  );
}

export function resolveWorkTimeoutMs(): number {
  return Math.min(120_000, Math.max(1_000, Number(process.env['PATH_RESOLVE_END_TO_END_TIMEOUT_MS'] ?? 15_000) || 15_000));
}

export function resolvePoolBackgroundLimits() {
  return {
    workers: Math.min(8, Math.max(1, Number(process.env['PATH_RESOLVE_WORKERS'] ?? 1) || 1)),
    queuedBackground: Math.min(1024, Math.max(1, Number(process.env['PATH_RESOLVE_BACKGROUND_QUEUE_MAX'] ?? 128) || 128)),
  };
}
