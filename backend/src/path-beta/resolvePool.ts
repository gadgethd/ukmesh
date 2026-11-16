/**
 * Application-owned worker pool shared by HTTP and committed ingest work.
 * One worker keeps computation off the main event loop while avoiding a
 * duplicate worker-owned path context. Larger hosts can raise PATH_RESOLVE_WORKERS.
 */
import { WorkerPool } from './workerPool.js';

export function createResolvePool() {
  return new WorkerPool(
    new URL('./resolveWorker.js', import.meta.url),
    Math.min(8, Math.max(1, Number(process.env['PATH_RESOLVE_WORKERS'] ?? 1) || 1)),
    Math.min(1024, Math.max(1, Number(process.env['PATH_RESOLVE_BACKGROUND_QUEUE_MAX'] ?? 128) || 128)),
    Math.min(256, Math.max(1, Number(process.env['PATH_RESOLVE_INTERACTIVE_QUEUE_MAX'] ?? 32) || 32)),
    Math.min(120_000, Math.max(1_000, Number(process.env['PATH_RESOLVE_END_TO_END_TIMEOUT_MS'] ?? 15_000) || 15_000)),
  );
}
