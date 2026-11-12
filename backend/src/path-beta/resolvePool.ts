/**
 * Application-owned worker pool shared by HTTP and committed ingest work.
 * Two workers keeps the main event loop free during CPU-heavy path computation
 * while still handling bursts without excessive DB connection overhead.
 */
import { WorkerPool } from './workerPool.js';

export function createResolvePool() {
  return new WorkerPool(
    new URL('./resolveWorker.js', import.meta.url),
    Math.min(8, Math.max(1, Number(process.env['PATH_RESOLVE_WORKERS'] ?? 2) || 2)),
    Math.min(1024, Math.max(1, Number(process.env['PATH_RESOLVE_BACKGROUND_QUEUE_MAX'] ?? 128) || 128)),
    Math.min(256, Math.max(1, Number(process.env['PATH_RESOLVE_INTERACTIVE_QUEUE_MAX'] ?? 32) || 32)),
    Math.min(120_000, Math.max(1_000, Number(process.env['PATH_RESOLVE_END_TO_END_TIMEOUT_MS'] ?? 15_000) || 15_000)),
  );
}
