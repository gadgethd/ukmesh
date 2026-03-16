/**
 * Shared singleton worker pool for path-beta resolution.
 * Imported by both routes.ts (HTTP handlers) and mqtt/client.ts (pre-resolve on ingestion).
 * Two workers keeps the main event loop free during CPU-heavy path computation
 * while still handling bursts without excessive DB connection overhead.
 */
import { WorkerPool } from './workerPool.js';

export const resolvePool = new WorkerPool(
  new URL('./resolveWorker.js', import.meta.url),
  2,
);
