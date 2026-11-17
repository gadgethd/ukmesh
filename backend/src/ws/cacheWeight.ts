import { setImmediate as checkpoint } from 'node:timers/promises';

const CHECKPOINT_TARGET_MS = 4;

/** WS-owned plain snapshots: ordinary row arrays, native Dates and parsed JSON.
 * Preserve the full legacy JSON byte weight without retaining one large string.
 * Yield between rows; a single row serialization cannot be preempted. */
export async function measureSnapshotWeight(entry: Record<string, unknown>, signal: AbortSignal): Promise<number> {
  try {
    signal.throwIfAborted();
    const fields = Object.entries(entry);
    const arrays = fields.filter((field): field is [string, unknown[]] => Array.isArray(field[1]));
    const skeleton = Object.fromEntries(fields.map(([key, value]) => [key, Array.isArray(value) ? [] : value]));
    let bytes = Buffer.byteLength(JSON.stringify(skeleton));
    let sliceStarted = performance.now();
    for (const [, rows] of arrays) {
      // The skeleton already includes each array's two brackets.
      if (rows.length > 1) bytes += rows.length - 1;
      for (const row of rows) {
        bytes += Buffer.byteLength(JSON.stringify(row) ?? 'null');
        if (performance.now() - sliceStarted >= CHECKPOINT_TARGET_MS) {
          signal.throwIfAborted();
          await checkpoint(undefined, { signal });
          sliceStarted = performance.now();
        }
      }
    }
    signal.throwIfAborted();
    return bytes;
  } catch {
    // Cancellation must reach the fetch owner. Unserializable DTOs retain the
    // existing byte-bounded cache's Infinity rejection behavior.
    signal.throwIfAborted();
    return Number.POSITIVE_INFINITY;
  }
}
