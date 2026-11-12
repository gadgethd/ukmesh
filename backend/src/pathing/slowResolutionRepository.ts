import type { DatabaseQueryFn } from '../db/contracts.js';

/** Observability row for the slow final — best-effort, never blocks ingest. */
export function createSlowResolutionRecorder(query: DatabaseQueryFn, slowModeWindowMs: () => number) {
 return async (
  packetHash: string,
  network: string,
  result: { observers?: unknown; canonicalPath?: unknown },
): Promise<void> => {
  await query(
    `INSERT INTO analytics.path_slow_resolutions
       (packet_hash, network, window_ms, observers_seen, resolved_at, canonical_path)
     VALUES ($1, $2, $3, $4, now(), $5)
     ON CONFLICT (packet_hash, network) DO UPDATE SET
       window_ms = EXCLUDED.window_ms,
       observers_seen = EXCLUDED.observers_seen,
       resolved_at = now(),
       canonical_path = EXCLUDED.canonical_path`,
    [
      packetHash,
      network,
      slowModeWindowMs(),
      Array.isArray(result.observers) ? result.observers.length : 0,
      result.canonicalPath != null ? JSON.stringify(result.canonicalPath) : null,
    ],
  );
  };
}
