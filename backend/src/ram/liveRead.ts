/**
 * RAM-first MQTT ingestion — merged read helper.
 *
 * The live read paths (recent packets, recent messages, WS initial state)
 * keep reading PostgreSQL. In RAM mode the batch flush window (up to
 * MQTT_RAM_MAX_AGE_MS) would hide recently admitted packets from those
 * queries, so this helper appends the pending stream entries — mapped to the
 * same row shape the SQL queries return — on top of the database result.
 *
 * Correctness notes:
 *  - only `visibilityOk` (public) pending entries are merged — the same
 *    public-privacy filter the SQL applies;
 *  - rows are deduplicated by packet hash against the database result, which
 *    also covers the tiny commit→XDEL bookkeeping window;
 *  - purely best-effort: any Redis failure returns the database rows
 *    unchanged, never an error.
 */
import { readPendingEntries } from './liveStore.js';
import { classifyForAdmission } from '../db/packetBatch.js';
import { getRamIngestMode } from './protocol.js';

export type RecentRow = Record<string, unknown>;

export type MergeOptions = {
  limit: number;
  network?: string;
  observer?: string;
  includePayload?: boolean;
};

function upper(value: unknown): string | null {
  const text = typeof value === 'string' ? value.trim() : '';
  return text.length > 0 ? text.toUpperCase() : null;
}

function timeMs(value: unknown): number {
  const parsed = Date.parse(String(value ?? ''));
  return Number.isFinite(parsed) ? parsed : 0;
}

export async function mergePendingPacketsIntoRecentRows<T>(
  rows: T[],
  opts: MergeOptions,
): Promise<T[]> {
  if (getRamIngestMode() !== 'ram' || opts.limit <= 0) return rows;
  try {
    const pending = await readPendingEntries(opts.limit);
    if (pending.length === 0) return rows;

    const base = rows as unknown as RecentRow[];
    const seen = new Set<string>();
    for (const row of base) {
      const hash = String(row['packet_hash'] ?? '').toLowerCase();
      if (hash) seen.add(hash);
    }

    const merged: RecentRow[] = [];
    for (const { entry } of pending) {
      if (!entry.visibilityOk) continue;
      const input = entry.input;
      // Re-classify against the CURRENT privacy cache: the entry's baked-in
      // flags can lag a node-privacy flip, so a pending packet that is now
      // private must not appear in public merged reads. Fails closed when the
      // cache is not loaded (generation 0 -> visibilityOk false).
      const fresh = await classifyForAdmission({
        network: input.network,
        rxNodeId: input.rxNodeId,
        srcNodeId: input.srcNodeId,
        pathHashes: input.pathHashes,
        pathHashSizeBytes: input.pathHashSizeBytes,
      });
      if (!fresh.visibilityOk) continue;
      if (opts.network && input.network !== opts.network) continue;
      if (opts.observer && upper(input.rxNodeId) !== opts.observer.toUpperCase()) continue;
      const hash = String(input.packetHash ?? '').toLowerCase();
      if (!hash || seen.has(hash)) continue;
      seen.add(hash);

      let payload: Record<string, unknown> | null = null;
      let summary: unknown = null;
      if (input.payloadJson) {
        try {
          const parsed = JSON.parse(input.payloadJson) as Record<string, unknown>;
          if (parsed && typeof parsed === 'object') {
            payload = parsed;
            summary = parsed['_summary'] ?? null;
          }
        } catch {
          /* stored payload is best-effort for the merge */
        }
      }
      const direction = typeof payload?.['direction'] === 'string' ? (payload['direction'] as string) : '';
      const isTx = direction === 'tx';

      merged.push({
        time: input.time,
        packet_hash: input.packetHash,
        rx_node_id: upper(input.rxNodeId),
        src_node_id: upper(input.srcNodeId),
        topic: input.topic,
        topic_prefix: input.topicPrefix,
        iata: input.iata,
        packet_type: input.packetType,
        route_type: input.routeType,
        hop_count: input.hopCount,
        rssi: input.rssi,
        snr: input.snr,
        summary,
        advert_count: input.advertCount,
        path_hashes: input.pathHashes,
        path_hash_size_bytes: input.pathHashSizeBytes,
        network: input.network,
        transport_codes: input.transportCodes,
        region_scope: input.regionScope,
        observer_node_ids: input.rxNodeId ? [String(input.rxNodeId).toUpperCase()] : [],
        observer_iatas: input.iata ? [input.iata] : [],
        rx_count: isTx ? 0 : 1,
        tx_count: isTx ? 1 : 0,
        ...(opts.includePayload ? { payload } : {}),
      });
    }
    if (merged.length === 0) return rows;

    const combined = [...base, ...merged];
    combined.sort((left, right) => timeMs(right['time']) - timeMs(left['time']));
    return combined.slice(0, opts.limit) as unknown as T[];
  } catch (error) {
    console.error('[ram] live merge failed (returning database rows only)', (error as Error).message);
    return rows;
  }
}
