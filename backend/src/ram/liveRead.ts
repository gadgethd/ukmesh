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
 *  - only entries classified public against the CURRENT privacy cache are
 *    merged — the same public-privacy filter the SQL applies (fails closed);
 *  - pending observations are aggregated per packet hash, mirroring the SQL's
 *    observer aggregation: multiple observers of one RF packet contribute a
 *    single row with combined observer arrays and RX/TX counts;
 *  - when the hash is already present in the database result, stable
 *    observation IDs distinguish retained overlap from new sightings;
 *  - purely best-effort: any Redis failure returns the database rows
 *    unchanged, never an error.
 */
import { readPendingEntries } from './liveStore.js';
import { loadCurrentPublicPrivacyPolicy } from '../db/packetBatch.js';
import { getRamIngestMode, type RamPendingEntry } from './protocol.js';

export type RecentRow = Record<string, unknown>;

export type MergeOptions = {
  limit: number;
  network?: string;
  observer?: string;
  includePayload?: boolean;
  fields?: 'full' | 'slim';
  /** Restrict pending contributions to these packet types (e.g. [5] for message history). */
  packetTypes?: number[];
};

function upper(value: unknown): string | null {
  const text = typeof value === 'string' ? value.trim() : '';
  return text.length > 0 ? text.toUpperCase() : null;
}

function timeMs(value: unknown): number {
  const parsed = Date.parse(String(value ?? ''));
  return Number.isFinite(parsed) ? parsed : 0;
}

function entryIsTx(entry: RamPendingEntry): boolean {
  if (!entry.input.payloadJson) return false;
  try {
    const parsed = JSON.parse(entry.input.payloadJson) as Record<string, unknown>;
    return parsed?.['direction'] === 'tx';
  } catch {
    return false;
  }
}

function entryPayload(entry: RamPendingEntry): { payload: Record<string, unknown> | null; summary: unknown } {
  if (!entry.input.payloadJson) return { payload: null, summary: null };
  try {
    const parsed = JSON.parse(entry.input.payloadJson) as Record<string, unknown>;
    if (parsed && typeof parsed === 'object') {
      return { payload: parsed, summary: parsed['_summary'] ?? null };
    }
  } catch {
    /* stored payload is best-effort for the merge */
  }
  return { payload: null, summary: null };
}

type EntryWithTx = { entry: RamPendingEntry; isTx: boolean };

/** Aggregate pending observations into a single row (mirrors the SQL observer aggregation). */
function buildMergedRow(group: EntryWithTx[], includePayload: boolean): RecentRow {
  const newestFirst = [...group].sort((a, b) => b.entry.admittedAtMs - a.entry.admittedAtMs);
  const representative = newestFirst.find((item) => upper(item.entry.input.srcNodeId)) ?? newestFirst[0]!;
  const observers = new Set<string>();
  const iatas = new Set<string>();
  let rxCount = 0;
  let txCount = 0;
  for (const { entry, isTx } of group) {
    const rx = upper(entry.input.rxNodeId);
    if (rx) observers.add(rx);
    if (entry.input.iata) iatas.add(entry.input.iata);
    if (isTx) txCount += 1;
    else rxCount += 1;
  }
  const input = representative.entry.input;
  const { payload, summary } = entryPayload(representative.entry);
  return {
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
    observer_node_ids: [...observers],
    observer_iatas: [...iatas],
    rx_count: rxCount,
    tx_count: txCount,
    ...(includePayload ? { payload } : {}),
  };
}

/** Add genuinely new pending observations to an existing database row. */
function augmentRow(row: RecentRow, group: EntryWithTx[]): void {
  const committedIds = new Set<string>(
    Array.isArray(row['_observation_ids'])
      ? (row['_observation_ids'] as unknown[]).map(String)
      : [],
  );
  const existing = new Set<string>(
    (Array.isArray(row['observer_node_ids']) ? (row['observer_node_ids'] as unknown[]) : [])
      .map((value) => String(value).toUpperCase()),
  );
  const iatas = new Set<string>(
    (Array.isArray(row['observer_iatas']) ? (row['observer_iatas'] as unknown[]) : []).map(String),
  );
  let rxAdd = 0;
  let txAdd = 0;
  for (const { entry, isTx } of group) {
    if (committedIds.has(entry.observationId)) continue;
    committedIds.add(entry.observationId);
    const rx = upper(entry.input.rxNodeId);
    if (rx) existing.add(rx);
    if (entry.input.iata) iatas.add(entry.input.iata);
    if (isTx) txAdd += 1;
    else rxAdd += 1;
  }
  if (rxAdd + txAdd > 0) {
    row['observer_node_ids'] = [...existing];
    row['observer_iatas'] = [...iatas];
    row['rx_count'] = Number(row['rx_count'] ?? 0) + rxAdd;
    row['tx_count'] = Number(row['tx_count'] ?? 0) + txAdd;
  }
}

function stripInternalIds<T>(rows: T[]): T[] {
  for (const row of rows) {
    if (row && typeof row === 'object') delete (row as RecentRow)['_observation_ids'];
    if (row && typeof row === 'object') delete (row as RecentRow)['_policy_network'];
  }
  return rows;
}

export async function mergePendingPacketsIntoRecentRows<T>(
  rows: T[],
  opts: MergeOptions,
): Promise<T[]> {
  if (getRamIngestMode() !== 'ram' || opts.limit <= 0) return stripInternalIds(rows);
  try {
    const policy = await loadCurrentPublicPrivacyPolicy();
    if (!policy) return [];
    const base = (rows as unknown as RecentRow[]).filter((row) => policy.classify({
      network: String(row['network'] ?? row['_policy_network'] ?? opts.network ?? ''),
      rxNodeId: typeof row['rx_node_id'] === 'string' ? row['rx_node_id'] : null,
      srcNodeId: typeof row['src_node_id'] === 'string' ? row['src_node_id'] : null,
      pathHashes: Array.isArray(row['path_hashes']) ? row['path_hashes'] as string[] : null,
      pathHashSizeBytes: typeof row['path_hash_size_bytes'] === 'number' ? row['path_hash_size_bytes'] : null,
    }).visibilityOk);
    // A type filter (message history) needs headroom: the newest entries of
    // the stream span all packet types, so sample wider before filtering.
    const sample = opts.packetTypes ? Math.min(2_000, opts.limit * 5) : opts.limit;
    const pending = await readPendingEntries(sample);
    if (pending.length === 0) return stripInternalIds(base as unknown as T[]);
    const rowByHash = new Map<string, RecentRow>();
    for (const row of base) {
      const hash = String(row['packet_hash'] ?? '').toLowerCase();
      if (hash && !rowByHash.has(hash)) rowByHash.set(hash, row);
    }

    const groups = new Map<string, EntryWithTx[]>();
    const seenPendingIds = new Set<string>();
    for (const { entry } of pending) {
      if (seenPendingIds.has(entry.observationId)) continue;
      seenPendingIds.add(entry.observationId);
      if (opts.packetTypes && !opts.packetTypes.includes(entry.input.packetType ?? -1)) continue;
      const input = entry.input;
      const fresh = policy.classify({
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
      if (!hash) continue;
      const group = groups.get(hash) ?? [];
      group.push({ entry, isTx: entryIsTx(entry) });
      groups.set(hash, group);
    }

    const merged: RecentRow[] = [];
    for (const [hash, group] of groups) {
      const dbRow = rowByHash.get(hash);
      if (dbRow) {
        // The database already carries a row (possibly with its own observer
        // set): augment it with genuinely new pending observations only.
        augmentRow(dbRow, group);
        continue;
      }
      merged.push(buildMergedRow(group, opts.includePayload === true));
    }
    if (merged.length === 0) return stripInternalIds(base as unknown as T[]);

    const combined = [...base, ...merged];
    combined.sort((left, right) => timeMs(right['time']) - timeMs(left['time']));
    return stripInternalIds(combined.slice(0, opts.limit) as unknown as T[]);
  } catch (error) {
    console.error('[ram] live merge failed (returning database rows only)', (error as Error).message);
    return [];
  }
}
