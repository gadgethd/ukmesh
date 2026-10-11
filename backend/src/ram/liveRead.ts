/**
 * RAM-first MQTT ingestion — merged read helper.
 *
 * The live read paths (recent packets, recent messages, WS initial state)
 * keep reading PostgreSQL. In RAM mode the batch flush window (up to
 * MQTT_RAM_MAX_AGE_MS) would hide recently admitted packets from those
 * queries, so readRecentWithPending captures stream overlap before starting
 * SQL, then this helper appends those entries — mapped to the
 * same row shape the SQL queries return — on top of the database result.
 *
 * Correctness notes:
 *  - both PG rows and stream entries are classified against a fresh public
 *    policy/generation snapshot; unavailable authority fails closed;
 *  - pending observations are aggregated per packet hash, mirroring the SQL's
 *    observer aggregation: multiple observers of one RF packet contribute a
 *    single row with combined observer arrays and RX/TX counts;
 *  - when the hash is already present in the database result, stable
 *    observation IDs distinguish retained overlap from new sightings;
 *  - Redis failure omits the pending overlay, while the authorized PG rows
 *    remain available.
 */
import { readPendingEntries, type PendingStreamEntry } from './liveStore.js';
import { loadCurrentPublicPrivacyPolicy } from '../db/packetBatch.js';
import { query } from '../db/index.js';
import { networkMatchesScope } from '../networks.js';
import { getRamIngestMode, type RamPendingEntry } from './protocol.js';

export type RecentRow = Record<string, unknown>;

export type MergeOptions = {
  limit: number;
  network?: string;
  observer?: string;
  includePayload?: boolean;
  fields?: 'full' | 'slim';
  /** The SQL recent-packets window is five minutes; message history is 24 hours. */
  windowMs?: number;
  nowMs?: number;
  /** Supplied by tests; production resolves stored node aliases in one PG query. */
  resolveAliases?: (nodeIds: string[]) => Promise<Map<string, string>>;
  /** Restrict pending contributions to these packet types (e.g. [5] for message history). */
  packetTypes?: number[];
  /** Capture before starting the SQL read to survive overlap expiry. */
  pendingSnapshot?: PendingStreamEntry[];
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

async function resolveCanonicalAliases(nodeIds: string[]): Promise<Map<string, string>> {
  if (nodeIds.length === 0) return new Map();
  const result = await query<{ source_node_id: string; canonical_node_id: string }>(`
    SELECT source_node_id, canonical_node_id
    FROM node_identity_aliases
    WHERE source_node_id = ANY($1::text[])
       OR canonical_node_id = ANY($1::text[])
       OR canonical_node_id IN (
         SELECT canonical_node_id FROM node_identity_aliases
         WHERE source_node_id = ANY($1::text[])
       )`, [nodeIds]);
  return new Map(result.rows.map((row) => [row.source_node_id, row.canonical_node_id]));
}

function canonical(value: string | null, aliases: Map<string, string>): string | null {
  return value ? aliases.get(value) ?? value : null;
}

function representativeRank(item: EntryWithTx): number {
  const { entry } = item;
  const { payload } = entryPayload(entry);
  return (payload && Object.hasOwn(payload, 'appData') ? 16 : 0)
    + (upper(entry.input.srcNodeId) ? 8 : 0)
    + (entry.input.advertCount !== null && entry.input.advertCount !== undefined ? 4 : 0)
    + (entry.input.packetType === 4 ? 2 : 0);
}

/** Aggregate pending observations into a single row (mirrors the SQL observer aggregation). */
function buildMergedRow(group: EntryWithTx[], includePayload: boolean, fields: 'full' | 'slim', aliases: Map<string, string>): RecentRow {
  const newestFirst = [...group].sort((a, b) => representativeRank(b) - representativeRank(a)
    || timeMs(b.entry.input.time) - timeMs(a.entry.input.time));
  const representative = newestFirst[0]!;
  const observers = new Set<string>();
  const iatas = new Set<string>();
  let rxCount = 0;
  let txCount = 0;
  for (const { entry, isTx } of group) {
    const rx = canonical(upper(entry.input.rxNodeId), aliases);
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
    rx_node_id: canonical(upper(input.rxNodeId), aliases),
    src_node_id: canonical(upper(input.srcNodeId), aliases),
    packet_type: input.packetType,
    hop_count: input.hopCount,
    rssi: input.rssi,
    snr: input.snr,
    summary,
    advert_count: input.advertCount,
    path_hashes: input.pathHashes,
    path_hash_size_bytes: input.pathHashSizeBytes,
    ...(fields === 'full' ? {
      topic: input.topic, topic_prefix: input.topicPrefix, iata: input.iata,
      route_type: input.routeType, network: input.network,
      transport_codes: input.transportCodes, region_scope: input.regionScope,
    } : {}),
    observer_node_ids: [...observers],
    observer_iatas: [...iatas],
    rx_count: rxCount,
    tx_count: txCount,
    ...(includePayload ? { payload } : {}),
  };
}

/** Add genuinely new pending observations to an existing database row. */
function augmentRow(row: RecentRow, group: EntryWithTx[], aliases: Map<string, string>): void {
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
    const rx = canonical(upper(entry.input.rxNodeId), aliases);
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

/**
 * Capture RAM first, then start SQL. An observation deleted before this
 * capture has already committed and is visible to the later SQL snapshot;
 * an observation deleted during SQL remains in this bounded capture. This
 * ordering makes the handoff independent of SQL latency/Redis retention.
 * Permission is checked after SQL, never frozen with the captured payload.
 */
export async function readRecentWithPending<T>(
  loadRows: () => Promise<T[]>,
  opts: MergeOptions,
): Promise<T[]> {
  const pendingSnapshot = getRamIngestMode() === 'ram'
    ? await readPendingEntries(opts.limit) : [];
  return mergePendingPacketsIntoRecentRows(await loadRows(), { ...opts, pendingSnapshot });
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
    // The store scans the bounded stream, then this layer filters and pages
    // distinct hashes. Sampling only the newest N observations would hide a
    // matching hash behind a large observer fanout or other packet types.
    const pending = opts.pendingSnapshot ?? await readPendingEntries(opts.limit);
    if (pending.length === 0) return stripInternalIds(base as unknown as T[]);
    const cutoff = (opts.nowMs ?? Date.now()) - (opts.windowMs ?? (opts.packetTypes ? 86_400_000 : 300_000));
    const eligible = pending.filter(({ entry }) => {
      const input = entry.input;
      if (opts.packetTypes && !opts.packetTypes.includes(input.packetType ?? -1)) return false;
      if (timeMs(input.time) <= cutoff) return false;
      if (opts.network ? !networkMatchesScope(input.network, opts.network)
        : input.network === 'test') return false;
      if (input.network !== 'test' && input.topicPrefix === 'meshcore-test') return false;
      return policy.classify({
        network: input.network,
        rxNodeId: input.rxNodeId,
        srcNodeId: input.srcNodeId,
        pathHashes: input.pathHashes,
        pathHashSizeBytes: input.pathHashSizeBytes,
      }).visibilityOk;
    });
    if (eligible.length === 0) return stripInternalIds(base as unknown as T[]);
    const nodeIds = new Set<string>();
    if (opts.observer) nodeIds.add(opts.observer.toUpperCase());
    for (const { entry } of eligible) {
      const rx = upper(entry.input.rxNodeId);
      const src = upper(entry.input.srcNodeId);
      if (rx) nodeIds.add(rx);
      if (src) nodeIds.add(src);
    }
    const aliases = await (opts.resolveAliases ?? resolveCanonicalAliases)([...nodeIds]);
    const requestedObserver = canonical(upper(opts.observer), aliases);
    const rowByHash = new Map<string, RecentRow>();
    for (const row of base) {
      const hash = String(row['packet_hash'] ?? '').toLowerCase();
      if (hash && !rowByHash.has(hash)) rowByHash.set(hash, row);
    }

    const groups = new Map<string, EntryWithTx[]>();
    const seenPendingIds = new Set<string>();
    for (const { entry } of eligible) {
      if (seenPendingIds.has(entry.observationId)) continue;
      seenPendingIds.add(entry.observationId);
      const input = entry.input;
      if (requestedObserver && canonical(upper(input.rxNodeId), aliases) !== requestedObserver) continue;
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
        augmentRow(dbRow, group, aliases);
        continue;
      }
      merged.push(buildMergedRow(group, opts.includePayload === true, opts.fields ?? 'full', aliases));
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
