import type { QueryResultRow } from 'pg';
import {
  packetBatchDuration,
  packetBatchFlushTotal,
  packetBatchRetryTotal,
  packetBatchSize,
  privacyFilterTotal,
} from '../metrics.js';
import {
  PrivatePrefixCache,
  type PrivatePrefixRow,
} from '../privacy/privatePrefixCache.js';

const MAX_BATCH_SIZE = 50;
const FLUSH_INTERVAL_MS = 50;
const MAX_WRITE_ATTEMPTS = 7;
const RETRY_DELAYS_MS = [100, 250, 500, 1_000, 2_000, 4_000] as const;

const RETRYABLE_DATABASE_CODES = new Set([
  '08000', // connection_exception
  '08001', // sqlclient_unable_to_establish_sqlconnection
  '08003', // connection_does_not_exist
  '08004', // sqlserver_rejected_establishment_of_sqlconnection
  '08006', // connection_failure
  '08007', // transaction_resolution_unknown
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '53300', // too_many_connections
]);

export type PacketBatchInput = {
  time: Date;
  packetHash: string;
  rxNodeId: string | null;
  srcNodeId: string | null;
  topic: string;
  topicPrefix: string;
  iata: string | null;
  packetType: number | null;
  routeType: number | null;
  hopCount: number | null;
  rssi: number | null;
  snr: number | null;
  payloadJson: string | null;
  companionSender: string | null;
  rawHex: string;
  advertCount: number | null;
  pathHashes: string[] | null;
  pathHashSizeBytes: number | null;
  network: string;
  transportCodes: string | null;
  regionScope: string | null;
};

export type PacketBatchResult = {
  isPrivate: boolean;
  visibilityOk: boolean;
};

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type PendingPacket = {
  packet: PacketBatchInput;
  resolve: (result: PacketBatchResult) => void;
  reject: (error: unknown) => void;
};

let queryFn: QueryFn | null = null;
let timer: NodeJS.Timeout | null = null;
let activeFlush: Promise<void> | null = null;
let draining = false;
const pending: PendingPacket[] = [];
let privatePrefixCache = new PrivatePrefixCache();
let privatePrefixRefresh: Promise<void> | null = null;

export function configurePacketBatch(query: QueryFn): void {
  queryFn = query;
  privatePrefixCache = new PrivatePrefixCache();
  privatePrefixRefresh = null;
}

function refreshPrivatePrefixCache(query: QueryFn): Promise<void> {
  if (privatePrefixRefresh) return privatePrefixRefresh;
  const refresh = query<{
    generation: string;
    prefixes: PrivatePrefixRow[];
  }>(
    `SELECT visibility.generation::text AS generation,
            COALESCE(
              jsonb_agg(jsonb_build_object(
                'node_id', prefix.node_id,
                'network', prefix.network,
                'prefix_size_bytes', prefix.prefix_size_bytes,
                'prefix', prefix.prefix
              ) ORDER BY prefix.network, prefix.node_id, prefix.prefix_size_bytes)
                FILTER (WHERE prefix.node_id IS NOT NULL),
              '[]'::jsonb
            ) AS prefixes
       FROM public_visibility_state visibility
       LEFT JOIN private_node_prefixes prefix ON TRUE
      WHERE visibility.singleton = TRUE
      GROUP BY visibility.generation`,
  ).then(({ rows }) => {
    const row = rows[0];
    const generation = Number(row?.generation);
    if (!row || !Number.isSafeInteger(generation) || generation < 1) {
      throw new Error('PRIVATE_PREFIX_CACHE_STATE_UNAVAILABLE');
    }
    privatePrefixCache.replace(generation, row.prefixes);
  }).finally(() => {
    if (privatePrefixRefresh === refresh) privatePrefixRefresh = null;
  });
  privatePrefixRefresh = refresh;
  return refresh;
}

function scheduleFlush(): void {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    void flush().catch((error) => {
      console.error('[db] packet batch flush failed', (error as Error).message);
    });
  }, FLUSH_INTERVAL_MS);
  timer.unref();
}

function isRetryableDatabaseError(error: unknown): boolean {
  if (typeof error === 'object' && error !== null && 'code' in error) {
    const code = (error as { code?: unknown }).code;
    if (typeof code === 'string' && RETRYABLE_DATABASE_CODES.has(code)) return true;
  }
  const message = error instanceof Error ? error.message : String(error);
  return /connection terminated|database system is not yet accepting connections|server closed the connection unexpectedly|connection reset|connection refused|could not connect/i.test(message);
}

function retryDelay(attempt: number): number {
  return RETRY_DELAYS_MS[Math.min(attempt, RETRY_DELAYS_MS.length - 1)] ?? 4_000;
}

export function enqueuePacket(packet: PacketBatchInput): Promise<PacketBatchResult> {
  if (draining) return Promise.reject(new Error('PACKET_BATCH_DRAINING'));
  if (!queryFn) return Promise.reject(new Error('PACKET_BATCH_NOT_CONFIGURED'));
  const result = new Promise<PacketBatchResult>((resolve, reject) => {
    pending.push({ packet, resolve, reject });
  });
  if (pending.length >= MAX_BATCH_SIZE) void flush();
  else scheduleFlush();
  return result;
}

async function writeBatch(batch: PendingPacket[], idempotent: boolean): Promise<void> {
  const query = queryFn;
  if (!query) throw new Error('PACKET_BATCH_NOT_CONFIGURED');

  if (privatePrefixCache.currentGeneration < 1) {
    try {
      await refreshPrivatePrefixCache(query);
    } catch (error) {
      console.error('[db] private prefix cache refresh failed', (error as Error).message);
    }
  }

  const columnsPerRow = 25;
  const casts = [
    'int', 'timestamptz', 'text', 'text', 'text', 'text', 'text', 'text',
    'int', 'int', 'int', 'double precision', 'double precision', 'text', 'text',
    'text', 'int', 'text[]', 'int', 'text', 'text', 'text', 'bigint', 'boolean', 'boolean',
  ];
  const params: unknown[] = [];
  const values = batch.map(({ packet }, rowIndex) => {
    const offset = rowIndex * columnsPerRow;
    const privacy = privatePrefixCache.classify({
      network: packet.network,
      rxNodeId: packet.rxNodeId,
      srcNodeId: packet.srcNodeId,
      pathHashes: packet.pathHashes,
      pathHashSizeBytes: packet.pathHashSizeBytes,
    });
    params.push(
      rowIndex,
      packet.time,
      packet.packetHash,
      packet.rxNodeId,
      packet.srcNodeId,
      packet.topic,
      packet.topicPrefix,
      packet.iata,
      packet.packetType,
      packet.routeType,
      packet.hopCount,
      packet.rssi,
      packet.snr,
      packet.payloadJson,
      packet.companionSender,
      packet.rawHex,
      packet.advertCount,
      packet.pathHashes,
      packet.pathHashSizeBytes,
      packet.network,
      packet.transportCodes,
      packet.regionScope,
      privacy.generation,
      privacy.isPrivate,
      privacy.pathIsValid,
    );
    const placeholders = Array.from(
      { length: columnsPerRow },
      (_, index) => `$${offset + index + 1}::${casts[index]}`,
    );
    return `(${placeholders.join(', ')})`;
  }).join(',\n');

  const persistenceSource = idempotent ? 'new_rows' : 'classified';
  const idempotencyCte = idempotent
    ? `new_rows AS MATERIALIZED (
         SELECT c.*
         FROM classified c
         WHERE NOT EXISTS (
           SELECT 1
           FROM packets existing
           WHERE existing.time = c.time
             AND existing.packet_hash = c.packet_hash
             AND existing.rx_node_id IS NOT DISTINCT FROM c.rx_node_id
             AND existing.topic = c.topic
             AND existing.network = c.network
         )
       ),`
    : '';

  const result = await query<{
    row_id: number;
    is_private: boolean;
    visibility_ok: boolean;
    prefix_cache_fresh: boolean;
  }>(
      `WITH current_visibility AS MATERIALIZED (
         SELECT generation
           FROM public_visibility_state
          WHERE singleton = TRUE
          FOR KEY SHARE
       ),
       incoming (
         row_id, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix,
         iata, packet_type, route_type, hop_count, rssi, snr, payload, companion_sender, raw_hex,
         advert_count, path_hashes, path_hash_size_bytes, network, transport_codes, region_scope,
         prefix_cache_generation, classified_private, path_is_valid
       ) AS (VALUES ${values}),
       classified AS MATERIALIZED (
         SELECT i.*,
           CASE
             WHEN i.prefix_cache_generation = visibility.generation THEN i.classified_private
             ELSE TRUE
           END AS is_private,
           i.prefix_cache_generation = visibility.generation AS prefix_cache_fresh
         FROM incoming i
         CROSS JOIN current_visibility visibility
       ),
       ${idempotencyCte}
       inserted AS (
         INSERT INTO packets (
           observation_id, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, iata,
           packet_type, route_type, hop_count, rssi, snr, payload, companion_sender, raw_hex,
           advert_count, path_hashes, path_hash_size_bytes, network, transport_codes,
           region_scope, is_private, visibility_ok
         )
         SELECT gen_random_uuid(), time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, iata,
                packet_type, route_type, hop_count, rssi, snr, payload::jsonb, companion_sender, raw_hex,
                advert_count, path_hashes, path_hash_size_bytes, network, transport_codes,
                region_scope, is_private, path_is_valid AND NOT is_private
         FROM ${persistenceSource}
         RETURNING 1
       ),
       observer_updates AS (
         INSERT INTO nodes (
           node_id, iata, observer_iata, last_seen, last_rx_at, is_online,
           network, last_mqtt_observer_seen_at
         )
         SELECT DISTINCT ON (rx_node_id)
           rx_node_id, iata, iata, time, time, TRUE, network, time
         FROM ${persistenceSource}
         WHERE rx_node_id IS NOT NULL AND rx_node_id <> ''
         ORDER BY rx_node_id, time DESC
         ON CONFLICT (node_id) DO UPDATE SET
           last_rx_at = GREATEST(nodes.last_rx_at, EXCLUDED.last_rx_at),
           iata = COALESCE(EXCLUDED.iata, nodes.iata),
           observer_iata = COALESCE(EXCLUDED.observer_iata, nodes.observer_iata),
           last_seen = GREATEST(nodes.last_seen, EXCLUDED.last_seen),
           network = CASE
             WHEN EXCLUDED.network = 'test' THEN 'test'
             WHEN EXCLUDED.network IN ('ukmesh', 'teesside') THEN EXCLUDED.network
             ELSE EXCLUDED.network
           END,
           last_mqtt_observer_seen_at = GREATEST(
             nodes.last_mqtt_observer_seen_at,
             EXCLUDED.last_mqtt_observer_seen_at
           ),
           is_online = TRUE
         RETURNING 1
       ),
       network_sightings AS (
         INSERT INTO node_network_sightings
           (node_id, network, first_seen_at, last_seen_at)
         SELECT src_node_id, network, MIN(time), MAX(time)
         FROM ${persistenceSource}
         WHERE src_node_id IS NOT NULL
           AND src_node_id <> ''
           AND network <> 'test'
         GROUP BY src_node_id, network
         ON CONFLICT (node_id, network) DO UPDATE SET
           first_seen_at = LEAST(
             node_network_sightings.first_seen_at,
             EXCLUDED.first_seen_at
           ),
           last_seen_at = GREATEST(
             node_network_sightings.last_seen_at,
             EXCLUDED.last_seen_at
           )
         RETURNING 1
       ),
       daily_candidates AS (
         SELECT DISTINCT ON (network)
           network,
           time::date AS day,
           hop_count,
           packet_hash,
           time
         FROM ${persistenceSource}
         WHERE path_is_valid
           AND NOT is_private
           AND hop_count IS NOT NULL
         ORDER BY network, hop_count DESC, row_id ASC
       ),
       daily_updates AS (
         INSERT INTO packet_daily_stats
           (network, day, max_hop_count, max_hop_hash, max_hop_seen_at, updated_at)
         SELECT network, day, hop_count, packet_hash, time, NOW()
         FROM daily_candidates
         ON CONFLICT (network, day) DO UPDATE SET
           max_hop_count = EXCLUDED.max_hop_count,
           max_hop_hash = EXCLUDED.max_hop_hash,
           max_hop_seen_at = EXCLUDED.max_hop_seen_at,
           updated_at = NOW()
         WHERE packet_daily_stats.max_hop_count IS NULL
            OR EXCLUDED.max_hop_count > packet_daily_stats.max_hop_count
         RETURNING 1
       ),
       hourly_updates AS (
         INSERT INTO packet_hourly_stats (
           network, hour, packet_type, hop_count, route_type,
           transport_code, region_scope, packet_count,
           rssi_sum, rssi_count, snr_sum, snr_count, updated_at
         )
         SELECT
           network,
           date_trunc('hour', time),
           COALESCE(packet_type, -1),
           COALESCE(hop_count, -1),
           COALESCE(route_type, -1),
           COALESCE(NULLIF(TRIM(transport_codes), ''), ''),
           COALESCE(NULLIF(TRIM(region_scope), ''), ''),
           COUNT(*)::bigint,
           COALESCE(SUM(rssi), 0)::double precision,
           COUNT(rssi)::bigint,
           COALESCE(SUM(snr), 0)::double precision,
           COUNT(snr)::bigint,
           NOW()
         FROM ${persistenceSource}
         WHERE network = 'test'
            OR topic_prefix <> 'meshcore-test'
         GROUP BY
           network,
           date_trunc('hour', time),
           COALESCE(packet_type, -1),
           COALESCE(hop_count, -1),
           COALESCE(route_type, -1),
           COALESCE(NULLIF(TRIM(transport_codes), ''), ''),
           COALESCE(NULLIF(TRIM(region_scope), ''), '')
         ON CONFLICT (
           network, hour, packet_type, hop_count, route_type,
           transport_code, region_scope
         ) DO UPDATE SET
           packet_count = packet_hourly_stats.packet_count + EXCLUDED.packet_count,
           rssi_sum = packet_hourly_stats.rssi_sum + EXCLUDED.rssi_sum,
           rssi_count = packet_hourly_stats.rssi_count + EXCLUDED.rssi_count,
           snr_sum = packet_hourly_stats.snr_sum + EXCLUDED.snr_sum,
           snr_count = packet_hourly_stats.snr_count + EXCLUDED.snr_count,
           updated_at = NOW()
         RETURNING 1
       ),
       packet_region_updates AS (
         INSERT INTO observer_region_packet_sightings
           (network, iata, packet_hash, first_seen, last_seen)
         SELECT network, iata, packet_hash, MIN(time), MAX(time)
         FROM ${persistenceSource}
         WHERE network <> 'test'
           AND path_is_valid
           AND NOT is_private
           AND iata IS NOT NULL
           AND iata <> ''
           AND packet_hash <> ''
           AND rx_node_id ~ '^[0-9A-Fa-f]{64}$'
         GROUP BY network, iata, packet_hash
         ON CONFLICT (network, iata, packet_hash) DO UPDATE SET
           first_seen = LEAST(
             observer_region_packet_sightings.first_seen,
             EXCLUDED.first_seen
           ),
           last_seen = GREATEST(
             observer_region_packet_sightings.last_seen,
             EXCLUDED.last_seen
           )
         RETURNING 1
       ),
       observer_region_updates AS (
         INSERT INTO observer_region_observer_sightings
           (network, iata, rx_node_id, first_seen, last_seen)
         SELECT network, iata, rx_node_id, MIN(time), MAX(time)
         FROM ${persistenceSource}
         WHERE network <> 'test'
           AND path_is_valid
           AND NOT is_private
           AND iata IS NOT NULL
           AND iata <> ''
           AND rx_node_id ~ '^[0-9A-Fa-f]{64}$'
         GROUP BY network, iata, rx_node_id
         ON CONFLICT (network, iata, rx_node_id) DO UPDATE SET
           first_seen = LEAST(
             observer_region_observer_sightings.first_seen,
             EXCLUDED.first_seen
           ),
           last_seen = GREATEST(
             observer_region_observer_sightings.last_seen,
             EXCLUDED.last_seen
           )
         RETURNING 1
       )
       SELECT row_id, is_private, path_is_valid AND NOT is_private AS visibility_ok,
              prefix_cache_fresh,
              (SELECT COUNT(*) FROM inserted) AS inserted_count,
              (SELECT COUNT(*) FROM observer_updates) AS observer_update_count,
              (SELECT COUNT(*) FROM network_sightings) AS network_sighting_count,
              (SELECT COUNT(*) FROM daily_updates) AS daily_update_count,
              (SELECT COUNT(*) FROM hourly_updates) AS hourly_update_count,
              (SELECT COUNT(*) FROM packet_region_updates) AS packet_region_update_count,
              (SELECT COUNT(*) FROM observer_region_updates) AS observer_region_update_count
       FROM classified
       ORDER BY row_id`,
      params,
    );

  if (result.rows.length !== batch.length) throw new Error('PACKET_BATCH_RESULT_MISMATCH');
  if (result.rows.some((row) => !row.prefix_cache_fresh)) {
    void refreshPrivatePrefixCache(query).catch((error: unknown) => {
      console.error('[db] private prefix cache refresh failed', (error as Error).message);
    });
  }
  for (const row of result.rows) {
    privacyFilterTotal.inc({
      operation: 'packet_ingest',
      outcome: row.visibility_ok ? 'public' : (row.is_private ? 'private' : 'invalid'),
    });
    batch[row.row_id]?.resolve({
      isPrivate: Boolean(row.is_private),
      visibilityOk: Boolean(row.visibility_ok),
    });
  }
}

async function writeBatchWithRetry(batch: PendingPacket[]): Promise<void> {
  const startedAt = process.hrtime.bigint();
  packetBatchSize.observe(batch.length);
  let lastError: unknown;

  for (let attempt = 0; attempt < MAX_WRITE_ATTEMPTS; attempt += 1) {
    try {
      await writeBatch(batch, attempt > 0);
      packetBatchFlushTotal.inc({ outcome: 'success' });
      packetBatchDuration.observe(
        { outcome: 'success' },
        Number(process.hrtime.bigint() - startedAt) / 1e9,
      );
      return;
    } catch (error) {
      lastError = error;
      const retriesRemaining = attempt < MAX_WRITE_ATTEMPTS - 1;
      if (!retriesRemaining || !isRetryableDatabaseError(error)) break;
      packetBatchRetryTotal.inc();
      await new Promise((resolve) => setTimeout(resolve, retryDelay(attempt)));
    }
  }

  for (const item of batch) item.reject(lastError);
  packetBatchFlushTotal.inc({ outcome: 'failure' });
  packetBatchDuration.observe(
    { outcome: 'failure' },
    Number(process.hrtime.bigint() - startedAt) / 1e9,
  );
  throw lastError;
}

export function flush(): Promise<void> {
  if (timer) {
    clearTimeout(timer);
    timer = null;
  }
  if (activeFlush) return activeFlush;

  activeFlush = (async () => {
    while (pending.length > 0) {
      const batch = pending.splice(0, MAX_BATCH_SIZE);
      await writeBatchWithRetry(batch);
    }
  })().finally(() => {
    activeFlush = null;
    if (pending.length > 0) scheduleFlush();
  });
  return activeFlush;
}

export async function closePacketBatch(): Promise<void> {
  draining = true;
  await flush();
}
