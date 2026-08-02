import type { QueryResultRow } from 'pg';
import {
  packetBatchDuration,
  packetBatchFlushTotal,
  packetBatchSize,
  privacyFilterTotal,
} from '../metrics.js';

const MAX_BATCH_SIZE = 50;
const FLUSH_INTERVAL_MS = 50;

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

export function configurePacketBatch(query: QueryFn): void {
  queryFn = query;
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

async function writeBatch(batch: PendingPacket[]): Promise<void> {
  const query = queryFn;
  if (!query) throw new Error('PACKET_BATCH_NOT_CONFIGURED');

  const columnsPerRow = 22;
  const casts = [
    'int', 'timestamptz', 'text', 'text', 'text', 'text', 'text', 'text',
    'int', 'int', 'int', 'double precision', 'double precision', 'text', 'text',
    'text', 'int', 'text[]', 'int', 'text', 'text', 'text',
  ];
  const params: unknown[] = [];
  const values = batch.map(({ packet }, rowIndex) => {
    const offset = rowIndex * columnsPerRow;
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
    );
    const placeholders = Array.from(
      { length: columnsPerRow },
      (_, index) => `$${offset + index + 1}::${casts[index]}`,
    );
    return `(${placeholders.join(', ')})`;
  }).join(',\n');

  const startedAt = process.hrtime.bigint();
  let outcome = 'success';
  packetBatchSize.observe(batch.length);
  try {
    const result = await query<{ row_id: number; is_private: boolean; visibility_ok: boolean }>(
      `WITH incoming (
         row_id, time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix,
         iata, packet_type, route_type, hop_count, rssi, snr, payload, companion_sender, raw_hex,
         advert_count, path_hashes, path_hash_size_bytes, network, transport_codes, region_scope
       ) AS (VALUES ${values}),
       classified AS MATERIALIZED (
         SELECT i.*,
           EXISTS (
             SELECT 1
             FROM private_node_prefixes pp
             WHERE (
                 pp.network = i.network
                 OR (
                   pp.network IN ('ukmesh', 'northeast', 'teesside')
                   AND i.network IN ('ukmesh', 'northeast', 'teesside')
                 )
               )
               AND (
                 pp.node_id IN (i.rx_node_id, i.src_node_id)
                 OR (
                   i.path_hash_size_bytes = pp.prefix_size_bytes
                   AND EXISTS (
                     SELECT 1
                     FROM unnest(COALESCE(i.path_hashes, ARRAY[]::text[])) AS packet_prefix
                     WHERE UPPER(packet_prefix) = pp.prefix
                   )
                 )
               )
           ) AS is_private,
           (
             (COALESCE(cardinality(i.path_hashes), 0) = 0 OR i.path_hash_size_bytes BETWEEN 1 AND 3)
             AND NOT EXISTS (
               SELECT 1
               FROM unnest(COALESCE(i.path_hashes, ARRAY[]::text[])) AS path_hash
               WHERE path_hash IS NULL
                  OR length(path_hash) <> i.path_hash_size_bytes * 2
                  OR path_hash !~ '^[0-9A-Fa-f]+$'
             )
           ) AS path_is_valid
         FROM incoming i
       ),
       inserted AS (
         INSERT INTO packets (
           time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, iata,
           packet_type, route_type, hop_count, rssi, snr, payload, companion_sender, raw_hex,
           advert_count, path_hashes, path_hash_size_bytes, network, transport_codes,
           region_scope, is_private, visibility_ok
         )
         SELECT time, packet_hash, rx_node_id, src_node_id, topic, topic_prefix, iata,
                packet_type, route_type, hop_count, rssi, snr, payload::jsonb, companion_sender, raw_hex,
                advert_count, path_hashes, path_hash_size_bytes, network, transport_codes,
                region_scope, is_private, path_is_valid AND NOT is_private
         FROM classified
         RETURNING 1
       ),
       observer_updates AS (
         INSERT INTO nodes (
           node_id, iata, observer_iata, last_seen, last_rx_at, is_online,
           network, last_mqtt_observer_seen_at
         )
         SELECT DISTINCT ON (rx_node_id)
           rx_node_id, iata, iata, time, time, TRUE, network, time
         FROM classified
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
         FROM classified
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
         FROM classified
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
         FROM classified
         WHERE path_is_valid
           AND NOT is_private
           AND (network = 'test' OR topic_prefix <> 'meshcore-test')
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
         FROM classified
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
         FROM classified
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
  } catch (error) {
    outcome = 'failure';
    for (const item of batch) item.reject(error);
    throw error;
  } finally {
    packetBatchFlushTotal.inc({ outcome });
    packetBatchDuration.observe(
      { outcome },
      Number(process.hrtime.bigint() - startedAt) / 1e9,
    );
  }
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
      await writeBatch(batch);
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
