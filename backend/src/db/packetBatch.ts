import type { QueryResultRow } from 'pg';

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
           node_id, iata, observer_iata, last_seen, last_rx_at, is_online, network
         )
         SELECT DISTINCT ON (rx_node_id)
           rx_node_id, iata, iata, time, time, TRUE, network
         FROM classified
         WHERE rx_node_id IS NOT NULL AND rx_node_id <> ''
         ORDER BY rx_node_id, time DESC
         ON CONFLICT (node_id) DO UPDATE SET
           last_rx_at = GREATEST(nodes.last_rx_at, EXCLUDED.last_rx_at),
           observer_iata = COALESCE(EXCLUDED.observer_iata, nodes.observer_iata),
           last_seen = GREATEST(nodes.last_seen, EXCLUDED.last_seen),
           is_online = TRUE
         RETURNING 1
       )
       SELECT row_id, is_private, path_is_valid AND NOT is_private AS visibility_ok,
              (SELECT COUNT(*) FROM inserted) AS inserted_count,
              (SELECT COUNT(*) FROM observer_updates) AS observer_update_count
       FROM classified
       ORDER BY row_id`,
      params,
    );

    if (result.rows.length !== batch.length) throw new Error('PACKET_BATCH_RESULT_MISMATCH');
    for (const row of result.rows) {
      batch[row.row_id]?.resolve({
        isPrivate: Boolean(row.is_private),
        visibilityOk: Boolean(row.visibility_ok),
      });
    }
  } catch (error) {
    for (const item of batch) item.reject(error);
    throw error;
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
