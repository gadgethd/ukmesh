import { pool, ingestQuery, withIngestTransaction } from '../db/runtime.js';
import { UKMESH_NETWORK_SQL_LIST } from '../networks.js';

const COORDINATE_RECALC_THRESHOLD_M = Number(process.env['NODE_COORDINATE_RECALC_THRESHOLD_M'] ?? 25);

function validCoordinatePair(lat?: number | null, lon?: number | null): boolean {
  return (
    typeof lat === 'number'
    && typeof lon === 'number'
    && Number.isFinite(lat)
    && Number.isFinite(lon)
    && !(Math.abs(lat) < 1e-9 && Math.abs(lon) < 1e-9)
  );
}

function distanceMeters(aLat: number, aLon: number, bLat: number, bLon: number): number {
  const midLat = ((aLat + bLat) / 2) * Math.PI / 180;
  const dLat = (bLat - aLat) * 111_320;
  const dLon = (bLon - aLon) * 111_320 * Math.cos(midLat);
  return Math.sqrt(dLat ** 2 + dLon ** 2);
}

export async function touchNodesPredictedOnline(nodeIds: string[]): Promise<void> {
  const ids = Array.from(new Set(nodeIds.map((id) => String(id).trim()).filter(Boolean)));
  if (ids.length < 1) return;
  await pool.query(
    `UPDATE core.nodes
     SET last_predicted_online_at = NOW()
     WHERE node_id = ANY($1::text[])`,
    [ids],
  );
}

export async function upsertNode(nodeId: string, updates: {
  name?: string;
  lat?: number;
  lon?: number;
  iata?: string;
  role?: number;
  hardwareModel?: string;
  firmwareVersion?: string;
  publicKey?: string;
  network?: string;
  allowTestOverride?: boolean;
  mqttObserver?: boolean;
  advertHash?: string;
}): Promise<{ coordinatesChanged: boolean; advertCount: number }> {
  const incomingLat = typeof updates.lat === 'number' && updates.lat !== 0 ? updates.lat : null;
  const incomingLon = typeof updates.lon === 'number' && updates.lon !== 0 ? updates.lon : null;
  const hasIncomingPosition = validCoordinatePair(incomingLat, incomingLon);

  // Status and every received packet touch the observer without coordinates.
  // PostgreSQL already makes this UPSERT atomic, so avoid a needless client
  // checkout plus BEGIN/COMMIT round trip on the ingest hot path. Coordinate
  // updates retain the transaction below because they also compare old values
  // and invalidate dependent coverage atomically.
  if (!hasIncomingPosition) {
    const result = await ingestQuery<{ advert_count: number }>(
      `WITH advert_once AS (
         INSERT INTO core.node_counted_adverts (canonical_advert_hash, node_id, counted_at)
         SELECT $13, $1, NOW()
         WHERE $13::text IS NOT NULL
         ON CONFLICT (canonical_advert_hash) DO NOTHING
         RETURNING 1
       )
       INSERT INTO core.nodes (node_id, name, lat, lon, iata, role, hardware_model, firmware_version, public_key, last_seen, is_online, network, last_mqtt_observer_seen_at, advert_count)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), TRUE, $10, CASE WHEN $12 THEN NOW() ELSE NULL END, (SELECT COUNT(*) FROM advert_once))
       ON CONFLICT (node_id) DO UPDATE SET
         name             = COALESCE(EXCLUDED.name, nodes.name),
         lat              = COALESCE(NULLIF(EXCLUDED.lat, 0), nodes.lat),
         lon              = COALESCE(NULLIF(EXCLUDED.lon, 0), nodes.lon),
         iata             = COALESCE(EXCLUDED.iata, nodes.iata),
         role             = COALESCE(EXCLUDED.role, nodes.role),
         hardware_model   = COALESCE(EXCLUDED.hardware_model, nodes.hardware_model),
         firmware_version = COALESCE(EXCLUDED.firmware_version, nodes.firmware_version),
         public_key       = COALESCE(EXCLUDED.public_key, nodes.public_key),
         network          = CASE
                              WHEN EXCLUDED.network IS NULL THEN nodes.network
                              WHEN EXCLUDED.network = 'test' AND $11 THEN 'test'
                              WHEN EXCLUDED.network = 'test' AND nodes.network IN (${UKMESH_NETWORK_SQL_LIST}) THEN nodes.network
                              WHEN EXCLUDED.network IN ('ukmesh', 'teesside') THEN EXCLUDED.network
                              ELSE EXCLUDED.network
                            END,
         last_seen        = NOW(),
         last_mqtt_observer_seen_at = CASE
                                        WHEN $12 THEN NOW()
                                        ELSE nodes.last_mqtt_observer_seen_at
                                      END,
         advert_count     = nodes.advert_count + (SELECT COUNT(*) FROM advert_once),
         is_online        = TRUE
       RETURNING advert_count`,
      [nodeId, updates.name, updates.lat, updates.lon, updates.iata, updates.role,
       updates.hardwareModel, updates.firmwareVersion, updates.publicKey, updates.network ?? null,
       Boolean(updates.allowTestOverride), Boolean(updates.mqttObserver), updates.advertHash ?? null],
    );
    return { coordinatesChanged: false, advertCount: Number(result.rows[0]?.advert_count ?? 0) };
  }

  return withIngestTransaction(async (transactionQuery) => {
    const existing = await transactionQuery<{ lat: number | null; lon: number | null }>(
      'SELECT lat, lon FROM core.nodes WHERE node_id = $1 FOR UPDATE',
      [nodeId],
    );

    const upsert = await transactionQuery<{ advert_count: number }>(
      `WITH advert_once AS (
         INSERT INTO core.node_counted_adverts (canonical_advert_hash, node_id, counted_at)
         SELECT $13, $1, NOW()
         WHERE $13::text IS NOT NULL
         ON CONFLICT (canonical_advert_hash) DO NOTHING
         RETURNING 1
       )
       INSERT INTO core.nodes (node_id, name, lat, lon, iata, role, hardware_model, firmware_version, public_key, last_seen, is_online, network, last_mqtt_observer_seen_at, advert_count)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), TRUE, $10, CASE WHEN $12 THEN NOW() ELSE NULL END, (SELECT COUNT(*) FROM advert_once))
       ON CONFLICT (node_id) DO UPDATE SET
         name             = COALESCE(EXCLUDED.name, nodes.name),
         lat              = COALESCE(NULLIF(EXCLUDED.lat, 0), nodes.lat),
         lon              = COALESCE(NULLIF(EXCLUDED.lon, 0), nodes.lon),
         iata             = COALESCE(EXCLUDED.iata, nodes.iata),
         role             = COALESCE(EXCLUDED.role, nodes.role),
         hardware_model   = COALESCE(EXCLUDED.hardware_model, nodes.hardware_model),
         firmware_version = COALESCE(EXCLUDED.firmware_version, nodes.firmware_version),
         public_key       = COALESCE(EXCLUDED.public_key, nodes.public_key),
         network          = CASE
                              WHEN EXCLUDED.network IS NULL THEN nodes.network
                              WHEN EXCLUDED.network = 'test' AND $11 THEN 'test'
                              WHEN EXCLUDED.network = 'test' AND nodes.network IN (${UKMESH_NETWORK_SQL_LIST}) THEN nodes.network
                              WHEN EXCLUDED.network IN ('ukmesh', 'teesside') THEN EXCLUDED.network
                              ELSE EXCLUDED.network
                            END,
         last_seen        = NOW(),
         last_mqtt_observer_seen_at = CASE
                                        WHEN $12 THEN NOW()
                                        ELSE nodes.last_mqtt_observer_seen_at
                                      END,
         advert_count     = nodes.advert_count + (SELECT COUNT(*) FROM advert_once),
         is_online        = TRUE
       RETURNING advert_count`,
      [nodeId, updates.name, updates.lat, updates.lon, updates.iata, updates.role,
       updates.hardwareModel, updates.firmwareVersion, updates.publicKey, updates.network ?? null,
       Boolean(updates.allowTestOverride), Boolean(updates.mqttObserver), updates.advertHash ?? null]
    );

    const row = existing?.rows[0];
    const coordinatesChanged = Boolean(
      hasIncomingPosition
      && row
      && validCoordinatePair(row.lat, row.lon)
      && incomingLat !== null
      && incomingLon !== null
      && row.lat !== null
      && row.lon !== null
      && distanceMeters(row.lat, row.lon, incomingLat, incomingLon) >= COORDINATE_RECALC_THRESHOLD_M
    );

    if (coordinatesChanged) {
      await transactionQuery('UPDATE core.nodes SET elevation_m = NULL WHERE node_id = $1', [nodeId]);
      await transactionQuery('DELETE FROM core.node_coverage WHERE node_id = $1', [nodeId]);
    }

    return {
      coordinatesChanged,
      advertCount: Number(upsert.rows[0]?.advert_count ?? 0),
    };
  });
}

export async function insertNodeStatusSample(sample: {
  observedAt?: string;
  nodeId: string;
  network?: string;
  batteryMv?: number | null;
  uptimeSecs?: number | null;
  txAirSecs?: number | null;
  rxAirSecs?: number | null;
  channelUtilization?: number | null;
  airUtilTx?: number | null;
  stats?: Record<string, unknown> | null;
  status?: string | null;
}): Promise<void> {
  await ingestQuery(
    `WITH sample_insert AS (
       INSERT INTO core.node_status_samples
       (time, node_id, network, battery_mv, uptime_secs, tx_air_secs, rx_air_secs, channel_utilization, air_util_tx, stats, status)
       VALUES (COALESCE($11::timestamptz, NOW()), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       RETURNING time
     )
     UPDATE core.nodes
        SET last_status_at = GREATEST(last_status_at, (SELECT time FROM sample_insert)),
            last_seen = GREATEST(last_seen, (SELECT time FROM sample_insert)),
            is_online = is_online OR (SELECT time FROM sample_insert) > NOW() - INTERVAL '15 minutes'
      WHERE node_id = $1`,
    [
      sample.nodeId,
      sample.network ?? 'ukmesh',
      sample.batteryMv ?? null,
      sample.uptimeSecs ?? null,
      sample.txAirSecs ?? null,
      sample.rxAirSecs ?? null,
      sample.channelUtilization ?? null,
      sample.airUtilTx ?? null,
      sample.stats ? JSON.stringify(sample.stats) : null,
      sample.status ?? null,
      sample.observedAt ?? null,
    ],
  );
}

export async function insertNodeNeighborSample(sample: {
  nodeId: string;
  network?: string;
  neighbors: unknown[];
}): Promise<void> {
  await ingestQuery(
    `INSERT INTO core.node_neighbor_samples (node_id, time, neighbors, network)
     VALUES ($1, NOW(), $2::jsonb, $3)`,
    [sample.nodeId, JSON.stringify(sample.neighbors), sample.network ?? 'ukmesh'],
  );
}
