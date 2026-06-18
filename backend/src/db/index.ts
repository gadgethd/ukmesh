import pg from 'pg';
import fs from 'node:fs';
import { databaseConfig } from '../platform/config/database.js';
import { resolveDbAssetPath } from './assets.js';
import { runMigrations } from './migrations.js';
import { UKMESH_NETWORKS } from '../networks.js';

const { Pool } = pg;
const COORDINATE_RECALC_THRESHOLD_M = Number(process.env['NODE_COORDINATE_RECALC_THRESHOLD_M'] ?? 25);
const MAX_MULTIBYTE_PATH_SEGMENT_KM = 150;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  application_name: databaseConfig.applicationName,
  options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
  max: databaseConfig.poolMax,
  idleTimeoutMillis: databaseConfig.idleTimeoutMs,
  connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
  statement_timeout: databaseConfig.statementTimeoutMs,
  query_timeout: databaseConfig.statementTimeoutMs,
});

pool.on('error', (err) => {
  console.error('[db] unexpected pool error', err.message);
});

// Dedicated pool for long-running analytical queries (e.g. refreshRecentPathEvidence).
// Kept separate so slow analytics can never exhaust the OLTP pool and take down the API.
const analyticsPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  application_name: `${databaseConfig.applicationName}-analytics`,
  options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
  max: 2,
  idleTimeoutMillis: databaseConfig.idleTimeoutMs,
  connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
  statement_timeout: 300_000, // 5 minutes — analytics queries are intentionally slow
  query_timeout: 300_000,
});

analyticsPool.on('error', (err) => {
  console.error('[db] unexpected analytics pool error', err.message);
});

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

export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[]
): Promise<pg.QueryResult<T>> {
  return pool.query<T>(text, params);
}

type ScopePlaceholders = {
  params: unknown[];
  networkParam: string | null;
  networkIsMulti: boolean;
  observerParam: string | null;
};

function buildScopePlaceholders(startIndex: number, network?: string, observer?: string): ScopePlaceholders {
  const params: unknown[] = [];
  let idx = startIndex;
  let networkParam: string | null = null;
  let networkIsMulti = false;
  if (network) {
    networkParam = `$${idx++}`;
    if (network === 'ukmesh') {
      params.push(UKMESH_NETWORKS);
      networkIsMulti = true;
    } else {
      params.push(network);
    }
  }
  const observerParam = observer ? `$${idx++}` : null;
  if (observer) params.push(observer);
  return { params, networkParam, networkIsMulti, observerParam };
}

function buildPacketScopeClause(
  placeholders: ScopePlaceholders,
  alias?: string,
  network?: string,
): string {
  const prefix = alias ? `${alias}.` : '';
  const conditions: string[] = [];
  if (placeholders.networkParam) {
    const netCond = placeholders.networkIsMulti
      ? `${prefix}network = ANY(${placeholders.networkParam})`
      : `${prefix}network = ${placeholders.networkParam}`;
    conditions.push(netCond);
    if (network !== 'test') {
      conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
    }
  } else {
    conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
  }
  if (placeholders.observerParam) {
    conditions.push(`${prefix}rx_node_id = ${placeholders.observerParam}`);
  }
  return conditions.length > 0 ? ` AND ${conditions.join(' AND ')}` : '';
}

function buildNodeScopeClause(
  placeholders: ScopePlaceholders,
  alias?: string,
): string {
  const prefix = alias ? `${alias}.` : '';
  // The outer node_id reference inside EXISTS must be qualified: an unqualified
  // node_id there resolves to s.node_id (always true). Callers without an alias
  // must be querying the nodes table directly.
  const nodeRef = alias ? `${alias}.node_id` : 'nodes.node_id';
  const conditions: string[] = [];

  if (placeholders.networkParam) {
    const netMatch = placeholders.networkIsMulti
      ? `= ANY(${placeholders.networkParam})`
      : `= ${placeholders.networkParam}`;
    // nodes.network is last-writer-wins across observers, so a node heard on
    // two networks flip-flops between them and randomly drops off each map.
    // node_network_sightings records every network a node is active on, so
    // also include nodes recently sighted on the requested network(s).
    conditions.push(
      `(
        ${prefix}network ${netMatch}
        OR (
          ${prefix}network IS DISTINCT FROM 'test'
          AND EXISTS (
            SELECT 1
            FROM node_network_sightings s
            WHERE s.node_id = ${nodeRef}
              AND s.network ${netMatch}
              AND s.last_seen_at > NOW() - INTERVAL '30 days'
          )
        )
      )`,
    );
  } else {
    conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
  }

  if (placeholders.observerParam) {
    const netCond = placeholders.networkParam
      ? (placeholders.networkIsMulti
          ? `AND p.network = ANY(${placeholders.networkParam})`
          : `AND p.network = ${placeholders.networkParam}`)
      : '';
    // IN (uncorrelated subquery) so the planner hashes the observer's heard-node
    // set once; a correlated EXISTS here runs per node row (minutes, not ms).
    // 7-day window matches the observer_meta lookback and keeps the packet
    // scan inside recent chunks (~700ms vs 5min unbounded).
    const observerNodeScope = [
      `${prefix}node_id = ${placeholders.observerParam}`,
      `OR ${nodeRef} IN (
         SELECT p.src_node_id
         FROM packets p
         WHERE p.rx_node_id = ${placeholders.observerParam}
           AND p.time > NOW() - INTERVAL '7 days'
           AND p.src_node_id IS NOT NULL`,
      netCond,
      `)`,
    ].filter(Boolean).join(' ');
    conditions.push(`(${observerNodeScope})`);
  }

  return conditions.length > 0 ? ` AND ${conditions.join(' AND ')}` : '';
}

export async function initDb(): Promise<void> {
  if (databaseConfig.skipSchemaInit) {
    console.log('[db] schema initialisation skipped by DATABASE_SKIP_SCHEMA_INIT');
    return;
  }

  const schemaPath = resolveDbAssetPath('schema', 'base.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  const startupPool = new Pool({
    connectionString: process.env.DATABASE_URL,
    application_name: `${databaseConfig.applicationName}-startup`,
    options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
    max: 1,
    idleTimeoutMillis: databaseConfig.idleTimeoutMs,
    connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
    statement_timeout: 0,
    query_timeout: 0,
  });

  let executedMigrations: string[] = [];
  try {
    if (databaseConfig.schema) {
      await startupPool.query(`CREATE SCHEMA IF NOT EXISTS "${databaseConfig.schema}"`);
    }
    await startupPool.query(sql);
    executedMigrations = await runMigrations(startupPool);
  } finally {
    await startupPool.end();
  }
  console.log(
    `[db] base schema initialised${executedMigrations.length > 0 ? `, migrations applied: ${executedMigrations.join(', ')}` : ', no pending migrations'}`,
  );
}

export async function incrementAdvertCount(nodeId: string): Promise<number> {
  const res = await pool.query<{ advert_count: number }>(
    `UPDATE nodes SET advert_count = advert_count + 1 WHERE node_id = $1 RETURNING advert_count`,
    [nodeId]
  );
  return res.rows[0]?.advert_count ?? 1;
}

export async function touchNodesPredictedOnline(nodeIds: string[]): Promise<void> {
  const ids = Array.from(new Set(nodeIds.map((id) => String(id).trim()).filter(Boolean)));
  if (ids.length < 1) return;
  await pool.query(
    `UPDATE nodes
     SET last_predicted_online_at = NOW()
     WHERE node_id = ANY($1::text[])`,
    [ids],
  );
}

export async function refreshRecentPathEvidence(
  hours = 1,
  network?: string,
): Promise<number> {
  const scope = buildScopePlaceholders(2, network);
  const params: unknown[] = [hours, ...scope.params];
  const result = await analyticsPool.query<{ updated_count: string }>(
    `WITH recent_hashes AS (
       SELECT p.time,
              p.path_hash_size_bytes,
              UPPER(h.hash) AS hash
       FROM packets p
       CROSS JOIN LATERAL unnest(p.path_hashes) AS h(hash)
       WHERE p.time > NOW() - INTERVAL '1 hour' * $1
         AND p.path_hashes IS NOT NULL
         AND cardinality(p.path_hashes) > 0
         ${buildPacketScopeClause(scope, 'p', network)}
     ),
     node_hashes AS (
       SELECT 1 AS path_hash_size_bytes, UPPER(LEFT(node_id, 2)) AS hash, node_id
       FROM nodes
       UNION ALL
       SELECT 2 AS path_hash_size_bytes, UPPER(LEFT(node_id, 4)) AS hash, node_id
       FROM nodes
       UNION ALL
       SELECT 3 AS path_hash_size_bytes, UPPER(LEFT(node_id, 6)) AS hash, node_id
       FROM nodes
     ),
     matched AS (
       SELECT nh.node_id,
              MAX(r.time) AS max_time
       FROM recent_hashes r
       JOIN node_hashes nh
         ON nh.path_hash_size_bytes = r.path_hash_size_bytes
        AND nh.hash = r.hash
       GROUP BY nh.node_id
     ),
     updated AS (
       UPDATE nodes n
       SET last_path_evidence_at = m.max_time
       FROM matched m
       WHERE n.node_id = m.node_id
         AND n.last_path_evidence_at IS DISTINCT FROM m.max_time
       RETURNING 1
     )
     SELECT COUNT(*)::text AS updated_count
     FROM updated`,
    params,
  );

  return Number(result.rows[0]?.updated_count ?? 0);
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
}): Promise<{ coordinatesChanged: boolean }> {
  const incomingLat = typeof updates.lat === 'number' && updates.lat !== 0 ? updates.lat : null;
  const incomingLon = typeof updates.lon === 'number' && updates.lon !== 0 ? updates.lon : null;
  const hasIncomingPosition = validCoordinatePair(incomingLat, incomingLon);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const existing = hasIncomingPosition
      ? await client.query<{ lat: number | null; lon: number | null }>(
          'SELECT lat, lon FROM nodes WHERE node_id = $1 FOR UPDATE',
          [nodeId],
        )
      : null;

    await client.query(
      `INSERT INTO nodes (node_id, name, lat, lon, iata, role, hardware_model, firmware_version, public_key, last_seen, is_online, network)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), TRUE, $10)
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
                              WHEN EXCLUDED.network = 'test' AND nodes.network IN ('ukmesh', 'northeast', 'teesside') THEN nodes.network
                              WHEN EXCLUDED.network IN ('ukmesh', 'teesside') THEN EXCLUDED.network
                              ELSE EXCLUDED.network
                            END,
         last_seen        = NOW(),
         is_online        = TRUE`,
      [nodeId, updates.name, updates.lat, updates.lon, updates.iata, updates.role,
       updates.hardwareModel, updates.firmwareVersion, updates.publicKey, updates.network ?? null, Boolean(updates.allowTestOverride)]
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
      await client.query('UPDATE nodes SET elevation_m = NULL WHERE node_id = $1', [nodeId]);
      await client.query('DELETE FROM node_coverage WHERE node_id = $1', [nodeId]);
    }

    await client.query('COMMIT');
    return { coordinatesChanged };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export async function insertPacket(p: {
  packetHash: string;
  rxNodeId?: string;
  srcNodeId?: string;
  topic: string;
  packetType?: number;
  routeType?: number;
  hopCount?: number;
  rssi?: number;
  snr?: number;
  payload?: Record<string, unknown>;
  summary?: string;
  rawHex: string;
  advertCount?: number;
  pathHashes?: string[];
  pathHashSizeBytes?: number;
  network?: string;
  transportCodes?: string;
  regionScope?: string;
}): Promise<void> {
  const inferredPathHashSizeBytes = (() => {
    if (typeof p.pathHashSizeBytes === 'number' && Number.isFinite(p.pathHashSizeBytes) && p.pathHashSizeBytes > 0) {
      return Math.trunc(p.pathHashSizeBytes);
    }
    const first = p.pathHashes?.[0];
    if (!first) return null;
    const len = String(first).trim().length;
    return len === 2 || len === 4 || len === 6 ? len / 2 : null;
  })();
  const storedPayload = p.payload
    ? (p.summary ? { ...p.payload, _summary: p.summary } : p.payload)
    : (p.summary ? { _summary: p.summary } : null);
  const network = p.network ?? 'teesside';
  await pool.query(
    `INSERT INTO packets
       (time, packet_hash, rx_node_id, src_node_id, topic, packet_type, route_type,
        hop_count, rssi, snr, payload, raw_hex, advert_count, path_hashes, path_hash_size_bytes, network,
        transport_codes, region_scope)
     VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
    [p.packetHash, p.rxNodeId, p.srcNodeId, p.topic, p.packetType,
     p.routeType, p.hopCount, p.rssi, p.snr,
     storedPayload ? JSON.stringify(storedPayload) : null, p.rawHex, p.advertCount ?? null,
     p.pathHashes ?? null, inferredPathHashSizeBytes, network,
     p.transportCodes ?? null, p.regionScope ?? null]
  );
  if (p.srcNodeId && network !== 'test') {
    pool.query(
      `INSERT INTO node_network_sightings (node_id, network, last_seen_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (node_id, network) DO UPDATE SET last_seen_at = NOW()`,
      [p.srcNodeId, network]
    ).catch(() => {}); // fire-and-forget, non-critical
  }
}

export async function insertNodeStatusSample(sample: {
  nodeId: string;
  network?: string;
  batteryMv?: number | null;
  uptimeSecs?: number | null;
  txAirSecs?: number | null;
  rxAirSecs?: number | null;
  channelUtilization?: number | null;
  airUtilTx?: number | null;
  stats?: Record<string, unknown> | null;
}): Promise<void> {
  await pool.query(
    `INSERT INTO node_status_samples
       (time, node_id, network, battery_mv, uptime_secs, tx_air_secs, rx_air_secs, channel_utilization, air_util_tx, stats)
     VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [
      sample.nodeId,
      sample.network ?? 'teesside',
      sample.batteryMv ?? null,
      sample.uptimeSecs ?? null,
      sample.txAirSecs ?? null,
      sample.rxAirSecs ?? null,
      sample.channelUtilization ?? null,
      sample.airUtilTx ?? null,
      sample.stats ? JSON.stringify(sample.stats) : null,
    ],
  );
}

export async function getNodes(network?: string, observer?: string) {
  const scope = buildScopePlaceholders(1, network, observer);
  const whereClause = `WHERE 1=1${buildNodeScopeClause(scope, 'n')}`;
  const res = await pool.query(
    `SELECT
       n.node_id,
       n.name,
       n.lat,
       n.lon,
       COALESCE(observer_meta.observer_iata, n.iata) AS iata,
       n.role,
       COALESCE(observer_meta.observer_last_seen, n.last_seen) AS last_seen,
       COALESCE(
         CASE
           WHEN observer_meta.observer_last_seen IS NOT NULL
           THEN observer_meta.observer_last_seen > NOW() - INTERVAL '15 minutes'
           ELSE NULL
         END,
         n.is_online
       ) AS is_online,
       n.hardware_model,
       n.public_key,
       n.advert_count,
       n.elevation_m
     FROM nodes n
     LEFT JOIN LATERAL (
       SELECT
         latest_topic.observer_iata,
         GREATEST(
           COALESCE(latest_topic.seen_at, '-infinity'::timestamptz),
           COALESCE(latest_status.seen_at, '-infinity'::timestamptz)
         ) AS observer_last_seen
       FROM LATERAL (
         SELECT
           p.time AS seen_at,
           UPPER(NULLIF(split_part(p.topic, '/', 2), '')) AS observer_iata
         FROM packets p
         WHERE p.rx_node_id = n.node_id
           AND split_part(p.topic, '/', 1) IN ('meshcore', 'ukmesh')
           AND p.time > NOW() - INTERVAL '7 days'
         ORDER BY p.time DESC
         LIMIT 1
       ) latest_topic
       FULL OUTER JOIN LATERAL (
         SELECT nss.time AS seen_at
         FROM node_status_samples nss
         WHERE nss.node_id = n.node_id
           AND nss.time > NOW() - INTERVAL '7 days'
         ORDER BY nss.time DESC
         LIMIT 1
       ) latest_status ON TRUE
     ) observer_meta ON TRUE
     ${whereClause}
     ORDER BY COALESCE(observer_meta.observer_last_seen, n.last_seen) DESC`,
    scope.params
  );
  return res.rows;
}

export async function getNodeHistory(nodeId: string, hours = 24) {
  const res = await pool.query(
    `SELECT time, packet_hash, src_node_id, topic, packet_type, hop_count, rssi, snr, payload
     FROM packets
     WHERE rx_node_id = $1 AND time > NOW() - INTERVAL '1 hour' * $2
     ORDER BY time DESC LIMIT 500`,
    [nodeId, hours]
  );
  return res.rows;
}

export async function getNodeAdverts(nodePublicKey: string, hours = 24, limit = 100) {
  // Get location packets (packet_type = 4) where payload->>'publicKey' = this public key
  // Location packets are sent as part of the advert broadcast
  const res = await pool.query(
    `SELECT time, packet_hash
     FROM packets
     WHERE packet_type = 4
       AND payload->>'publicKey' = $1
       AND time > NOW() - INTERVAL '1 hour' * $2
     ORDER BY time DESC
     LIMIT $3`,
    [nodePublicKey, hours, limit]
  );
  return res.rows;
}

export async function getRecentPackets(limit = 200, network?: string, observer?: string) {
  const scope = buildScopePlaceholders(2, network, observer);
  const fiveMinAgo = 'NOW() - INTERVAL \'5 minutes\'';
  const res = await pool.query(
    `WITH recent_packets AS (
      SELECT DISTINCT ON (p.packet_hash)
             p.time, p.packet_hash, p.rx_node_id, p.src_node_id, p.topic,
             p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
             p.payload->>'_summary' AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network
      FROM packets p
      WHERE p.time > ${fiveMinAgo}
        ${buildPacketScopeClause(scope, 'p', network)}
      ORDER BY p.packet_hash,
               CASE WHEN p.payload ? 'appData' THEN 1 ELSE 0 END DESC,
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               CASE WHEN p.advert_count IS NOT NULL THEN 1 ELSE 0 END DESC,
               CASE WHEN p.packet_type = 4 THEN 1 ELSE 0 END DESC,
               p.time DESC
    ),
    packet_stats AS (
      SELECT 
        packet_hash,
        ARRAY_AGG(DISTINCT rx_node_id ORDER BY rx_node_id) FILTER (WHERE rx_node_id IS NOT NULL) AS observer_node_ids,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') <> 'tx')::int AS rx_count,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') = 'tx')::int AS tx_count
      FROM packets
      WHERE packet_hash = ANY(SELECT packet_hash FROM recent_packets)
        AND time > ${fiveMinAgo}
        ${buildPacketScopeClause(scope, '', network)}
      GROUP BY packet_hash
    )
    SELECT 
      rp.time, rp.packet_hash, rp.rx_node_id, rp.src_node_id, rp.topic,
      rp.packet_type, rp.hop_count, rp.rssi, rp.snr, rp.payload,
      rp.summary, rp.advert_count, rp.path_hashes, rp.path_hash_size_bytes,
      ps.observer_node_ids, ps.rx_count, ps.tx_count
    FROM recent_packets rp
    LEFT JOIN packet_stats ps ON ps.packet_hash = rp.packet_hash
    ORDER BY rp.time DESC
    LIMIT $1`,
    [limit, ...scope.params]
  );
  return res.rows;
}

/**
 * Fetch the last N GroupText (type=5) messages from the last 24 hours.
 * Used to pre-populate the channel feed on page load so it isn't blank.
 * Returns rows with the same shape as getRecentPackets.
 */
export async function getRecentMessages(limit = 50, network?: string, observer?: string) {
  const scope = buildScopePlaceholders(2, network, observer);
  const res = await pool.query(
    `WITH recent_msgs AS (
      SELECT DISTINCT ON (p.packet_hash)
             p.time, p.packet_hash, p.rx_node_id, p.src_node_id, p.topic,
             p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
             p.payload->>'_summary' AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network
      FROM packets p
      WHERE p.packet_type = 5
        AND p.time > NOW() - INTERVAL '24 hours'
        ${buildPacketScopeClause(scope, 'p', network)}
      ORDER BY p.packet_hash,
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               p.time DESC
    ),
    msg_stats AS (
      SELECT
        packet_hash,
        ARRAY_AGG(DISTINCT rx_node_id ORDER BY rx_node_id) FILTER (WHERE rx_node_id IS NOT NULL) AS observer_node_ids,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') <> 'tx')::int AS rx_count,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') = 'tx')::int AS tx_count
      FROM packets
      WHERE packet_hash = ANY(SELECT packet_hash FROM recent_msgs)
        AND time > NOW() - INTERVAL '24 hours'
        ${buildPacketScopeClause(scope, '', network)}
      GROUP BY packet_hash
    )
    SELECT
      m.time, m.packet_hash, m.rx_node_id, m.src_node_id, m.topic,
      m.packet_type, m.hop_count, m.rssi, m.snr, m.payload,
      m.summary, m.advert_count, m.path_hashes, m.path_hash_size_bytes,
      ms.observer_node_ids, ms.rx_count, ms.tx_count
    FROM recent_msgs m
    LEFT JOIN msg_stats ms ON ms.packet_hash = m.packet_hash
    ORDER BY m.time DESC
    LIMIT $1`,
    [limit, ...scope.params],
  );
  return res.rows;
}

export async function getRecentPacketEvents(limit = 200, network?: string, observer?: string) {
  const scope = buildScopePlaceholders(2, network, observer);
  const params: unknown[] = [limit, ...scope.params];
  const res = await pool.query(
    `SELECT
        p.time, p.packet_hash, p.rx_node_id, p.src_node_id, p.topic,
        p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
        p.payload->>'_summary' AS summary,
        p.advert_count, p.path_hashes, p.path_hash_size_bytes
     FROM packets p
     WHERE p.time > NOW() - INTERVAL '24 hours'
         ${buildPacketScopeClause(scope, 'p', network)}
     ORDER BY p.time DESC
     LIMIT $1`,
    params,
  );
  return res.rows;
}

export async function getPacketDetail(hash: string, network?: string) {
  const netParam = network ?? null;
  const [primary, observations] = await Promise.all([
    pool.query(
      `SELECT p.time, p.packet_hash, p.rx_node_id, p.src_node_id, p.topic,
              p.packet_type, p.route_type, p.hop_count, p.rssi, p.snr,
              p.payload, p.path_hashes, p.path_hash_size_bytes, p.raw_hex
       FROM packets p
       WHERE p.packet_hash = $1
         AND ($2::text IS NULL OR p.network = $2)
       ORDER BY
         CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
         CASE WHEN p.raw_hex IS NOT NULL THEN 1 ELSE 0 END DESC,
         p.time DESC
       LIMIT 1`,
      [hash, netParam],
    ),
    pool.query(
      `SELECT p.rx_node_id, p.time, p.rssi, p.snr, p.hop_count
       FROM packets p
       WHERE p.packet_hash = $1
         AND ($2::text IS NULL OR p.network = $2)
       ORDER BY p.time ASC`,
      [hash, netParam],
    ),
  ]);
  const row = primary.rows[0];
  if (!row) return null;
  return {
    time: row.time as Date,
    packetHash: row.packet_hash as string,
    rxNodeId: row.rx_node_id as string | null,
    srcNodeId: row.src_node_id as string | null,
    topic: row.topic as string,
    packetType: row.packet_type as number | null,
    routeType: row.route_type as number | null,
    hopCount: row.hop_count as number | null,
    rssi: row.rssi as number | null,
    snr: row.snr as number | null,
    payload: row.payload as Record<string, unknown> | null,
    pathHashes: row.path_hashes as string[] | null,
    pathHashSizeBytes: row.path_hash_size_bytes as number | null,
    rawHex: row.raw_hex as string | null,
    observations: observations.rows.map((r) => ({
      rxNodeId: r.rx_node_id as string | null,
      time: r.time as Date,
      rssi: r.rssi as number | null,
      snr: r.snr as number | null,
      hopCount: r.hop_count as number | null,
    })),
  };
}

export async function getLastNPackets(n: number, network?: string, observer?: string) {
  // DISTINCT ON deduplicates by hash (same packet heard by multiple observers),
  // preferring the richest observation per hash within the last 24 hours.
  const scope = buildScopePlaceholders(2, network, observer);
  const params: unknown[] = [n, ...scope.params];
  const res = await pool.query(
    `SELECT * FROM (
       SELECT DISTINCT ON (p.packet_hash) p.time, p.packet_hash, p.rx_node_id, p.src_node_id,
              p.packet_type, p.hop_count, p.payload, p.payload->>'_summary' AS summary, p.advert_count, p.path_hashes, p.path_hash_size_bytes,
              (
                SELECT ARRAY_AGG(DISTINCT p2.rx_node_id ORDER BY p2.rx_node_id)
                FROM packets p2
                WHERE p2.packet_hash = p.packet_hash
                  AND p2.time > NOW() - INTERVAL '24 hours'
                  AND p2.rx_node_id IS NOT NULL
                  ${buildPacketScopeClause(scope, 'p2', network)}
              ) AS observer_node_ids,
              (
                SELECT COUNT(*)::int
                FROM packets p2
                WHERE p2.packet_hash = p.packet_hash
                  AND p2.time > NOW() - INTERVAL '24 hours'
                  AND COALESCE(p2.payload->>'direction', 'rx') <> 'tx'
                  ${buildPacketScopeClause(scope, 'p2', network)}
              ) AS rx_count,
              (
                SELECT COUNT(*)::int
                FROM packets p2
                WHERE p2.packet_hash = p.packet_hash
                  AND p2.time > NOW() - INTERVAL '24 hours'
                  AND COALESCE(p2.payload->>'direction', 'rx') = 'tx'
                  ${buildPacketScopeClause(scope, 'p2', network)}
              ) AS tx_count
       FROM packets p
       WHERE p.time > NOW() - INTERVAL '24 hours' ${buildPacketScopeClause(scope, 'p', network)}
       ORDER BY p.packet_hash,
                CASE WHEN payload ? 'appData' THEN 1 ELSE 0 END DESC,
                CASE WHEN src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                CASE WHEN advert_count IS NOT NULL THEN 1 ELSE 0 END DESC,
                CASE WHEN packet_type = 4 THEN 1 ELSE 0 END DESC,
                CASE WHEN payload->>'direction' = 'tx' THEN 1 ELSE 0 END DESC,
                p.time DESC
     ) deduped
     ORDER BY time DESC LIMIT $1`,
    params
  );
  return res.rows;
}

export type PathHistorySegmentRow = {
  positions: [[number, number], [number, number]];
  count: number;
};

export type PathHistoryCacheRow = {
  scope: string;
  window_start: string;
  updated_at: string;
  packet_count: number;
  resolved_packet_count: number;
  segment_counts: PathHistorySegmentRow[];
};

export async function getRecentPathHistoryPacketHashes(
  hours = 1,
  network?: string,
  limit = 1200,
): Promise<string[]> {
  const scope = buildScopePlaceholders(3, network);
  const params: unknown[] = [hours, limit, ...scope.params];
  const res = await pool.query<{ packet_hash: string }>(
    `SELECT packet_hash
     FROM (
       SELECT p.packet_hash, MAX(p.time) AS last_seen
       FROM packets p
       WHERE p.time > NOW() - INTERVAL '1 hour' * $1
         AND p.path_hashes IS NOT NULL
         AND cardinality(p.path_hashes) > 0
         ${buildPacketScopeClause(scope, 'p', network)}
       GROUP BY p.packet_hash
     ) recent
     ORDER BY last_seen DESC
     LIMIT $2`,
    params,
  );
  return res.rows.map((row) => row.packet_hash).filter(Boolean);
}

export async function upsertPathHistoryCache(entry: {
  scope: string;
  windowStart: Date;
  packetCount: number;
  resolvedPacketCount: number;
  segmentCounts: PathHistorySegmentRow[];
}): Promise<void> {
  await pool.query(
    `INSERT INTO path_history_cache (scope, window_start, updated_at, packet_count, resolved_packet_count, segment_counts)
     VALUES ($1, $2, NOW(), $3, $4, $5::jsonb)
     ON CONFLICT (scope) DO UPDATE SET
       window_start = EXCLUDED.window_start,
       updated_at = NOW(),
       packet_count = EXCLUDED.packet_count,
       resolved_packet_count = EXCLUDED.resolved_packet_count,
       segment_counts = EXCLUDED.segment_counts`,
    [
      entry.scope,
      entry.windowStart.toISOString(),
      entry.packetCount,
      entry.resolvedPacketCount,
      JSON.stringify(entry.segmentCounts),
    ],
  );
}

export async function getPathHistoryCache(scope: string): Promise<PathHistoryCacheRow | null> {
  const res = await pool.query<{
    scope: string;
    window_start: string;
    updated_at: string;
    packet_count: number;
    resolved_packet_count: number;
    segment_counts: PathHistorySegmentRow[] | null;
  }>(
    `SELECT scope, window_start, updated_at, packet_count, resolved_packet_count, segment_counts
     FROM path_history_cache
     WHERE scope = $1`,
    [scope],
  );
  const row = res.rows[0];
  if (!row) return null;
  return {
    scope: row.scope,
    window_start: row.window_start,
    updated_at: row.updated_at,
    packet_count: row.packet_count,
    resolved_packet_count: row.resolved_packet_count,
    segment_counts: Array.isArray(row.segment_counts) ? row.segment_counts : [],
  };
}

export type MultibytePathSegmentRow = {
  positions: [[number, number], [number, number]];
  count: number;
};

export async function getMultibytePathSegments(network?: string, observer?: string): Promise<{
  maxCount: number;
  segments: MultibytePathSegmentRow[];
}> {
  const scope = buildScopePlaceholders(1, network, observer);
  const params: unknown[] = [...scope.params];

  const res = await pool.query<{
    a_lat: number;
    a_lon: number;
    b_lat: number;
    b_lon: number;
    count: number;
  }>(
    `SELECT
       a.lat AS a_lat,
       a.lon AS a_lon,
       b.lat AS b_lat,
       b.lon AS b_lon,
       nl.multibyte_observed_count AS count
     FROM node_links nl
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE nl.multibyte_observed_count > 0
       AND nl.itm_viable = true
       AND a.lat IS NOT NULL
       AND a.lon IS NOT NULL
       AND b.lat IS NOT NULL
       AND b.lon IS NOT NULL
       AND a.lat BETWEEN -90 AND 90
       AND b.lat BETWEEN -90 AND 90
       AND a.lon BETWEEN -180 AND 180
       AND b.lon BETWEEN -180 AND 180
       AND NOT (ABS(a.lat) < 5 AND ABS(a.lon) < 5)
       AND NOT (ABS(b.lat) < 5 AND ABS(b.lon) < 5)
       AND SQRT(
         POWER((a.lat - b.lat) * 111, 2)
         + POWER((a.lon - b.lon) * 111 * COS(RADIANS((a.lat + b.lat) / 2)), 2)
       ) <= ${MAX_MULTIBYTE_PATH_SEGMENT_KM}
       ${buildNodeScopeClause(scope, 'a')}
       ${buildNodeScopeClause(scope, 'b')}
     ORDER BY nl.multibyte_observed_count DESC`,
    params,
  );

  const segments = res.rows.map((row) => ({
    positions: [
      [Number(row.a_lat), Number(row.a_lon)],
      [Number(row.b_lat), Number(row.b_lon)],
    ] as [[number, number], [number, number]],
    count: Number(row.count ?? 0),
  }));

  const maxCount = segments.reduce((max, segment) => Math.max(max, segment.count), 0);
  return { maxCount, segments };
}

/** Minimum observations required before a link is considered confirmed. */
export const MIN_LINK_OBSERVATIONS = 5;

/** Returns only confirmed viable link pairs — compact for sending in initial WebSocket state. */
export async function getViableLinkPairs(network?: string, observer?: string): Promise<[string, string][]> {
  const scope = buildScopePlaceholders(1, network, observer);
  const params: unknown[] = [...scope.params];

  const res = await pool.query<{ node_a_id: string; node_b_id: string }>(
    `SELECT nl.node_a_id, nl.node_b_id
     FROM node_links nl
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       ${buildNodeScopeClause(scope, 'a')}
       ${buildNodeScopeClause(scope, 'b')}`,
    params,
  );
  return res.rows.map((r) => [r.node_a_id, r.node_b_id]);
}

export type ViableLinkRow = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  multibyte_observed_count: number;
  neighbor_report_count: number;
  neighbor_best_snr_db: number | null;
  itm_viable: boolean | null;
  itm_path_loss_db: number | null;
  count_a_to_b: number;
  count_b_to_a: number;
};

/** Returns viable links with metrics so UI can render precomputed styles immediately. */
export async function getViableLinks(network?: string, observer?: string): Promise<ViableLinkRow[]> {
  // For network-scoped queries we pre-compute the set of nodes seen on that
  // network in a CTE, then join on it — replacing the correlated EXISTS
  // subquery in buildNodeScopeClause which ran once per row and caused
  // full scans on the packets table (30 s+ for teesside).
  if (network && !observer) {
    const res = await pool.query<ViableLinkRow>(
      `WITH net_nodes AS (
         SELECT DISTINCT node_id FROM nodes WHERE network = $1
       )
       SELECT
         nl.node_a_id,
         nl.node_b_id,
         nl.observed_count,
         nl.multibyte_observed_count,
         COALESCE(nr.neighbor_report_count, 0) AS neighbor_report_count,
         nr.neighbor_best_snr_db,
         nl.itm_viable,
         nl.itm_path_loss_db,
         nl.count_a_to_b,
         nl.count_b_to_a
       FROM node_links nl
       LEFT JOIN (
         SELECT node_a_id, node_b_id,
           SUM(sample_count)::int AS neighbor_report_count,
           MAX(best_snr_db) AS neighbor_best_snr_db
         FROM node_link_radio_reports
         GROUP BY node_a_id, node_b_id
       ) nr ON nr.node_a_id = nl.node_a_id AND nr.node_b_id = nl.node_b_id
       WHERE (nl.itm_viable = true OR nl.force_viable = true)
         AND nl.node_a_id IN (SELECT node_id FROM net_nodes)
         AND nl.node_b_id IN (SELECT node_id FROM net_nodes)`,
      [network],
    );
    return res.rows;
  }

  const scope = buildScopePlaceholders(1, network, observer);
  const params: unknown[] = [...scope.params];

  const res = await pool.query<ViableLinkRow>(
    `SELECT
       nl.node_a_id,
       nl.node_b_id,
       nl.observed_count,
       nl.multibyte_observed_count,
       COALESCE(nr.neighbor_report_count, 0) AS neighbor_report_count,
       nr.neighbor_best_snr_db,
       nl.itm_viable,
       nl.itm_path_loss_db,
       nl.count_a_to_b,
       nl.count_b_to_a
     FROM node_links nl
     LEFT JOIN (
       SELECT node_a_id, node_b_id,
         SUM(sample_count)::int AS neighbor_report_count,
         MAX(best_snr_db) AS neighbor_best_snr_db
       FROM node_link_radio_reports
       GROUP BY node_a_id, node_b_id
     ) nr ON nr.node_a_id = nl.node_a_id AND nr.node_b_id = nl.node_b_id
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       ${buildNodeScopeClause(scope, 'a')}
       ${buildNodeScopeClause(scope, 'b')}`,
    params,
  );
  return res.rows;
}

export { pool, analyticsPool };

// ---------------------------------------------------------------------------
// Spam detection
// ---------------------------------------------------------------------------

export interface SpamSuspectRow {
  srcNodeId: string;
  spoofedName: string;
  publicKey?: string;
  claimedLat?: number;
  claimedLon?: number;
  canonicalKey?: string;
  verdict: string;
  signals: Array<{ name: string; score: number; detail: string }>;
  totalScore: number;
  network: string;
}

export async function insertOrUpdateSpamSuspect(s: SpamSuspectRow): Promise<void> {
  await pool.query(
    `INSERT INTO spam_suspects
       (time, first_seen, src_node_id, spoofed_name, public_key, claimed_lat, claimed_lon,
        canonical_key, verdict, signals, total_score, network)
     VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT (src_node_id) DO UPDATE SET
       time        = NOW(),
       first_seen  = COALESCE(spam_suspects.first_seen, EXCLUDED.first_seen),
       verdict     = EXCLUDED.verdict,
       signals     = EXCLUDED.signals,
       total_score = EXCLUDED.total_score,
       claimed_lat = COALESCE(EXCLUDED.claimed_lat, spam_suspects.claimed_lat),
       claimed_lon = COALESCE(EXCLUDED.claimed_lon, spam_suspects.claimed_lon)`,
    [s.srcNodeId, s.spoofedName, s.publicKey ?? null, s.claimedLat ?? null, s.claimedLon ?? null,
     s.canonicalKey ?? null, s.verdict, JSON.stringify(s.signals), s.totalScore, s.network]
  );
}

export async function replaceSpamSuspects(suspects: SpamSuspectRow[]): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM spam_suspects');
    for (const s of suspects) {
      await client.query(
        `INSERT INTO spam_suspects
           (time, first_seen, src_node_id, spoofed_name, public_key, claimed_lat, claimed_lon,
            canonical_key, verdict, signals, total_score, network)
         VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [s.srcNodeId, s.spoofedName, s.publicKey ?? null, s.claimedLat ?? null, s.claimedLon ?? null,
         s.canonicalKey ?? null, s.verdict, JSON.stringify(s.signals), s.totalScore, s.network]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export interface SpamSuspectQueryOptions {
  hours?: number;
  verdict?: 'spam' | 'suspect';
  minScore?: number;
  limit?: number;
  offset?: number;
  includePacketCounts?: boolean;
}

function buildSpamSuspectWhere(options: SpamSuspectQueryOptions, params: unknown[]): string {
  const clauses = [`ss.time > NOW() - ($1 * INTERVAL '1 hour')`];

  if (options.verdict) {
    params.push(options.verdict);
    clauses.push(`ss.verdict = $${params.length}`);
  }

  if (options.minScore != null) {
    params.push(options.minScore);
    clauses.push(`ss.total_score >= $${params.length}`);
  }

  return clauses.join(' AND ');
}

export async function getSpamSuspects(options: number | SpamSuspectQueryOptions = 48) {
  const queryOptions: SpamSuspectQueryOptions = typeof options === 'number' ? { hours: options } : options;
  const params: unknown[] = [queryOptions.hours ?? 48];
  const where = buildSpamSuspectWhere(queryOptions, params);
  const limit = Math.max(1, Math.min(200, Math.floor(queryOptions.limit ?? 100)));
  const offset = Math.max(0, Math.floor(queryOptions.offset ?? 0));
  params.push(limit, offset);
  const limitParam = params.length - 1;
  const offsetParam = params.length;

  if (!queryOptions.includePacketCounts) {
    const res = await pool.query(
      `SELECT
         ss.src_node_id,
         ss.spoofed_name,
         ss.public_key,
         ss.claimed_lat,
         ss.claimed_lon,
         ss.canonical_key,
         ss.verdict,
         ss.signals,
         ss.total_score,
         ss.network,
         ss.time AS detected_at,
         ss.first_seen,
         NULL::int AS packet_count,
         NULL::timestamptz AS first_packet,
         NULL::timestamptz AS last_packet
       FROM spam_suspects ss
       WHERE ${where}
       ORDER BY ss.total_score DESC, ss.time DESC
       LIMIT $${limitParam} OFFSET $${offsetParam}`,
      params
    );
    return res.rows;
  }

  const res = await pool.query(
    `WITH filtered AS (
       SELECT
         ss.src_node_id,
         ss.spoofed_name,
         ss.public_key,
         ss.claimed_lat,
         ss.claimed_lon,
         ss.canonical_key,
         ss.verdict,
         ss.signals,
         ss.total_score,
         ss.network,
         ss.time,
         ss.first_seen
       FROM spam_suspects ss
       WHERE ${where}
       ORDER BY ss.total_score DESC, ss.time DESC
       LIMIT $${limitParam} OFFSET $${offsetParam}
     )
     SELECT
       f.src_node_id,
       f.spoofed_name,
       f.public_key,
       f.claimed_lat,
       f.claimed_lon,
       f.canonical_key,
       f.verdict,
       f.signals,
       f.total_score,
       f.network,
       f.time AS detected_at,
       f.first_seen,
       COUNT(p.packet_hash) AS packet_count,
       MIN(p.time) AS first_packet,
       MAX(p.time) AS last_packet
     FROM filtered f
     LEFT JOIN packets p ON p.src_node_id = f.src_node_id
       AND p.time > NOW() - INTERVAL '1 hour' * $1
     GROUP BY f.src_node_id, f.spoofed_name, f.public_key, f.claimed_lat, f.claimed_lon,
              f.canonical_key, f.verdict, f.signals, f.total_score, f.network, f.time, f.first_seen
     ORDER BY f.total_score DESC, f.time DESC`,
    params
  );
  return res.rows;
}

export async function getSpamSuspectSummary(options: SpamSuspectQueryOptions = {}) {
  const params: unknown[] = [options.hours ?? 48];
  const where = buildSpamSuspectWhere(options, params);
  const res = await pool.query(
    `SELECT verdict, COUNT(*)::int AS count
     FROM spam_suspects ss
     WHERE ${where}
     GROUP BY verdict`,
    params
  );

  const summary = { total: 0, spam: 0, suspect: 0 };
  for (const row of res.rows) {
    const count = Number(row.count ?? 0);
    summary.total += count;
    if (row.verdict === 'spam') summary.spam = count;
    if (row.verdict === 'suspect') summary.suspect = count;
  }
  return summary;
}

export async function getSpamPacketObservers(srcNodeId: string) {
  const res = await pool.query(
    `SELECT DISTINCT ON (p.rx_node_id)
       p.rx_node_id AS node_id,
       n.name,
       n.iata,
       n.lat,
       n.lon,
       p.hop_count,
       p.rssi,
       p.time,
       ss.claimed_lat,
       ss.claimed_lon,
       ss.spoofed_name
     FROM packets p
     LEFT JOIN nodes n ON n.node_id = p.rx_node_id
     LEFT JOIN spam_suspects ss ON ss.src_node_id = p.src_node_id
     WHERE p.src_node_id = $1
       AND p.packet_type = 4
       AND p.time > NOW() - INTERVAL '30 days'
     ORDER BY p.rx_node_id, p.hop_count ASC NULLS LAST, p.time ASC`,
    [srcNodeId]
  );
  return res.rows;
}

export async function getSpamAllObservers() {
  const res = await pool.query(
    `SELECT
       ss.src_node_id,
       ss.claimed_lat,
       ss.claimed_lon,
       ss.spoofed_name,
       p.rx_node_id        AS observer_id,
       n.name              AS observer_name,
       n.lat               AS observer_lat,
       n.lon               AS observer_lon,
       MIN(p.hop_count)    AS hop_count,
       MAX(p.rssi)         AS rssi
     FROM spam_suspects ss
     JOIN packets p
       ON  p.src_node_id = ss.src_node_id
       AND p.packet_type  = 4
       AND p.time > NOW() - INTERVAL '30 days'
       AND p.rx_node_id   IS NOT NULL
     JOIN nodes n ON n.node_id = p.rx_node_id
     WHERE ss.claimed_lat IS NOT NULL
       AND ss.claimed_lon IS NOT NULL
       AND n.lat IS NOT NULL
       AND n.lon IS NOT NULL
     GROUP BY
       ss.src_node_id, ss.claimed_lat, ss.claimed_lon, ss.spoofed_name,
       p.rx_node_id, n.name, n.lat, n.lon
     ORDER BY ss.src_node_id, MIN(p.hop_count) ASC NULLS LAST`
  );
  return res.rows;
}
