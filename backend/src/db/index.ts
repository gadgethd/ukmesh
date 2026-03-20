import pg from 'pg';
import fs from 'node:fs';
import { databaseConfig } from '../platform/config/database.js';
import { resolveDbAssetPath } from './assets.js';
import { runMigrations } from './migrations.js';

const { Pool } = pg;

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

export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[]
): Promise<pg.QueryResult<T>> {
  return pool.query<T>(text, params);
}

type ScopePlaceholders = {
  params: unknown[];
  networkParam: string | null;
  observerParam: string | null;
};

function buildScopePlaceholders(startIndex: number, network?: string, observer?: string): ScopePlaceholders {
  const params: unknown[] = [];
  let idx = startIndex;
  const networkParam = network ? `$${idx++}` : null;
  if (network) params.push(network);
  const observerParam = observer ? `$${idx++}` : null;
  if (observer) params.push(observer);
  return { params, networkParam, observerParam };
}

function buildPacketScopeClause(
  placeholders: ScopePlaceholders,
  alias?: string,
  network?: string,
): string {
  const prefix = alias ? `${alias}.` : '';
  const conditions: string[] = [];
  if (placeholders.networkParam) {
    conditions.push(`${prefix}network = ${placeholders.networkParam}`);
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
  const conditions: string[] = [];

  if (placeholders.networkParam) {
    conditions.push(
      `(
        ${prefix}network = ${placeholders.networkParam}
        OR (
          ${placeholders.networkParam} = 'teesside'
          AND EXISTS (
            SELECT 1
            FROM node_network_sightings s
            WHERE s.node_id = ${prefix}node_id
              AND s.network = 'teesside'
          )
        )
      )`,
    );
  } else {
    conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
  }

  if (placeholders.observerParam) {
    const observerNodeScope = [
      `${prefix}node_id = ${placeholders.observerParam}`,
      `EXISTS (
         SELECT 1
         FROM packets p
         WHERE p.rx_node_id = ${placeholders.observerParam}`,
      placeholders.networkParam ? `AND p.network = ${placeholders.networkParam}` : '',
      `AND p.src_node_id = ${prefix}node_id
       )`,
    ].filter(Boolean).join(' ');
    conditions.push(`(${observerNodeScope})`);
  }

  return conditions.length > 0 ? ` AND ${conditions.join(' AND ')}` : '';
}

export async function initDb(): Promise<void> {
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
  const result = await pool.query<{ updated_count: string }>(
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
     matched AS (
       SELECT n.node_id,
              MAX(r.time) AS max_time
       FROM recent_hashes r
       JOIN nodes n
         ON (
           (r.path_hash_size_bytes = 1 AND r.hash = UPPER(LEFT(n.node_id, 2)))
           OR (r.path_hash_size_bytes = 2 AND r.hash = UPPER(LEFT(n.node_id, 4)))
           OR (r.path_hash_size_bytes = 3 AND r.hash = UPPER(LEFT(n.node_id, 6)))
         )
       GROUP BY n.node_id
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
}): Promise<void> {
  await pool.query(
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
                            WHEN EXCLUDED.network = 'test' AND nodes.network IN ('ukmesh', 'teesside') THEN nodes.network
                            WHEN EXCLUDED.network IN ('ukmesh', 'teesside') THEN EXCLUDED.network
                            ELSE EXCLUDED.network
                          END,
       last_seen        = NOW(),
       is_online        = TRUE`,
    [nodeId, updates.name, updates.lat, updates.lon, updates.iata, updates.role,
     updates.hardwareModel, updates.firmwareVersion, updates.publicKey, updates.network ?? null, Boolean(updates.allowTestOverride)]
  );
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
        hop_count, rssi, snr, payload, raw_hex, advert_count, path_hashes, path_hash_size_bytes, network)
     VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
    [p.packetHash, p.rxNodeId, p.srcNodeId, p.topic, p.packetType,
     p.routeType, p.hopCount, p.rssi, p.snr,
     storedPayload ? JSON.stringify(storedPayload) : null, p.rawHex, p.advertCount ?? null,
     p.pathHashes ?? null, inferredPathHashSizeBytes, network]
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
  const whereClause = `WHERE 1=1${buildNodeScopeClause(scope)}`;
  const res = await pool.query(
    `SELECT node_id, name, lat, lon, iata, role, last_seen, is_online, hardware_model, public_key, advert_count, elevation_m
     FROM nodes ${whereClause} ORDER BY last_seen DESC`,
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
       LEFT JOIN LATERAL (
         SELECT
           SUM(sample_count)::int AS neighbor_report_count,
           MAX(best_snr_db) AS neighbor_best_snr_db
         FROM node_link_radio_reports rr
         WHERE rr.node_a_id = nl.node_a_id
           AND rr.node_b_id = nl.node_b_id
       ) nr ON TRUE
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
     LEFT JOIN LATERAL (
       SELECT
         SUM(sample_count)::int AS neighbor_report_count,
         MAX(best_snr_db) AS neighbor_best_snr_db
       FROM node_link_radio_reports rr
       WHERE rr.node_a_id = nl.node_a_id
         AND rr.node_b_id = nl.node_b_id
     ) nr ON TRUE
     JOIN nodes a ON a.node_id = nl.node_a_id
     JOIN nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       ${buildNodeScopeClause(scope, 'a')}
       ${buildNodeScopeClause(scope, 'b')}`,
    params,
  );
  return res.rows;
}

export { pool };
