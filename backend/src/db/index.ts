import pg from 'pg';
import fs from 'node:fs';
import {
  analyticsStatementTimeoutMs,
  databaseConfig,
} from '../platform/config/database.js';
import { resolveDbAssetPath } from './assets.js';
import { runMigrations } from './migrations.js';
import { UKMESH_NETWORKS } from '../networks.js';
import {
  configurePacketBatch,
  enqueuePacket,
  type PacketBatchResult,
} from './packetBatch.js';
import { nodeAliasArraySql, publicPacketPrivacySql } from '../api/utils/networkFilters.js';
import {
  dbQueriesTotal,
  dbQueryDuration,
  updateDbPoolMetrics,
} from '../metrics.js';
import {
  nodeEffectiveLastSeenSql,
  nodeEffectiveOnlineSql,
} from '../nodes/presence.js';
import {
  reactivateHistoricPathNodes,
  type HistoricPathNode,
} from '../repositories/pathEvidence.js';
import { channelHashesForName } from '../mqtt/channelRegistry.js';

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
configurePacketBatch((text, params) => pool.query(text, params));

// Dedicated pool for long-running analytical queries (e.g. refreshRecentPathEvidence).
// Kept separate so slow analytics can never exhaust the OLTP pool and take down the API.
const analyticsPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  application_name: `${databaseConfig.applicationName}-analytics`,
  options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
  max: 2,
  idleTimeoutMillis: databaseConfig.idleTimeoutMs,
  connectionTimeoutMillis: databaseConfig.connectionTimeoutMs,
  statement_timeout: analyticsStatementTimeoutMs(databaseConfig.statementTimeoutMs),
  query_timeout: analyticsStatementTimeoutMs(databaseConfig.statementTimeoutMs),
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

function observerRegionFromTopic(topic: string): string | null {
  const [prefix = '', rawRegion = ''] = topic.split('/');
  if (prefix === 'meshcore-test') return null;
  return rawRegion.trim().toUpperCase() || 'UNK';
}

async function queryPool<T extends pg.QueryResultRow = pg.QueryResultRow>(
  targetPool: pg.Pool,
  poolName: 'oltp' | 'analytics',
  querySpec: string | pg.QueryConfig<unknown[]>,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  const startedAt = process.hrtime.bigint();
  let outcome = 'success';
  try {
    updateDbPoolMetrics(poolName, targetPool);
    const execute = (target: pg.Pool | pg.PoolClient) => (
      typeof querySpec === 'string'
        ? target.query<T>(querySpec, params)
        : target.query<T>(querySpec)
    );
    if (!signal) return await execute(targetPool);
    signal.throwIfAborted();
    const client = await targetPool.connect();
    let destroyed = false;
    let abortedCleanup: (() => void) | undefined;
    const aborted = new Promise<never>((_resolve, reject) => {
      const onAbort = () => {
        if (destroyed) return;
        destroyed = true;
        // node-postgres has no AbortSignal query API. Destroy this one
        // checked-out connection so PostgreSQL cancels the active statement and
        // the connection can never return to the pool.
        client.release(true);
        reject(
          signal.reason instanceof Error
            ? signal.reason
            : new Error('database query aborted'),
        );
      };
      signal.addEventListener('abort', onAbort, { once: true });
      abortedCleanup = () => signal.removeEventListener('abort', onAbort);
    });
    try {
      signal.throwIfAborted();
      const result = await Promise.race([
        execute(client),
        aborted,
      ]);
      signal.throwIfAborted();
      return result;
    } finally {
      abortedCleanup?.();
      if (!destroyed) client.release();
    }
  } catch (error) {
    outcome = signal?.aborted ? 'aborted' : 'failure';
    throw error;
  } finally {
    const durationSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9;
    dbQueriesTotal.inc({ pool: poolName, outcome });
    dbQueryDuration.observe({ pool: poolName, outcome }, durationSeconds);
    updateDbPoolMetrics(poolName, targetPool);
  }
}

export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  return queryPool<T>(pool, 'oltp', text, params, signal);
}

export async function namedQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
  name: string,
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  if (!/^[a-z0-9_-]{1,63}$/.test(name)) throw new Error('INVALID_PREPARED_STATEMENT_NAME');
  return queryPool<T>(pool, 'oltp', { name, text, values: params }, undefined, signal);
}

export async function analyticsQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<pg.QueryResult<T>> {
  return queryPool<T>(analyticsPool, 'analytics', text, params, signal);
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
      conditions.push(
        `COALESCE(NULLIF(${prefix}topic_prefix, ''), split_part(${prefix}topic, '/', 1)) <> 'meshcore-test'`,
      );
    }
  } else {
    conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    conditions.push(
      `COALESCE(NULLIF(${prefix}topic_prefix, ''), split_part(${prefix}topic, '/', 1)) <> 'meshcore-test'`,
    );
  }
  if (placeholders.observerParam) {
    conditions.push(
      `${prefix}rx_node_id = ANY(${nodeAliasArraySql(placeholders.observerParam)})`,
    );
  }
  return conditions.length > 0 ? ` AND ${conditions.join(' AND ')}` : '';
}

function buildPublicPacketPrivacyClause(alias?: string): string {
  return ` AND ${publicPacketPrivacySql(alias)}`;
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
            FROM node_identity_sightings s
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
      `${nodeRef} = ANY(${nodeAliasArraySql(placeholders.observerParam)})`,
      `OR ${nodeRef} IN (
         SELECT COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id)))
         FROM packets p
         LEFT JOIN node_identity_aliases src_alias
           ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
         WHERE p.rx_node_id = ANY(${nodeAliasArraySql(placeholders.observerParam)})
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

/**
 * Refresh `last_path_evidence_at` from recent MULTIBYTE path hashes (2–3 byte).
 *
 * A repeater that shows up in a 2- or 3-byte path hash almost certainly relayed
 * that packet, so it was online at that moment — single-byte (1-byte) hashes are
 * too collision-prone to trust and are deliberately ignored here. Prefixes shared
 * by more than one repeater are also excluded (ambiguous → no credit). This is the
 * hourly backstop for the real-time `recordMultibyteEvidence` path; both only ever
 * move the timestamp forward.
 */
export async function refreshRecentPathEvidence(
  hours = 1,
  network?: string,
): Promise<number> {
  const scope = buildScopePlaceholders(2, network);
  const params: unknown[] = [hours, ...scope.params];
  // Path hashes are intentionally short. Never resolve a test packet against a
  // public node (or vice versa), even when a prefix happens to be unique across
  // the combined node table.
  const pathEvidenceNodeScope = network === 'test'
    ? "network = 'test'"
    : "network IS DISTINCT FROM 'test'";
  const result = await analyticsPool.query<{ updated_count: string }>(
    `WITH recent_hashes AS (
       SELECT p.path_hash_size_bytes,
              UPPER(h.hash) AS hash,
              MAX(p.time) AS max_time
       FROM packets p
       CROSS JOIN LATERAL unnest(p.path_hashes) AS h(hash)
       WHERE p.time > NOW() - INTERVAL '1 hour' * $1
         AND p.path_hash_size_bytes >= 2
         AND p.path_hashes IS NOT NULL
         AND cardinality(p.path_hashes) > 0
         -- Direct routes encode remaining destinations, not relays already
         -- traversed. Only Flood/TransportFlood paths prove recent presence.
         AND p.route_type IN (0, 1)
         ${buildPacketScopeClause(scope, 'p', network)}
       GROUP BY p.path_hash_size_bytes, UPPER(h.hash)
     ),
     node_hashes AS (
       SELECT 2 AS path_hash_size_bytes, UPPER(LEFT(node_id, 4)) AS hash, node_id
       FROM nodes
       WHERE (role = 2 OR role IS NULL) AND ${pathEvidenceNodeScope}
         AND lat BETWEEN -90 AND 90
         AND lon BETWEEN -180 AND 180
         AND NOT (ABS(lat) < 1e-9 AND ABS(lon) < 1e-9)
       UNION ALL
       SELECT 3 AS path_hash_size_bytes, UPPER(LEFT(node_id, 6)) AS hash, node_id
       FROM nodes
       WHERE (role = 2 OR role IS NULL) AND ${pathEvidenceNodeScope}
         AND lat BETWEEN -90 AND 90
         AND lon BETWEEN -180 AND 180
         AND NOT (ABS(lat) < 1e-9 AND ABS(lon) < 1e-9)
     ),
     unique_node_hashes AS (
       SELECT path_hash_size_bytes, hash, MIN(node_id) AS node_id
       FROM node_hashes
       GROUP BY path_hash_size_bytes, hash
       HAVING COUNT(*) = 1
     ),
     matched AS (
       SELECT nh.node_id,
              MAX(r.max_time) AS max_time
       FROM recent_hashes r
       JOIN unique_node_hashes nh
         ON nh.path_hash_size_bytes = r.path_hash_size_bytes
        AND nh.hash = r.hash
       GROUP BY nh.node_id
     ),
     updated AS (
       UPDATE nodes n
       SET last_path_evidence_at = m.max_time
       FROM matched m
       WHERE n.node_id = m.node_id
         AND (n.last_path_evidence_at IS NULL OR n.last_path_evidence_at < m.max_time)
       RETURNING 1
     )
     SELECT COUNT(*)::text AS updated_count
     FROM updated`,
    params,
  );

  return Number(result.rows[0]?.updated_count ?? 0);
}

/**
 * Real-time counterpart to {@link refreshRecentPathEvidence}: given the multibyte
 * path hashes of a single freshly-ingested packet, credit each repeater whose
 * prefix uniquely matches with `last_path_evidence_at = seenAt`. Returns the
 * preserved historic node rows so the caller can broadcast their coordinates.
 *
 * Only 2- and 3-byte hashes are accepted (single-byte is too collision-prone), and
 * prefixes shared by more than one repeater are skipped (ambiguous). Best-effort:
 * callers fire-and-forget.
 */
export async function recordMultibyteEvidence(
  pathHashes: string[],
  sizeBytes: number,
  seenAt: Date,
  routeType?: number,
  network?: string,
): Promise<HistoricPathNode[]> {
  return reactivateHistoricPathNodes(query, {
    pathHashes,
    sizeBytes,
    seenAt,
    routeType,
    network,
  });
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
    const result = await pool.query<{ advert_count: number }>(
      `WITH advert_once AS (
         INSERT INTO node_counted_adverts (canonical_advert_hash, node_id, counted_at)
         SELECT $13, $1, NOW()
         WHERE $13::text IS NOT NULL
         ON CONFLICT (canonical_advert_hash) DO NOTHING
         RETURNING 1
       )
       INSERT INTO nodes (node_id, name, lat, lon, iata, role, hardware_model, firmware_version, public_key, last_seen, is_online, network, last_mqtt_observer_seen_at, advert_count)
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
                              WHEN EXCLUDED.network = 'test' AND nodes.network IN ('ukmesh', 'northeast', 'teesside') THEN nodes.network
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

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const existing = hasIncomingPosition
      ? await client.query<{ lat: number | null; lon: number | null }>(
          'SELECT lat, lon FROM nodes WHERE node_id = $1 FOR UPDATE',
          [nodeId],
        )
      : null;

    const upsert = await client.query<{ advert_count: number }>(
      `WITH advert_once AS (
         INSERT INTO node_counted_adverts (canonical_advert_hash, node_id, counted_at)
         SELECT $13, $1, NOW()
         WHERE $13::text IS NOT NULL
         ON CONFLICT (canonical_advert_hash) DO NOTHING
         RETURNING 1
       )
       INSERT INTO nodes (node_id, name, lat, lon, iata, role, hardware_model, firmware_version, public_key, last_seen, is_online, network, last_mqtt_observer_seen_at, advert_count)
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
                              WHEN EXCLUDED.network = 'test' AND nodes.network IN ('ukmesh', 'northeast', 'teesside') THEN nodes.network
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
      await client.query('UPDATE nodes SET elevation_m = NULL WHERE node_id = $1', [nodeId]);
      await client.query('DELETE FROM node_coverage WHERE node_id = $1', [nodeId]);
    }

    await client.query('COMMIT');
    return {
      coordinatesChanged,
      advertCount: Number(upsert.rows[0]?.advert_count ?? 0),
    };
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
}): Promise<PacketBatchResult> {
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
  const companionSender = (() => {
    if (p.packetType !== 5 || !storedPayload) return null;
    const decrypted = storedPayload['decrypted'];
    if (!decrypted || typeof decrypted !== 'object' || Array.isArray(decrypted)) return null;
    const sender = (decrypted as Record<string, unknown>)['sender'];
    if (typeof sender !== 'string') return null;
    const normalized = sender.trim();
    return normalized.length > 0 ? normalized.slice(0, 256) : null;
  })();
  const network = p.network ?? 'ukmesh';
  const topicPrefix = p.topic.split('/', 1)[0]?.trim() || '';
  const iata = observerRegionFromTopic(p.topic);
  return enqueuePacket({
    time: new Date(),
    packetHash: p.packetHash,
    rxNodeId: p.rxNodeId ?? null,
    srcNodeId: p.srcNodeId ?? null,
    topic: p.topic,
    topicPrefix,
    iata,
    packetType: p.packetType ?? null,
    routeType: p.routeType ?? null,
    hopCount: p.hopCount ?? null,
    rssi: p.rssi ?? null,
    snr: p.snr ?? null,
    payloadJson: storedPayload ? JSON.stringify(storedPayload) : null,
    companionSender,
    rawHex: p.rawHex,
    advertCount: p.advertCount ?? null,
    pathHashes: p.pathHashes?.map((hash) => String(hash).trim().toUpperCase()) ?? null,
    pathHashSizeBytes: inferredPathHashSizeBytes,
    network,
    transportCodes: p.transportCodes ?? null,
    regionScope: p.regionScope ?? null,
  });
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
    `WITH sample_insert AS (
       INSERT INTO node_status_samples
       (time, node_id, network, battery_mv, uptime_secs, tx_air_secs, rx_air_secs, channel_utilization, air_util_tx, stats)
       VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9)
       RETURNING time
     )
     UPDATE nodes
        SET last_status_at = (SELECT time FROM sample_insert),
            last_seen = GREATEST(last_seen, (SELECT time FROM sample_insert)),
            is_online = TRUE
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
    ],
  );
}

export async function insertNodeNeighborSample(sample: {
  nodeId: string;
  network?: string;
  neighbors: unknown[];
}): Promise<void> {
  await pool.query(
    `INSERT INTO node_neighbor_samples (node_id, time, neighbors, network)
     VALUES ($1, NOW(), $2::jsonb, $3)`,
    [sample.nodeId, JSON.stringify(sample.neighbors), sample.network ?? 'ukmesh'],
  );
}

export async function getNodes(
  network?: string,
  observer?: string,
  fields: 'full' | 'slim' = 'full',
  signal?: AbortSignal,
) {
  const scope = buildScopePlaceholders(1, network, observer);
  const slimConditions = fields === 'slim'
    ? ` AND n.lat BETWEEN -90 AND 90
        AND n.lon BETWEEN -180 AND 180
        AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
        AND (n.role IS NULL OR n.role = 2)`
    : '';
  const whereClause = `WHERE 1=1${buildNodeScopeClause(scope, 'n')}${slimConditions}`;
  const optionalFields = fields === 'full'
    ? `, n.hardware_model, n.public_key, n.elevation_m`
    : ', n.elevation_m';
  const res = await query<{
    node_id: string;
    name: string | null;
    lat: number | null;
    lon: number | null;
    role: number | null;
  }>(
    `SELECT
       n.node_id,
       n.name,
       n.lat,
       n.lon,
       COALESCE(n.observer_iata, n.iata) AS iata,
       n.role,
       -- Advert/status writes, direct observer reception, and trustworthy
       -- multibyte relay evidence are independent proofs of presence. Always
       -- expose the newest proof; preferring observer metadata with COALESCE
       -- could make a freshly advertised node appear days older on the map.
       ${nodeEffectiveLastSeenSql('n')} AS last_seen,
       ${nodeEffectiveOnlineSql('n')} AS is_online,
       n.advert_count
       ${optionalFields}
     FROM node_identity_nodes n
     ${whereClause}
     ORDER BY ${nodeEffectiveLastSeenSql('n')} DESC`,
    scope.params,
    signal,
  );
  return res.rows;
}

export async function getNodeHistory(nodeId: string, hours = 24, network = 'ukmesh') {
  const scope = buildScopePlaceholders(3, network);
  const res = await pool.query(
    `SELECT p.time, p.packet_hash,
            COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id))) AS src_node_id,
            topic, packet_type, hop_count, rssi, snr, payload
     FROM packets p
     LEFT JOIN node_identity_aliases src_alias
       ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
     WHERE p.rx_node_id = ANY(${nodeAliasArraySql('$1')})
       AND p.time > NOW() - INTERVAL '1 hour' * $2
       ${buildPacketScopeClause(scope, 'p', network)}
       ${buildPublicPacketPrivacyClause('p')}
     ORDER BY time DESC LIMIT 500`,
    [nodeId, hours, ...scope.params]
  );
  return res.rows;
}

export async function getNodeAdverts(nodePublicKey: string, hours = 24, limit = 100, network = 'ukmesh') {
  const scope = buildScopePlaceholders(4, network);
  // Get location packets (packet_type = 4) where payload->>'publicKey' = this public key
  // Location packets are sent as part of the advert broadcast
  const res = await pool.query(
    `SELECT time, packet_hash
     FROM packets p
     WHERE p.packet_type = 4
       AND UPPER(BTRIM(COALESCE(p.src_node_id, p.payload->>'publicKey')))
             = ANY(${nodeAliasArraySql('$1')})
       AND p.time > NOW() - INTERVAL '1 hour' * $2
       ${buildPacketScopeClause(scope, 'p', network)}
       ${buildPublicPacketPrivacyClause('p')}
     ORDER BY time DESC
     LIMIT $3`,
    [nodePublicKey, hours, limit, ...scope.params]
  );
  return res.rows;
}

/**
 * Top adverting repeaters over a rolling window. Counts come from
 * node_counted_adverts (deduplicated per canonical advert hash, ~16k rows),
 * joined to the node identity view for display fields. Cheap enough to run
 * directly; the route wraps it in a 1h in-memory cache.
 */
export async function getTopAdvertingRepeaters(hours = 24, limit = 10, network = 'ukmesh') {
  const scope = buildScopePlaceholders(3, network);
  const res = await pool.query(
    `SELECT
       nca.node_id,
       n.name,
       COALESCE(n.observer_iata, n.iata) AS iata,
       n.role,
       n.lat,
       n.lon,
       ${nodeEffectiveLastSeenSql('n')} AS last_seen,
       ${nodeEffectiveOnlineSql('n')} AS is_online,
       n.advert_count,
       COUNT(*) AS adverts_in_window,
       MAX(nca.counted_at) AS last_advert_at
     FROM node_counted_adverts nca
     JOIN node_identity_nodes n ON n.node_id = nca.node_id
     WHERE nca.counted_at > NOW() - INTERVAL '1 hour' * $1
       AND n.role = 2
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       ${buildNodeScopeClause(scope, 'n')}
     GROUP BY nca.node_id, n.name, COALESCE(n.observer_iata, n.iata), n.role,
              n.lat, n.lon, ${nodeEffectiveLastSeenSql('n')},
              ${nodeEffectiveOnlineSql('n')}, n.advert_count
     ORDER BY adverts_in_window DESC, n.name ASC NULLS LAST
     LIMIT $2`,
    [hours, limit, ...scope.params]
  );
  return res.rows;
}

export async function getRecentPackets(
  limit = 200,
  network?: string,
  observer?: string,
  fields: 'full' | 'slim' = 'full',
  signal?: AbortSignal,
) {
  const scope = buildScopePlaceholders(2, network, observer);
  const fiveMinAgo = 'NOW() - INTERVAL \'5 minutes\'';
  const res = await query(
    `WITH recent_packets AS (
      SELECT DISTINCT ON (p.packet_hash)
             p.time, p.packet_hash,
             COALESCE(pa_rx.canonical_node_id, upper(btrim(p.rx_node_id))) AS rx_node_id,
             COALESCE(pa_src.canonical_node_id, upper(btrim(p.src_node_id))) AS src_node_id, p.topic,
             p.topic_prefix, p.iata, p.packet_type, p.route_type, p.hop_count, p.rssi, p.snr,
             COALESCE(p.payload->>'_summary', pd.summary) AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network, p.transport_codes, p.region_scope
      FROM packets p
      LEFT JOIN packet_decryptions pd ON pd.packet_hash = p.packet_hash
      LEFT JOIN node_identity_aliases pa_rx ON pa_rx.source_node_id = upper(btrim(p.rx_node_id))
      LEFT JOIN node_identity_aliases pa_src ON pa_src.source_node_id = upper(btrim(p.src_node_id))
      WHERE p.time > ${fiveMinAgo}
        ${buildPacketScopeClause(scope, 'p', network)}
        ${buildPublicPacketPrivacyClause('p')}
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
        ARRAY_AGG(DISTINCT COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id)))
                  ORDER BY COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id))))
          FILTER (WHERE packets.rx_node_id IS NOT NULL) AS observer_node_ids,
        ARRAY_AGG(DISTINCT iata ORDER BY iata) FILTER (WHERE NULLIF(TRIM(iata), '') IS NOT NULL) AS observer_iatas,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') <> 'tx')::int AS rx_count,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') = 'tx')::int AS tx_count
      FROM packets
      LEFT JOIN node_identity_aliases pa2 ON pa2.source_node_id = upper(btrim(packets.rx_node_id))
      WHERE packet_hash = ANY(SELECT packet_hash FROM recent_packets)
        AND time > ${fiveMinAgo}
        ${buildPacketScopeClause(scope, '', network)}
        ${buildPublicPacketPrivacyClause('packets')}
      GROUP BY packet_hash
    )
    SELECT
      rp.time, rp.packet_hash, rp.rx_node_id, rp.src_node_id,
      rp.packet_type, rp.hop_count, rp.rssi, rp.snr,
      rp.summary, rp.advert_count, rp.path_hashes, rp.path_hash_size_bytes,
      ${fields === 'full'
        ? 'rp.topic, rp.topic_prefix, rp.iata, rp.route_type, rp.network, rp.transport_codes, rp.region_scope,'
        : ''}
      ps.observer_node_ids, ps.observer_iatas, ps.rx_count, ps.tx_count
    FROM recent_packets rp
    LEFT JOIN packet_stats ps ON ps.packet_hash = rp.packet_hash
    ORDER BY rp.time DESC
    LIMIT $1`,
    [limit, ...scope.params],
    signal,
  );
  return res.rows;
}

/**
 * Fetch the last N GroupText (type=5) messages from the last 24 hours.
 * Used to pre-populate the channel feed on page load so it isn't blank.
 * Returns rows with the same shape as getRecentPackets.
 */
export async function getRecentMessages(
  limit = 50,
  network?: string,
  observer?: string,
  signal?: AbortSignal,
) {
  const scope = buildScopePlaceholders(2, network, observer);
  const res = await query(
    `WITH recent_msgs AS (
      SELECT DISTINCT ON (p.packet_hash)
             p.time, p.packet_hash,
             COALESCE(pa_rx.canonical_node_id, upper(btrim(p.rx_node_id))) AS rx_node_id,
             COALESCE(pa_src.canonical_node_id, upper(btrim(p.src_node_id))) AS src_node_id, p.topic,
             p.iata,
             p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
             COALESCE(p.payload->>'_summary', pd.summary) AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network
      FROM packets p
      LEFT JOIN packet_decryptions pd ON pd.packet_hash = p.packet_hash
      LEFT JOIN node_identity_aliases pa_rx ON pa_rx.source_node_id = upper(btrim(p.rx_node_id))
      LEFT JOIN node_identity_aliases pa_src ON pa_src.source_node_id = upper(btrim(p.src_node_id))
      WHERE p.packet_type = 5
        AND p.time > NOW() - INTERVAL '24 hours'
        ${buildPacketScopeClause(scope, 'p', network)}
        ${buildPublicPacketPrivacyClause('p')}
      ORDER BY p.packet_hash,
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               p.time DESC
    ),
    msg_stats AS (
      SELECT
        packet_hash,
        ARRAY_AGG(DISTINCT COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id)))
                  ORDER BY COALESCE(pa2.canonical_node_id, upper(btrim(packets.rx_node_id))))
          FILTER (WHERE packets.rx_node_id IS NOT NULL) AS observer_node_ids,
        ARRAY_AGG(DISTINCT iata ORDER BY iata) FILTER (WHERE NULLIF(TRIM(iata), '') IS NOT NULL) AS observer_iatas,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') <> 'tx')::int AS rx_count,
        COUNT(*) FILTER (WHERE COALESCE(payload->>'direction', 'rx') = 'tx')::int AS tx_count
      FROM packets
      LEFT JOIN node_identity_aliases pa2 ON pa2.source_node_id = upper(btrim(packets.rx_node_id))
      WHERE packet_hash = ANY(SELECT packet_hash FROM recent_msgs)
        AND time > NOW() - INTERVAL '24 hours'
        ${buildPacketScopeClause(scope, '', network)}
        ${buildPublicPacketPrivacyClause('packets')}
      GROUP BY packet_hash
    )
    SELECT
      m.time, m.packet_hash, m.rx_node_id, m.src_node_id, m.topic, m.iata,
      m.packet_type, m.hop_count, m.rssi, m.snr, m.payload,
      m.summary, m.advert_count, m.path_hashes, m.path_hash_size_bytes,
      ms.observer_node_ids, ms.observer_iatas, ms.rx_count, ms.tx_count
    FROM recent_msgs m
    LEFT JOIN msg_stats ms ON ms.packet_hash = m.packet_hash
    ORDER BY m.time DESC
    LIMIT $1`,
    [limit, ...scope.params],
    signal,
  );
  return res.rows;
}

/**
 * Fetch one channel's decrypted GroupText history. The WebSocket snapshot stays
 * intentionally recent and bounded; this demand-driven read reaches back 90
 * days so quiet channels can still fill the feed's 50-row view.
 */
export async function getChannelMessageHistory(
  channel: string,
  limit = 50,
  network?: string,
  observer?: string,
) {
  const normalizedChannel = channel.trim().toLowerCase();
  const channelHashes = channelHashesForName(normalizedChannel);
  const scope = buildScopePlaceholders(6, network, observer);
  // A historical packet may retain the ingest-time "[encrypted]" marker in
  // payload while packet_decryptions has the channel-specific summary. Prefer
  // the durable decryption summary for channel matching and display.
  const summaryExpr = `COALESCE(NULLIF(BTRIM(pd.summary), ''), NULLIF(BTRIM(p.payload->>'_summary'), ''))`;
  const historyWindow = `NOW() - INTERVAL '90 days'`;
  const historyBatchLimit = 100;
  const selectionLimit = Math.min(100, Math.max(limit, Math.ceil(limit * 1.5)));
  const selected = new Set<string>();
  let cursorTime = new Date();
  let cursorHash = '\uffff';

  while (selected.size < selectionLimit) {
    const batch = await analyticsQuery<{ packet_hash: string; time: Date }>(
      `SELECT p.time, p.packet_hash
       FROM packets p
       LEFT JOIN packet_decryptions pd ON pd.packet_hash = p.packet_hash
       WHERE p.packet_type = 5
         AND p.time > ${historyWindow}
         AND (
           p.time < $2::timestamptz
           OR (p.time = $2::timestamptz AND p.packet_hash < $3)
         )
         ${buildPacketScopeClause(scope, 'p', network)}
         -- Apply the complete privacy predicate after this bounded candidate
         -- page is selected. The path/private-prefix checks are intentionally
         -- not evaluated for every historical packet observation.
         AND p.visibility_ok IS TRUE
         AND p.is_private IS NOT TRUE
         AND (
           LOWER(${summaryExpr}) LIKE ('[' || $4 || ']%')
           OR LOWER(COALESCE(
             p.payload->>'channelHash',
             p.payload->'decrypted'->>'channelHash',
             pd.decrypted->>'channelHash',
             ''
           )) = ANY($5::text[])
         )
       ORDER BY p.time DESC, p.packet_hash DESC
       LIMIT $1`,
      [historyBatchLimit, cursorTime, cursorHash, normalizedChannel, channelHashes, ...scope.params],
    );
    if (batch.rows.length === 0) break;

    for (const row of batch.rows) {
      if (typeof row.packet_hash !== 'string' || selected.has(row.packet_hash)) continue;
      selected.add(row.packet_hash);
      if (selected.size >= selectionLimit) break;
    }

    const last = batch.rows.at(-1);
    if (!last || batch.rows.length < historyBatchLimit) break;
    cursorTime = last.time;
    cursorHash = last.packet_hash;
  }

  const selectedHashes = Array.from(selected.keys());
  if (selectedHashes.length === 0) return [];

  const detailScope = buildScopePlaceholders(2, network, observer);
  const res = await analyticsQuery(
    `WITH channel_candidates AS MATERIALIZED (
      SELECT DISTINCT ON (p.packet_hash)
             p.time, p.packet_hash,
             COALESCE(rx_alias.canonical_node_id, UPPER(BTRIM(p.rx_node_id))) AS rx_node_id,
             COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id))) AS src_node_id, p.topic,
             p.iata,
             p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
             ${summaryExpr} AS summary,
             p.advert_count, p.path_hashes, p.path_hash_size_bytes,
             p.network, p.visibility_ok, p.is_private
      FROM packets p
      LEFT JOIN packet_decryptions pd ON pd.packet_hash = p.packet_hash
      LEFT JOIN node_identity_aliases rx_alias
        ON rx_alias.source_node_id = UPPER(BTRIM(p.rx_node_id))
      LEFT JOIN node_identity_aliases src_alias
        ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
      WHERE p.packet_hash = ANY($1::text[])
        AND p.packet_type = 5
        AND p.time > ${historyWindow}
        ${buildPacketScopeClause(detailScope, 'p', network)}
        AND p.visibility_ok IS TRUE
        AND p.is_private IS NOT TRUE
      ORDER BY p.packet_hash,
               CASE WHEN ${summaryExpr} IS NOT NULL THEN 1 ELSE 0 END DESC,
               CASE WHEN p.payload ? 'decrypted' THEN 1 ELSE 0 END DESC,
               CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
               p.time DESC
    )
    SELECT
      m.time, m.packet_hash, m.rx_node_id, m.src_node_id, m.topic, m.iata,
      m.packet_type, m.hop_count, m.rssi, m.snr, m.payload,
      m.summary, m.advert_count, m.path_hashes, m.path_hash_size_bytes,
      CASE WHEN m.rx_node_id IS NOT NULL THEN ARRAY[m.rx_node_id]::text[] ELSE ARRAY[]::text[] END AS observer_node_ids,
      CASE WHEN NULLIF(TRIM(m.iata), '') IS NOT NULL THEN ARRAY[m.iata]::text[] ELSE ARRAY[]::text[] END AS observer_iatas,
      CASE WHEN COALESCE(m.payload->>'direction', 'rx') <> 'tx' THEN 1 ELSE 0 END::int AS rx_count,
      CASE WHEN COALESCE(m.payload->>'direction', 'rx') = 'tx' THEN 1 ELSE 0 END::int AS tx_count
    FROM channel_candidates m
    WHERE 1 = 1
      ${buildPublicPacketPrivacyClause('m')}
    ORDER BY m.time DESC`,
    [selectedHashes, ...detailScope.params],
  );
  const channelPrefix = `[${normalizedChannel}]`;
  return res.rows.slice(0, limit).map((row) => {
    const summary = typeof row.summary === 'string' ? row.summary.trim() : '';
    if (summary.toLowerCase().startsWith(channelPrefix)) return row;
    // A channel-hash match can carry the generic live "[encrypted]" marker;
    // normalize it to the selected channel so the existing feed scope matcher
    // includes the row without changing the live packet path.
    return { ...row, summary: `${channelPrefix}${summary ? ` ${summary}` : ''}` };
  });
}

export async function getRecentPacketEvents(limit = 200, network?: string, observer?: string) {
  const scope = buildScopePlaceholders(2, network, observer);
  const params: unknown[] = [limit, ...scope.params];
  const res = await pool.query(
    `SELECT
        p.time, p.packet_hash,
        COALESCE(rx_alias.canonical_node_id, UPPER(BTRIM(p.rx_node_id))) AS rx_node_id,
        COALESCE(src_alias.canonical_node_id, UPPER(BTRIM(p.src_node_id))) AS src_node_id,
        p.topic, p.iata,
        p.packet_type, p.hop_count, p.rssi, p.snr, p.payload,
        p.payload->>'_summary' AS summary,
        p.advert_count, p.path_hashes, p.path_hash_size_bytes
     FROM packets p
     LEFT JOIN node_identity_aliases rx_alias
       ON rx_alias.source_node_id = UPPER(BTRIM(p.rx_node_id))
     LEFT JOIN node_identity_aliases src_alias
       ON src_alias.source_node_id = UPPER(BTRIM(p.src_node_id))
     WHERE p.time > NOW() - INTERVAL '24 hours'
         ${buildPacketScopeClause(scope, 'p', network)}
         ${buildPublicPacketPrivacyClause('p')}
     ORDER BY p.time DESC
     LIMIT $1`,
    params,
  );
  return res.rows;
}

export async function getPacketDetail(hash: string, network = 'ukmesh') {
  const scope = buildScopePlaceholders(2, network);
  const [primary, observations] = await Promise.all([
    pool.query(
      `SELECT p.time, p.packet_hash, p.rx_node_id, p.src_node_id, p.topic, p.iata,
              p.packet_type, p.route_type, p.hop_count, p.rssi, p.snr,
              p.payload, p.path_hashes, p.path_hash_size_bytes, p.raw_hex
       FROM packets p
       WHERE p.packet_hash = $1
         ${buildPacketScopeClause(scope, 'p', network)}
         ${buildPublicPacketPrivacyClause('p')}
       ORDER BY
         CASE WHEN p.src_node_id IS NOT NULL THEN 1 ELSE 0 END DESC,
         CASE WHEN p.raw_hex IS NOT NULL THEN 1 ELSE 0 END DESC,
         p.time DESC
       LIMIT 1`,
      [hash, ...scope.params],
    ),
    pool.query(
      `SELECT COALESCE(rx_alias.canonical_node_id, UPPER(BTRIM(p.rx_node_id))) AS rx_node_id,
              p.iata, p.time, p.rssi, p.snr, p.hop_count
       FROM packets p
       LEFT JOIN node_identity_aliases rx_alias
         ON rx_alias.source_node_id = UPPER(BTRIM(p.rx_node_id))
       WHERE p.packet_hash = $1
         ${buildPacketScopeClause(scope, 'p', network)}
         ${buildPublicPacketPrivacyClause('p')}
       ORDER BY p.time ASC`,
      [hash, ...scope.params],
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
    iata: row.iata as string | null,
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
      iata: r.iata as string | null,
      time: r.time as Date,
      rssi: r.rssi as number | null,
      snr: r.snr as number | null,
      hopCount: r.hop_count as number | null,
    })),
  };
}

export async function getPublicVisibilityGeneration(signal?: AbortSignal): Promise<number> {
  const result = await query<{ generation: string }>(
    `SELECT generation::text AS generation
       FROM public_visibility_state
      WHERE singleton = TRUE`,
    undefined,
    signal,
  );
  const generation = Number(result.rows[0]?.generation);
  if (!Number.isSafeInteger(generation) || generation < 1) {
    throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE');
  }
  return generation;
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

  // Two simple statements joined in JS. A single JOIN blob makes the planner
  // guess rows=1 on the scoped-node CTE (the sightings EXISTS is not
  // estimable) and pick nested loops that re-materialise the identity view
  // per row: 100s+. Splitting keeps both statements simple enough that the
  // planner cannot mis-estimate them (measured: ~325ms + ~92ms).
  const [nodesRes, linksRes] = await Promise.all([
    pool.query<{
      node_id: string;
      lat: number | null;
      lon: number | null;
      name: string | null;
    }>(
      `SELECT node_id, lat, lon, name
       FROM node_identity_nodes n
       WHERE 1=1${buildNodeScopeClause(scope, 'n')}`,
      params,
    ),
    pool.query<{
      node_a_id: string;
      node_b_id: string;
      multibyte_observed_count: number;
    }>(
      `SELECT node_a_id, node_b_id, multibyte_observed_count
       FROM node_identity_links
       WHERE multibyte_observed_count > 0
         AND itm_viable = true`,
    ),
  ]);

  const nodesById = new Map(nodesRes.rows.map((row) => [row.node_id, row]));
  const segments: Array<{
    positions: [[number, number], [number, number]];
    count: number;
  }> = [];
  for (const link of linksRes.rows) {
    const a = nodesById.get(link.node_a_id);
    const b = nodesById.get(link.node_b_id);
    if (!a || !b) continue;
    if (a.lat == null || a.lon == null || b.lat == null || b.lon == null) continue;
    if (a.lat < -90 || a.lat > 90 || b.lat < -90 || b.lat > 90) continue;
    if (a.lon < -180 || a.lon > 180 || b.lon < -180 || b.lon > 180) continue;
    if (Math.abs(a.lat) < 5 && Math.abs(a.lon) < 5) continue;
    if (Math.abs(b.lat) < 5 && Math.abs(b.lon) < 5) continue;
    if ((a.name ?? '').includes('🚫') || (b.name ?? '').includes('🚫')) continue;
    const distKm = Math.sqrt(
      Math.pow((a.lat - b.lat) * 111, 2)
        + Math.pow(
          (a.lon - b.lon) * 111 * Math.cos(((a.lat + b.lat) / 2) * (Math.PI / 180)),
          2,
        ),
    );
    if (distKm > MAX_MULTIBYTE_PATH_SEGMENT_KM) continue;
    segments.push({
      positions: [
        [a.lat, a.lon],
        [b.lat, b.lon],
      ],
      count: link.multibyte_observed_count,
    });
  }
  segments.sort((x, y) => y.count - x.count);

  const maxCount = segments.reduce((max, segment) => Math.max(max, segment.count), 0);
  return { maxCount, segments };
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
export async function getViableLinks(
  network?: string,
  observer?: string,
  signal?: AbortSignal,
): Promise<ViableLinkRow[]> {
  if (network && !observer) {
    const scopedNetworks = network === 'ukmesh' ? UKMESH_NETWORKS : [network];
    // Keep the identity scope and link scan as separate statements. The
    // identity view's representative-rank predicate is estimated at one row;
    // combining it with both link endpoints makes PostgreSQL form the scoped
    // node Cartesian product and perform ~174M composite-index probes for
    // ukmesh. Each simple statement is fast even with that bad estimate, and
    // the in-memory intersection preserves the exact endpoint scope.
    const [nodesRes, linksRes] = await Promise.all([
      query<{ node_id: string }>(
        `SELECT DISTINCT node_id
         FROM node_identity_nodes
         WHERE network = ANY($1::text[])
           AND (name IS NULL OR name NOT LIKE '%🚫%')`,
        [scopedNetworks],
        signal,
      ),
      query<ViableLinkRow>(
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
         FROM node_identity_links nl
         LEFT JOIN (
           SELECT node_a_id, node_b_id,
             SUM(sample_count)::int AS neighbor_report_count,
             MAX(best_snr_db) AS neighbor_best_snr_db
           FROM node_identity_link_radio_reports
           GROUP BY node_a_id, node_b_id
         ) nr ON nr.node_a_id = nl.node_a_id AND nr.node_b_id = nl.node_b_id
         WHERE (nl.itm_viable = true OR nl.force_viable = true)`,
        undefined,
        signal,
      ),
    ]);
    const scopedNodeIds = new Set(nodesRes.rows.map((row) => row.node_id));
    return linksRes.rows.filter((link) => (
      scopedNodeIds.has(link.node_a_id) && scopedNodeIds.has(link.node_b_id)
    ));
  }

  const scope = buildScopePlaceholders(1, network, observer);
  const params: unknown[] = [...scope.params];

  const res = await query<ViableLinkRow>(
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
     FROM node_identity_links nl
     LEFT JOIN (
       SELECT node_a_id, node_b_id,
         SUM(sample_count)::int AS neighbor_report_count,
         MAX(best_snr_db) AS neighbor_best_snr_db
       FROM node_identity_link_radio_reports
       GROUP BY node_a_id, node_b_id
     ) nr ON nr.node_a_id = nl.node_a_id AND nr.node_b_id = nl.node_b_id
     JOIN node_identity_nodes a ON a.node_id = nl.node_a_id
     JOIN node_identity_nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       AND (a.name IS NULL OR a.name NOT LIKE '%🚫%')
       AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')
       ${buildNodeScopeClause(scope, 'a')}
       ${buildNodeScopeClause(scope, 'b')}`,
    params,
    signal,
  );
  return res.rows;
}

export { pool, analyticsPool };

export async function closeDb(): Promise<void> {
  await Promise.all([pool.end(), analyticsPool.end()]);
}

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
     ON CONFLICT (network, src_node_id) DO UPDATE SET
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

export async function replaceSpamSuspects(
  sourceNetworks: readonly string[],
  suspects: SpamSuspectRow[],
): Promise<void> {
  if (sourceNetworks.length === 0) throw new Error('SPAM_SOURCE_SCOPE_REQUIRED');
  assertSpamSuspectScope(sourceNetworks, suspects);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM spam_suspects WHERE network = ANY($1)', [sourceNetworks]);
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

export function assertSpamSuspectScope(
  sourceNetworks: readonly string[],
  suspects: ReadonlyArray<Pick<SpamSuspectRow, 'network'>>,
): void {
  const allowed = new Set(sourceNetworks);
  if (suspects.some((suspect) => !allowed.has(suspect.network))) {
    throw new Error('SPAM_SUSPECT_OUT_OF_SCOPE');
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
  const clauses = [
    `ss.time > NOW() - ($1 * INTERVAL '1 hour')`,
    `ss.network = ANY($2)`,
    `ss.spoofed_name NOT LIKE '%🚫%'`,
    `NOT EXISTS (
       SELECT 1
       FROM nodes private_node
       WHERE private_node.node_id = ss.src_node_id
         AND private_node.name LIKE '%🚫%'
     )`,
  ];

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
  const params: unknown[] = [queryOptions.hours ?? 48, UKMESH_NETWORKS];
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
       AND p.network = f.network
       AND p.time > NOW() - INTERVAL '1 hour' * $1
     GROUP BY f.src_node_id, f.spoofed_name, f.public_key, f.claimed_lat, f.claimed_lon,
              f.canonical_key, f.verdict, f.signals, f.total_score, f.network, f.time, f.first_seen
     ORDER BY f.total_score DESC, f.time DESC`,
    params
  );
  return res.rows;
}

export async function getSpamSuspectSummary(options: SpamSuspectQueryOptions = {}) {
  const params: unknown[] = [options.hours ?? 48, UKMESH_NETWORKS];
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
       AND ss.network = p.network
     WHERE p.src_node_id = $1
       AND p.network = ANY($2)
       AND COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
       AND p.packet_type = 4
       AND p.time > NOW() - INTERVAL '30 days'
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       AND (ss.spoofed_name IS NULL OR ss.spoofed_name NOT LIKE '%🚫%')
       AND NOT EXISTS (
         SELECT 1 FROM nodes source_node
         WHERE source_node.node_id = p.src_node_id
           AND source_node.name LIKE '%🚫%'
       )
     ORDER BY p.rx_node_id, p.hop_count ASC NULLS LAST, p.time ASC`,
    [srcNodeId, UKMESH_NETWORKS]
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
       AND p.network = ss.network
       AND p.packet_type  = 4
       AND p.time > NOW() - INTERVAL '30 days'
       AND p.rx_node_id   IS NOT NULL
     JOIN nodes n ON n.node_id = p.rx_node_id
     WHERE ss.claimed_lat IS NOT NULL
       AND ss.claimed_lon IS NOT NULL
       AND ss.network = ANY($1)
       AND COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
       AND n.lat IS NOT NULL
       AND n.lon IS NOT NULL
       AND ss.spoofed_name NOT LIKE '%🚫%'
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       AND NOT EXISTS (
         SELECT 1 FROM nodes source_node
         WHERE source_node.node_id = ss.src_node_id
           AND source_node.name LIKE '%🚫%'
       )
     GROUP BY
       ss.src_node_id, ss.claimed_lat, ss.claimed_lon, ss.spoofed_name,
       p.rx_node_id, n.name, n.lat, n.lon
     ORDER BY ss.src_node_id, MIN(p.hop_count) ASC NULLS LAST`
  , [UKMESH_NETWORKS]);
  return res.rows;
}
