import pg from 'pg';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
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

export async function initDb(): Promise<void> {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const schemaPath = path.join(__dirname, 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  await pool.query(sql);
  console.log('[db] schema initialised, no retention policy (data kept indefinitely)');
}

export async function incrementAdvertCount(nodeId: string): Promise<number> {
  const res = await pool.query<{ advert_count: number }>(
    `UPDATE nodes SET advert_count = advert_count + 1 WHERE node_id = $1 RETURNING advert_count`,
    [nodeId]
  );
  return res.rows[0]?.advert_count ?? 1;
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
}): Promise<void> {
  await pool.query(
    `INSERT INTO nodes (node_id, name, lat, lon, iata, role, hardware_model, firmware_version, public_key, last_seen, is_online)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), TRUE)
     ON CONFLICT (node_id) DO UPDATE SET
       name             = COALESCE(EXCLUDED.name, nodes.name),
       lat              = COALESCE(NULLIF(EXCLUDED.lat, 0), nodes.lat),
       lon              = COALESCE(NULLIF(EXCLUDED.lon, 0), nodes.lon),
       iata             = COALESCE(EXCLUDED.iata, nodes.iata),
       role             = COALESCE(EXCLUDED.role, nodes.role),
       hardware_model   = COALESCE(EXCLUDED.hardware_model, nodes.hardware_model),
       firmware_version = COALESCE(EXCLUDED.firmware_version, nodes.firmware_version),
       public_key       = COALESCE(EXCLUDED.public_key, nodes.public_key),
       last_seen        = NOW(),
       is_online        = TRUE`,
    [nodeId, updates.name, updates.lat, updates.lon, updates.iata, updates.role,
     updates.hardwareModel, updates.firmwareVersion, updates.publicKey]
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
  rawHex: string;
  advertCount?: number;
  pathHashes?: string[];
}): Promise<void> {
  await pool.query(
    `INSERT INTO packets
       (time, packet_hash, rx_node_id, src_node_id, topic, packet_type, route_type,
        hop_count, rssi, snr, payload, raw_hex, advert_count, path_hashes)
     VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
    [p.packetHash, p.rxNodeId, p.srcNodeId, p.topic, p.packetType,
     p.routeType, p.hopCount, p.rssi, p.snr,
     p.payload ? JSON.stringify(p.payload) : null, p.rawHex, p.advertCount ?? null,
     p.pathHashes ?? null]
  );
}

export async function getNodes() {
  const res = await pool.query(
    `SELECT node_id, name, lat, lon, iata, role, last_seen, is_online, hardware_model, public_key, advert_count, elevation_m
     FROM nodes ORDER BY last_seen DESC`
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

export async function getRecentPackets(limit = 200) {
  const res = await pool.query(
    `SELECT time, packet_hash, rx_node_id, src_node_id, topic,
            packet_type, hop_count, rssi, snr, payload
     FROM packets
     WHERE time > NOW() - INTERVAL '5 minutes'
     ORDER BY time DESC LIMIT $1`,
    [limit]
  );
  return res.rows;
}

export async function getLastNPackets(n: number) {
  // DISTINCT ON deduplicates by hash (same packet heard by multiple observers),
  // picking the most recent observation per hash within the last 24 hours.
  const res = await pool.query(
    `SELECT * FROM (
       SELECT DISTINCT ON (packet_hash) time, packet_hash, rx_node_id, src_node_id,
              packet_type, hop_count, payload, advert_count, path_hashes
       FROM packets
       WHERE time > NOW() - INTERVAL '24 hours'
       ORDER BY packet_hash, time DESC
     ) deduped
     ORDER BY time DESC LIMIT $1`,
    [n]
  );
  return res.rows;
}

/** Minimum observations required before a link is considered confirmed. */
export const MIN_LINK_OBSERVATIONS = 5;

/** Returns only confirmed viable link pairs — compact for sending in initial WebSocket state. */
export async function getViableLinkPairs(): Promise<[string, string][]> {
  const res = await pool.query<{ node_a_id: string; node_b_id: string }>(
    `SELECT node_a_id, node_b_id FROM node_links
     WHERE (itm_viable = true OR force_viable = true) AND observed_count >= $1`,
    [MIN_LINK_OBSERVATIONS],
  );
  return res.rows.map((r) => [r.node_a_id, r.node_b_id]);
}

export { pool };
