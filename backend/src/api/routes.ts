import { Request, Response, Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { isIP } from 'node:net';
import mqtt from 'mqtt';
import { getNodes, getNodeHistory, getNodeAdverts, getPathHistoryCache, getRecentPacketEvents, getRecentPackets, query, MIN_LINK_OBSERVATIONS } from '../db/index.js';
import { addOwnerNodeForUsername, getBestNodeForMqttUsername, getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { getWorkerHealthOverview } from '../health/status.js';
import { resolveRequestNetwork } from '../http/requestScope.js';
import { getResolveCache, setResolveCache } from '../path-beta/resolveCache.js';
import { resolvePool } from '../path-beta/resolvePool.js';

const router = Router();
const OWNER_COOKIE_NAME = 'meshcore_owner_session';
const OWNER_LIVE_CACHE_TTL_MS = 5_000;
const ownerLiveCache = new Map<string, { ts: number; data: unknown }>();

// Server-side response caches — shared across all clients for the same scope.
// Reduces repeated DB hits when multiple browser tabs / users poll simultaneously.
const STATS_CACHE_TTL_MS          = 15_000;  // stats don't need sub-second freshness
const INFERRED_NODES_CACHE_TTL_MS = 60_000;  // 7-day packet scan, changes slowly
const PATH_HISTORY_CACHE_TTL_MS   = 60_000;  // history cache is rebuilt by worker, not real-time
const COVERAGE_CACHE_TTL_MS       = 30_000;  // geometry changes only on coverage rebuild
const CHARTS_CACHE_TTL_MS         = 30 * 60_000; // 30 min — background refresh keeps it warm
const statsCache         = new Map<string, { ts: number; data: unknown }>();
const inferredNodesCache = new Map<string, { ts: number; data: unknown }>();
const pathHistoryCache   = new Map<string, { ts: number; data: unknown }>();
const coverageCache      = new Map<string, { ts: number; data: unknown }>();
const chartsCache        = new Map<string, { ts: number; data: unknown }>();
const chartsInflight     = new Map<string, Promise<unknown>>();
const OWNER_SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const MQTT_USERNAME_MAX_LEN = 128;
const MQTT_PASSWORD_MAX_LEN = 128;
const OWNER_LOGIN_LIMITER = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 8,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many login attempts, try again in 15 minutes' },
});
const PATH_BETA_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many path requests, slow down' },
});
const PATH_HISTORY_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many history requests, slow down' },
});
const COVERAGE_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 12,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many coverage requests, slow down' },
});
const PATH_LEARNING_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many path learning requests, slow down' },
});
const EXPENSIVE_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 12,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, slow down' },
});
const STATS_CHARTS_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 12,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many stats chart requests, slow down' },
});
const PROHIBITED_NODE_MARKER = '🚫';
const HIDDEN_NODE_MASK_RADIUS_MILES = 1;

type OwnerSession = {
  nodeIds: string[];
  exp: number;
  mqttUsername?: string;
};

function getOwnerCookieKey(): Buffer {
  const secret = process.env['OWNER_COOKIE_SECRET'];
  if (!secret) throw new Error('OWNER_COOKIE_SECRET environment variable is not set');
  return createHash('sha256').update(secret).digest();
}

function encryptOwnerSession(payload: OwnerSession): string {
  const iv = randomBytes(12);
  const key = getOwnerCookieKey();
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const plaintext = Buffer.from(JSON.stringify(payload), 'utf8');
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64url')}.${tag.toString('base64url')}.${encrypted.toString('base64url')}`;
}

function decryptOwnerSession(token: string): OwnerSession | null {
  try {
    const [ivB64, tagB64, ciphertextB64] = token.split('.');
    if (!ivB64 || !tagB64 || !ciphertextB64) return null;
    const iv = Buffer.from(ivB64, 'base64url');
    const tag = Buffer.from(tagB64, 'base64url');
    const ciphertext = Buffer.from(ciphertextB64, 'base64url');
    const key = getOwnerCookieKey();
    const decipher = createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    const decoded = Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString('utf8');
    const parsed = JSON.parse(decoded) as Partial<OwnerSession>;
    if (!Array.isArray(parsed.nodeIds) || typeof parsed.exp !== 'number') return null;
    const nodeIds = parsed.nodeIds
      .map((value) => String(value).trim().toLowerCase())
      .filter((value) => /^[0-9a-f]{64}$/.test(value));
    if (nodeIds.length < 1) return null;
    const mqttUsername = typeof parsed.mqttUsername === 'string' ? parsed.mqttUsername.trim() : undefined;
    return { nodeIds, exp: parsed.exp, mqttUsername: mqttUsername || undefined };
  } catch {
    return null;
  }
}

function readCookieValue(cookieHeader: string | undefined, key: string): string | null {
  if (!cookieHeader) return null;
  const parts = cookieHeader.split(';');
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed.startsWith(`${key}=`)) continue;
    return decodeURIComponent(trimmed.slice(key.length + 1));
  }
  return null;
}

function hasControlChars(value: string): boolean {
  return /[\u0000-\u001F\u007F]/.test(value);
}

function isProhibitedMapNode(node: { name?: string | null } | null | undefined): boolean {
  return Boolean(node?.name?.includes(PROHIBITED_NODE_MARKER));
}

function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededUnitPair(seed: string): [number, number] {
  const distanceUnit = hashSeed(`${seed}:distance`) / 0xffffffff;
  const bearingUnit = hashSeed(`${seed}:bearing`) / 0xffffffff;
  return [distanceUnit, bearingUnit];
}

function stablePointWithinMiles(
  lat: number,
  lon: number,
  seed: string,
  radiusMiles = HIDDEN_NODE_MASK_RADIUS_MILES,
): [number, number] {
  const radiusKm = radiusMiles * 1.609344;
  const [distanceUnit, bearingUnit] = seededUnitPair(seed);
  const distanceKm = Math.sqrt(distanceUnit) * radiusKm;
  const bearing = bearingUnit * Math.PI * 2;
  const latRad = lat * (Math.PI / 180);
  const dLat = (distanceKm / 111) * Math.cos(bearing);
  const lonScale = Math.max(0.01, Math.cos(latRad));
  const dLon = (distanceKm / (111 * lonScale)) * Math.sin(bearing);
  return [lat + dLat, lon + dLon];
}

function maskDecodedPathNodes(
  rawNodes: Array<{
    ord: number;
    node_id: string | null;
    name: string | null;
    lat: number | null;
    lon: number | null;
    last_seen?: string | null;
  }> | null | undefined,
): Array<{
  ord: number;
  node_id: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
}> {
  if (!Array.isArray(rawNodes)) return [];
  return rawNodes.map((node) => {
    if (!node || typeof node !== 'object') return node;
    if (!isProhibitedMapNode(node)) {
      return {
        ord: Number(node.ord ?? 0),
        node_id: node.node_id ?? null,
        name: node.name ?? null,
        lat: node.lat ?? null,
        lon: node.lon ?? null,
      };
    }
    if (typeof node.lat !== 'number' || typeof node.lon !== 'number') {
      return {
        ord: Number(node.ord ?? 0),
        node_id: node.node_id ?? null,
        name: 'Redacted repeater',
        lat: node.lat ?? null,
        lon: node.lon ?? null,
      };
    }
    const activityKey = node.last_seen ?? 'unknown';
    const seed = `${node.node_id ?? 'unknown'}|${activityKey}`;
    const [maskedLat, maskedLon] = stablePointWithinMiles(node.lat, node.lon, seed);
    return {
      ord: Number(node.ord ?? 0),
      node_id: node.node_id ?? null,
      name: 'Redacted repeater',
      lat: maskedLat,
      lon: maskedLon,
    };
  });
}

function parseOwnerMqttUsernameMap(): Map<string, string[]> {
  const raw = String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? '').trim();
  const map = new Map<string, string[]>();
  if (!raw) return map;

  for (const entry of raw.split(',')) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx <= 0) continue;
    const username = trimmed.slice(0, eqIdx).trim();
    const rawNodes = trimmed.slice(eqIdx + 1).trim();
    if (!username || !rawNodes) continue;
    const nodeIds = rawNodes
      .split('|')
      .map((nodeId) => nodeId.trim().toLowerCase())
      .filter((nodeId) => /^[0-9a-f]{64}$/.test(nodeId));
    if (nodeIds.length < 1) continue;
    map.set(username, Array.from(new Set(nodeIds)));
  }
  return map;
}

async function resolveOwnerNodeIds(mqttUsername: string): Promise<string[]> {
  const databaseNodeIds = await getOwnerNodeIdsForUsername(mqttUsername);
  if (databaseNodeIds.length > 0) return databaseNodeIds;
  const legacyMap = parseOwnerMqttUsernameMap();
  return legacyMap.get(mqttUsername) ?? [];
}

async function autoLinkOwnerNodeIds(mqttUsername: string): Promise<string[]> {
  const existing = await resolveOwnerNodeIds(mqttUsername);
  if (existing.length > 0) return existing;

  // Look up the most recently connected node for this MQTT login.
  // Populated by the connection monitor that tails the Mosquitto log.
  const nodeId = await getBestNodeForMqttUsername(mqttUsername);
  if (!nodeId) return [];

  await addOwnerNodeForUsername(mqttUsername, nodeId);
  return [nodeId];
}

function verifyMqttCredentials(mqttUsername: string, mqttPassword: string): Promise<boolean> {
  const brokerUrl = String(process.env['MQTT_BROKER_URL'] ?? 'ws://mosquitto:9001');
  const clientId = `owner-auth-${randomBytes(6).toString('hex')}`;
  const client = mqtt.connect(brokerUrl, {
    username: mqttUsername,
    password: mqttPassword,
    reconnectPeriod: 0,
    connectTimeout: 5_000,
    clean: true,
    clientId,
  });

  return new Promise((resolve) => {
    let done = false;
    const finish = (ok: boolean) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      client.removeAllListeners();
      client.end(true);
      resolve(ok);
    };
    const timer = setTimeout(() => finish(false), 6_000);
    client.once('connect', () => finish(true));
    client.once('error', () => finish(false));
    client.once('close', () => finish(false));
  });
}

function isSecureRequest(req: { secure: boolean; headers: Record<string, string | string[] | undefined> }): boolean {
  if (req.secure) return true;
  const proto = String(req.headers['x-forwarded-proto'] ?? '').toLowerCase();
  return proto === 'https';
}

async function buildOwnerDashboard(nodeIds: string[]) {
  if (nodeIds.length < 1) {
    return {
      nodes: [],
      totals: {
        ownedNodes: 0,
        packets24h: 0,
        packets7d: 0,
        packetsReceived24h: 0,
      },
      roadmap: [
        'Per-node packet history for owner nodes',
        'Advert and heartbeat trend views',
        'RSSI and SNR trend views from observer reports',
        'Node placement planner (coming next)',
      ],
    };
  }

  const [ownedNodes, packetSummary, rxSummary] = await Promise.all([
    query<{
      node_id: string;
      name: string | null;
      network: string;
      last_seen: string | null;
      advert_count: number | null;
      lat: number | null;
      lon: number | null;
      iata: string | null;
    }>(
      `SELECT node_id, name, network, last_seen, advert_count, lat, lon, iata
       FROM nodes
       WHERE LOWER(node_id) = ANY($1::text[])
       ORDER BY last_seen DESC NULLS LAST`,
      [nodeIds],
    ),
    query<{ packets_24h: number; packets_7d: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours')::int AS packets_24h,
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '7 days')::int AS packets_7d
       FROM packets
       WHERE LOWER(src_node_id) = ANY($1::text[])`,
      [nodeIds],
    ),
    query<{ packets_24h: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours')::int AS packets_24h
       FROM packets
       WHERE LOWER(rx_node_id) = ANY($1::text[])`,
      [nodeIds],
    ),
  ]);

  return {
    nodes: ownedNodes.rows.map((row) => ({
      ...row,
      last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
      advert_count: Number(row.advert_count ?? 0),
    })),
    totals: {
      ownedNodes: ownedNodes.rows.length,
      packets24h: Number(packetSummary.rows[0]?.packets_24h ?? 0),
      packets7d: Number(packetSummary.rows[0]?.packets_7d ?? 0),
      packetsReceived24h: Number(rxSummary.rows[0]?.packets_24h ?? 0),
    },
    roadmap: [
      'Per-node packet history for owner nodes',
      'Advert and heartbeat trend views',
      'RSSI and SNR trend views from observer reports',
      'Node placement planner (coming next)',
    ],
  };
}

function getOwnerSession(req: Request): OwnerSession | null {
  const token = readCookieValue(req.headers.cookie, OWNER_COOKIE_NAME);
  if (!token) return null;
  const session = decryptOwnerSession(token);
  if (!session || session.exp <= Date.now()) return null;
  return session;
}

async function requireOwnerSession(req: Request, res: Response): Promise<string[] | null> {
  const session = getOwnerSession(req);
  if (!session) {
    res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
    res.status(401).json({ error: 'Not logged in' });
    return null;
  }
  return session.nodeIds;
}

type NetworkFilters = {
  params: string[];
  packets: string;
  packetsAlias: (alias: string) => string;
  nodes: string;
  nodesAlias: (alias: string) => string;
};

function normalizeObserverQuery(value: unknown): string | undefined {
  const observer = String(value ?? '').trim().toLowerCase();
  return observer && /^[0-9a-f]{64}$/.test(observer) ? observer : undefined;
}

function normalizeIp(value: string | undefined): string {
  const raw = String(value ?? '').trim();
  if (!raw) return '';
  const first = raw.split(',')[0]?.trim() ?? '';
  if (first.startsWith('::ffff:')) return first.slice(7);
  return first;
}

function isPrivateClientIp(ip: string): boolean {
  const normalized = normalizeIp(ip);
  if (!normalized) return false;
  if (normalized === '::1' || normalized === '127.0.0.1') return true;
  if (normalized.startsWith('10.')) return true;
  if (normalized.startsWith('192.168.')) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(normalized)) return true;
  if (/^(fc|fd)/i.test(normalized)) return true;
  if (/^fe80:/i.test(normalized)) return true;
  return false;
}

function requireLocalOnly(req: Request, res: Response): boolean {
  const candidates = [
    req.ip,
    normalizeIp(String(req.headers['cf-connecting-ip'] ?? '')),
    normalizeIp(String(req.headers['x-forwarded-for'] ?? '')),
    normalizeIp(req.socket.remoteAddress ?? ''),
  ].filter(Boolean) as string[];

  if (candidates.some((ip) => isPrivateClientIp(ip) || isIP(ip) === 0 && ip === 'localhost')) {
    return true;
  }

  res.status(403).json({ error: 'Local access only' });
  return false;
}

function networkFilters(network?: string, observer?: string): NetworkFilters {
  const params: string[] = [];
  let networkParam: string | null = null;
  let observerParam: string | null = null;

  if (network) {
    networkParam = `$${params.length + 1}`;
    params.push(network);
  }

  if (observer) {
    observerParam = `$${params.length + 1}`;
    params.push(observer);
  }

  const packetConditions: string[] = [];
  if (networkParam) packetConditions.push(`network = ${networkParam}`);
  else {
    packetConditions.push(`network IS DISTINCT FROM 'test'`);
    packetConditions.push(`LOWER(COALESCE(rx_node_id, '')) NOT IN (SELECT LOWER(node_id) FROM nodes WHERE network = 'test')`);
  }
  if (observerParam) packetConditions.push(`LOWER(rx_node_id) = LOWER(${observerParam})`);

  const nodeConditions = (alias?: string) => {
    const prefix = alias ? `${alias}.` : '';
    const conditions: string[] = [];
    if (networkParam) conditions.push(`${prefix}network = ${networkParam}`);
    else conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    if (observerParam) {
      conditions.push(
        `(
          LOWER(${prefix}node_id) = LOWER(${observerParam})
          OR EXISTS (
            SELECT 1
            FROM packets p
            WHERE LOWER(p.rx_node_id) = LOWER(${observerParam})
              ${networkParam ? `AND p.network = ${networkParam}` : ''}
              AND LOWER(p.src_node_id) = LOWER(${prefix}node_id)
          )
        )`,
      );
    }
    return conditions;
  };

  return {
    params,
    packets: packetConditions.length > 0 ? `AND ${packetConditions.join(' AND ')}` : '',
    packetsAlias: (alias: string) => {
      const prefix = `${alias}.`;
      const conditions: string[] = [];
      if (networkParam) {
        conditions.push(`${prefix}network = ${networkParam}`);
        conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
      } else {
        conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
        conditions.push(`split_part(${prefix}topic, '/', 1) <> 'meshcore-test'`);
        conditions.push(`LOWER(COALESCE(${prefix}rx_node_id, '')) NOT IN (SELECT LOWER(node_id) FROM nodes WHERE network = 'test')`);
      }
      if (observerParam) conditions.push(`LOWER(${prefix}rx_node_id) = LOWER(${observerParam})`);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
    nodes: nodeConditions().length > 0 ? `AND ${nodeConditions().join(' AND ')}` : '',
    nodesAlias: (alias: string) => {
      const conditions = nodeConditions(alias);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
  };
}

type InferredMultibyteNode = {
  node_id: string;
  name: string;
  lat: number;
  lon: number;
  last_seen: string;
  is_online: boolean;
  role: number;
  inferred_prefix: string;
  inferred_hash_size_bytes: number;
  inferred_observations: number;
  inferred_packet_count: number;
  inferred_prev_name?: string | null;
  inferred_next_name?: string | null;
};

type InferredActiveResponse = {
  inferredNodes: InferredMultibyteNode[];
  inferredActiveNodeIds: string[];
};

// GET /api/node-status/latest — latest raw status telemetry per scoped node
router.get('/node-status/latest', async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);

    const params: string[] = [];
    const conditions: string[] = [];
    if (network) {
      params.push(network);
      conditions.push(`nss.network = $${params.length}`);
    } else {
      conditions.push(`nss.network IS DISTINCT FROM 'test'`);
    }
    if (observer) {
      params.push(observer);
      // observer is already lowercase from normalizeObserverQuery; node_id stored lowercase at insert
      conditions.push(`nss.node_id = $${params.length}`);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const result = await query<{
      time: string;
      node_id: string;
      network: string | null;
      battery_mv: number | null;
      uptime_secs: number | null;
      tx_air_secs: number | null;
      rx_air_secs: number | null;
      channel_utilization: number | null;
      air_util_tx: number | null;
      stats: Record<string, unknown> | null;
      name: string | null;
      iata: string | null;
      hardware_model: string | null;
      firmware_version: string | null;
    }>(
      `SELECT * FROM (
         SELECT DISTINCT ON (nss.node_id)
           nss.time::text,
           nss.node_id,
           nss.network,
           nss.battery_mv,
           nss.uptime_secs,
           nss.tx_air_secs,
           nss.rx_air_secs,
           nss.channel_utilization,
           nss.air_util_tx,
           nss.stats,
           n.name,
           n.iata,
           n.hardware_model,
           n.firmware_version
         FROM node_status_samples nss
         LEFT JOIN nodes n ON n.node_id = nss.node_id
         ${whereClause}
         ORDER BY nss.node_id, nss.time DESC
       ) latest
       ORDER BY time DESC`,
      params,
    );

    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /node-status/latest', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/node-status/history — recent telemetry history for a scoped node
router.get('/node-status/history', async (req, res) => {
  try {
    const ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH = 0.12;
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const requestedNodeId = String(req.query['nodeId'] ?? '').trim();
    const hours = Math.max(1, Math.min(Number(req.query['hours'] ?? 24), 168));

    let nodeId = requestedNodeId.toLowerCase();
    if (nodeId && !/^[0-9a-f]{64}$/.test(nodeId)) {
      res.status(400).json({ error: 'Invalid nodeId format' });
      return;
    }

    if (!nodeId) {
      const params: string[] = [];
      const conditions: string[] = [];
      if (network) {
        params.push(network);
        conditions.push(`nss.network = $${params.length}`);
      } else {
        conditions.push(`nss.network IS DISTINCT FROM 'test'`);
      }
      if (observer) {
        params.push(observer);
        // observer is already lowercase from normalizeObserverQuery; node_id stored lowercase at insert
        conditions.push(`nss.node_id = $${params.length}`);
      }
      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      const latestNode = await query<{ node_id: string }>(
        `SELECT nss.node_id
         FROM node_status_samples nss
         ${whereClause}
         ORDER BY nss.time DESC
         LIMIT 1`,
        params,
      );
      nodeId = latestNode.rows[0]?.node_id ?? '';
      if (!nodeId) {
        res.json({ nodeId: null, points: [] });
        return;
      }
    }

    const result = await query<{
      time: string;
      battery_mv: number | null;
      uptime_secs: number | null;
      channel_utilization: number | null;
      air_util_tx: number | null;
      heap_free: number | null;
      heap_min_free: number | null;
      uptime_ms: number | null;
      rx_publish_calls: number | null;
      tx_publish_calls: number | null;
      tx_queue_depth: number | null;
      tx_queue_depth_peak: number | null;
    }>(
      `SELECT
         time::text,
         battery_mv,
         uptime_secs,
         channel_utilization,
         air_util_tx,
         CASE
           WHEN jsonb_typeof(stats->'heap_free') = 'number' THEN (stats->>'heap_free')::double precision
           ELSE NULL
         END AS heap_free,
         CASE
           WHEN jsonb_typeof(stats->'heap_min_free') = 'number' THEN (stats->>'heap_min_free')::double precision
           ELSE NULL
         END AS heap_min_free,
         CASE
           WHEN jsonb_typeof(stats->'uptime_ms') = 'number' THEN (stats->>'uptime_ms')::double precision
           ELSE NULL
         END AS uptime_ms,
         CASE
           WHEN jsonb_typeof(stats->'rx_publish_calls') = 'number' THEN (stats->>'rx_publish_calls')::double precision
           ELSE NULL
         END AS rx_publish_calls,
         CASE
           WHEN jsonb_typeof(stats->'tx_publish_calls') = 'number' THEN (stats->>'tx_publish_calls')::double precision
           ELSE NULL
         END AS tx_publish_calls,
         CASE
           WHEN jsonb_typeof(stats->'tx_queue_depth') = 'number' THEN (stats->>'tx_queue_depth')::double precision
           ELSE NULL
         END AS tx_queue_depth,
         CASE
           WHEN jsonb_typeof(stats->'tx_queue_depth_peak') = 'number' THEN (stats->>'tx_queue_depth_peak')::double precision
           ELSE NULL
         END AS tx_queue_depth_peak
       FROM node_status_samples
       WHERE node_id = $1
         AND time > NOW() - ($2::text || ' hours')::interval
       ORDER BY time ASC`,
      [nodeId, String(hours)],
    );

    const points = result.rows.map((row, index) => {
      if (row.channel_utilization != null || row.air_util_tx != null) {
        return row;
      }

      const previous = index > 0 ? result.rows[index - 1] : null;
      const currentUptimeMs = row.uptime_ms;
      const previousUptimeMs = previous?.uptime_ms ?? null;
      const deltaUptimeSeconds =
        currentUptimeMs != null && previousUptimeMs != null && currentUptimeMs > previousUptimeMs
          ? (currentUptimeMs - previousUptimeMs) / 1000
          : null;

      if (!deltaUptimeSeconds || deltaUptimeSeconds <= 0) {
        return row;
      }

      const deltaRxCalls =
        row.rx_publish_calls != null && previous?.rx_publish_calls != null
          ? Math.max(0, row.rx_publish_calls - previous.rx_publish_calls)
          : 0;
      const deltaTxCalls =
        row.tx_publish_calls != null && previous?.tx_publish_calls != null
          ? Math.max(0, row.tx_publish_calls - previous.tx_publish_calls)
          : 0;

      const estimatedTxPct = Math.min(
        100,
        (deltaTxCalls * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
      );
      const estimatedTotalPct = Math.min(
        100,
        ((deltaTxCalls + deltaRxCalls) * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
      );

      return {
        ...row,
        channel_utilization: estimatedTotalPct,
        air_util_tx: estimatedTxPct,
      };
    });

    res.json({
      nodeId,
      points,
    });
  } catch (err) {
    console.error('[api] GET /node-status/history', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/local/test-diagnostics — local-only consolidated test-site data
router.get('/local/test-diagnostics', async (req, res) => {
  try {
    if (!requireLocalOnly(req, res)) return;

    const [nodes, packetsResult, latestStatusRows, statusSamplesResult] = await Promise.all([
      getNodes('test'),
      query<{
        time: string;
        topic: string;
        packet_hash: string | null;
        packet_type: number | null;
        route_type: number | null;
        hop_count: number | null;
        src_node_id: string | null;
        rx_node_id: string | null;
        rssi: number | null;
        snr: number | null;
        payload: Record<string, unknown> | null;
        raw_hex: string | null;
        path_hash_size_bytes: number | null;
        path_hashes: string[] | null;
      }>(
        `SELECT
           time::text,
           topic,
           packet_hash,
           packet_type,
           route_type,
           hop_count,
           src_node_id,
           rx_node_id,
           rssi,
           snr,
           payload,
           raw_hex,
           path_hash_size_bytes,
           path_hashes
         FROM packets
         WHERE network = 'test'
         ORDER BY time DESC`,
        [],
      ),
      query<{
        time: string;
        node_id: string;
        network: string | null;
        battery_mv: number | null;
        uptime_secs: number | null;
        tx_air_secs: number | null;
        rx_air_secs: number | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        stats: Record<string, unknown> | null;
        name: string | null;
        iata: string | null;
        hardware_model: string | null;
        firmware_version: string | null;
      }>(
        `SELECT * FROM (
           SELECT DISTINCT ON (nss.node_id)
             nss.time::text,
             nss.node_id,
             nss.network,
             nss.battery_mv,
             nss.uptime_secs,
             nss.tx_air_secs,
             nss.rx_air_secs,
             nss.channel_utilization,
             nss.air_util_tx,
             nss.stats,
             n.name,
             n.iata,
             n.hardware_model,
             n.firmware_version
           FROM node_status_samples nss
           LEFT JOIN nodes n ON n.node_id = nss.node_id
           WHERE nss.network = 'test'
           ORDER BY nss.node_id, nss.time DESC
         ) latest
         ORDER BY time DESC`,
        [],
      ),
      query<{
        time: string;
        node_id: string;
        network: string | null;
        battery_mv: number | null;
        uptime_secs: number | null;
        tx_air_secs: number | null;
        rx_air_secs: number | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        stats: Record<string, unknown> | null;
      }>(
        `SELECT
           time::text,
           node_id,
           network,
           battery_mv,
           uptime_secs,
           tx_air_secs,
           rx_air_secs,
           channel_utilization,
           air_util_tx,
           stats
         FROM node_status_samples
         WHERE network = 'test'
         ORDER BY time DESC`,
        [],
      ),
    ]);

    const packets = packetsResult.rows;
    const latestStatuses = latestStatusRows.rows;
    const statusSamples = statusSamplesResult.rows;
    const latestStatus = latestStatusRows.rows[0] ?? null;
    let history: unknown[] = [];
    if (latestStatus?.node_id) {
      const historyRows = await query<{
        time: string;
        battery_mv: number | null;
        uptime_secs: number | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        heap_free: number | null;
        heap_min_free: number | null;
        uptime_ms: number | null;
        rx_publish_calls: number | null;
        tx_publish_calls: number | null;
        tx_queue_depth: number | null;
        tx_queue_depth_peak: number | null;
      }>(
        `SELECT
           time::text,
           battery_mv,
           uptime_secs,
           channel_utilization,
           air_util_tx,
           CASE
             WHEN jsonb_typeof(stats->'heap_free') = 'number' THEN (stats->>'heap_free')::double precision
             ELSE NULL
           END AS heap_free,
           CASE
             WHEN jsonb_typeof(stats->'heap_min_free') = 'number' THEN (stats->>'heap_min_free')::double precision
             ELSE NULL
           END AS heap_min_free,
           CASE
             WHEN jsonb_typeof(stats->'uptime_ms') = 'number' THEN (stats->>'uptime_ms')::double precision
             ELSE NULL
           END AS uptime_ms,
           CASE
             WHEN jsonb_typeof(stats->'rx_publish_calls') = 'number' THEN (stats->>'rx_publish_calls')::double precision
             ELSE NULL
           END AS rx_publish_calls,
           CASE
             WHEN jsonb_typeof(stats->'tx_publish_calls') = 'number' THEN (stats->>'tx_publish_calls')::double precision
             ELSE NULL
           END AS tx_publish_calls,
           CASE
             WHEN jsonb_typeof(stats->'tx_queue_depth') = 'number' THEN (stats->>'tx_queue_depth')::double precision
             ELSE NULL
           END AS tx_queue_depth,
           CASE
             WHEN jsonb_typeof(stats->'tx_queue_depth_peak') = 'number' THEN (stats->>'tx_queue_depth_peak')::double precision
             ELSE NULL
           END AS tx_queue_depth_peak
         FROM node_status_samples
         WHERE node_id = $1
           AND network = 'test'
           AND time > NOW() - INTERVAL '24 hours'
         ORDER BY time ASC`,
        [latestStatus.node_id],
      );
      history = historyRows.rows;
    }

    res.json({
      network: 'test',
      nodes,
      packets,
      latestStatuses,
      statusSamples,
      latestStatus,
      history,
    });
  } catch (err) {
    console.error('[api] GET /local/test-diagnostics', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/nodes — all known nodes
router.get('/nodes', async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const nodes = await getNodes(network, observer);
    res.json(nodes);
  } catch (err) {
    console.error('[api] GET /nodes', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/inferred-nodes', async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const scope = networkFilters(network, observer);

    const inferredCacheKey = `${network ?? 'all'}:${observer ?? ''}`;
    const inferredCached = inferredNodesCache.get(inferredCacheKey);
    if (inferredCached && Date.now() - inferredCached.ts < INFERRED_NODES_CACHE_TTL_MS) {
      res.json(inferredCached.data);
      return;
    }

    const [visibleNodes, allNodeIds, packetsResult] = await Promise.all([
      getNodes(network, observer),
      query<{ node_id: string }>('SELECT node_id FROM nodes'),
      query<{
        packet_hash: string;
        time: string;
        path_hashes: string[] | null;
        path_hash_size_bytes: number | null;
      }>(
        `SELECT p.packet_hash, p.time::text, p.path_hashes, p.path_hash_size_bytes
         FROM packets p
         WHERE p.time > NOW() - INTERVAL '7 days'
           ${scope.packetsAlias('p')}
           AND p.path_hash_size_bytes > 1
           AND p.path_hashes IS NOT NULL
           AND array_length(p.path_hashes, 1) > 0
         ORDER BY p.time DESC`,
        scope.params,
      ),
    ]);

    const exactNodes = visibleNodes.filter((node) =>
      node.role === undefined || node.role === 2,
    );

    const inferredUnknowns = new Map<string, {
      prefix: string;
      hashSizeBytes: number;
      packetHashes: Set<string>;
      observations: number;
      latestSeen: string;
      sumLat: number;
      sumLon: number;
      prevNameCounts: Map<string, number>;
      nextNameCounts: Map<string, number>;
    }>();
    const inferredKnowns = new Map<string, {
      nodeId: string;
      packetHashes: Set<string>;
      observations: number;
      latestSeen: string;
    }>();

    const exactMatch = (pathHash: string) => {
      const normalized = pathHash.toUpperCase();
      const matches = exactNodes.filter((node) =>
        typeof node.lat === 'number'
        && typeof node.lon === 'number'
        && node.node_id.toUpperCase().startsWith(normalized),
      );
      return matches.length === 1 ? matches[0] : null;
    };

    for (const row of packetsResult.rows) {
      const pathHashes = Array.isArray(row.path_hashes) ? row.path_hashes : [];
      if (pathHashes.length < 3) continue;
      const hashSizeBytes = Number(row.path_hash_size_bytes ?? 0);
      if (hashSizeBytes < 2 || hashSizeBytes > 3) continue;

      for (let idx = 1; idx < pathHashes.length - 1; idx += 1) {
        const current = pathHashes[idx];
        const prev = pathHashes[idx - 1];
        const next = pathHashes[idx + 1];
        if (!current || !prev || !next) continue;
        if (current.length !== hashSizeBytes * 2) continue;

        const currentMatch = exactMatch(current);
        if (currentMatch) {
          const key = currentMatch.node_id;
          const existing = inferredKnowns.get(key) ?? {
            nodeId: currentMatch.node_id,
            packetHashes: new Set<string>(),
            observations: 0,
            latestSeen: row.time,
          };
          existing.packetHashes.add(row.packet_hash);
          existing.observations += 1;
          existing.latestSeen = existing.latestSeen > row.time ? existing.latestSeen : row.time;
          inferredKnowns.set(key, existing);
          continue;
        }

        const prevMatch = exactMatch(prev);
        const nextMatch = exactMatch(next);
        if (!prevMatch || !nextMatch) continue;

        const estimateLat = (Number(prevMatch.lat) + Number(nextMatch.lat)) / 2;
        const estimateLon = (Number(prevMatch.lon) + Number(nextMatch.lon)) / 2;
        const key = `${hashSizeBytes}:${current.toUpperCase()}`;
        const existing = inferredUnknowns.get(key) ?? {
          prefix: current.toUpperCase(),
          hashSizeBytes,
          packetHashes: new Set<string>(),
          observations: 0,
          latestSeen: row.time,
          sumLat: 0,
          sumLon: 0,
          prevNameCounts: new Map<string, number>(),
          nextNameCounts: new Map<string, number>(),
        };

        existing.packetHashes.add(row.packet_hash);
        existing.observations += 1;
        existing.latestSeen = existing.latestSeen > row.time ? existing.latestSeen : row.time;
        existing.sumLat += estimateLat;
        existing.sumLon += estimateLon;
        const prevLabel = prevMatch.name?.trim() || prevMatch.node_id.slice(0, 8);
        const nextLabel = nextMatch.name?.trim() || nextMatch.node_id.slice(0, 8);
        existing.prevNameCounts.set(prevLabel, (existing.prevNameCounts.get(prevLabel) ?? 0) + 1);
        existing.nextNameCounts.set(nextLabel, (existing.nextNameCounts.get(nextLabel) ?? 0) + 1);
        inferredUnknowns.set(key, existing);
      }
    }

    const bestLabel = (counts: Map<string, number>): string | null => {
      let best: string | null = null;
      let bestCount = -1;
      for (const [label, count] of counts) {
        if (count > bestCount) {
          best = label;
          bestCount = count;
        }
      }
      return best;
    };

    const knownNodeIdSet = new Set(allNodeIds.rows.map((n) => n.node_id.toUpperCase()));
    const inferredNodes: InferredMultibyteNode[] = Array.from(inferredUnknowns.values())
      .filter((entry) => entry.packetHashes.size >= 2
        && !Array.from(knownNodeIdSet).some((id) => id.startsWith(entry.prefix.toUpperCase())))
      .map((entry) => ({
        node_id: `inferred:${entry.hashSizeBytes}:${entry.prefix}`,
        name: `Inferred ${entry.prefix}`,
        lat: entry.sumLat / entry.observations,
        lon: entry.sumLon / entry.observations,
        last_seen: new Date(entry.latestSeen).toISOString(),
        is_online: true,
        role: 2,
        inferred_prefix: entry.prefix,
        inferred_hash_size_bytes: entry.hashSizeBytes,
        inferred_observations: entry.observations,
        inferred_packet_count: entry.packetHashes.size,
        inferred_prev_name: bestLabel(entry.prevNameCounts),
        inferred_next_name: bestLabel(entry.nextNameCounts),
      }))
      .sort((a, b) => (
        b.inferred_packet_count - a.inferred_packet_count
        || b.inferred_observations - a.inferred_observations
        || b.last_seen.localeCompare(a.last_seen)
      ));
    const inferredActiveNodeIds = Array.from(inferredKnowns.values())
      .filter((entry) => entry.packetHashes.size >= 2)
      .sort((a, b) => (
        b.packetHashes.size - a.packetHashes.size
        || b.observations - a.observations
        || b.latestSeen.localeCompare(a.latestSeen)
      ))
      .map((entry) => entry.nodeId);

    const payload: InferredActiveResponse = {
      inferredNodes,
      inferredActiveNodeIds,
    };
    inferredNodesCache.set(inferredCacheKey, { ts: Date.now(), data: payload });
    res.json(payload);
  } catch (err) {
    console.error('[api] GET /inferred-nodes', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/nodes/:id/links — ITM-viable neighbours for a node
router.get('/nodes/:id/links', async (req, res) => {
  try {
    const id = req.params['id']!;
    if (!/^[0-9a-fA-F]{64}$/.test(id)) {
      res.status(400).json({ error: 'Invalid node ID format' });
      return;
    }
    const result = await query<{
      peer_id: string; peer_name: string | null; observed_count: number;
      itm_path_loss_db: number | null;
      count_this_to_peer: number; count_peer_to_this: number;
    }>(
      `SELECT
         CASE WHEN node_a_id = $1 THEN node_b_id ELSE node_a_id END AS peer_id,
         n.name AS peer_name,
         observed_count,
         itm_path_loss_db,
         CASE WHEN node_a_id = $1 THEN count_a_to_b ELSE count_b_to_a END AS count_this_to_peer,
         CASE WHEN node_a_id = $1 THEN count_b_to_a ELSE count_a_to_b END AS count_peer_to_this
       FROM node_links
       LEFT JOIN nodes n ON n.node_id = CASE WHEN node_a_id = $1 THEN node_b_id ELSE node_a_id END
       WHERE (node_a_id = $1 OR node_b_id = $1) AND (itm_viable = true OR force_viable = true) AND observed_count >= $2
       ORDER BY observed_count DESC`,
      [id, MIN_LINK_OBSERVATIONS],
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /nodes/:id/links', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/nodes/:id/history?hours=24
router.get('/nodes/:id/history', async (req, res) => {
  try {
    const id = req.params['id']!;
    if (!/^[0-9a-fA-F]{64}$/.test(id)) {
      res.status(400).json({ error: 'Invalid node ID format' });
      return;
    }
    const hours = Math.min(Number(req.query['hours'] ?? 24), 672); // max 28 days
    const history = await getNodeHistory(id, hours);
    res.json(history);
  } catch (err) {
    console.error('[api] GET /nodes/:id/history', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/nodes/:publicKey/adverts?hours=24 — advert packets for a node by public key
router.get('/nodes/:id/adverts', async (req, res) => {
  try {
    const publicKey = req.params['id']!;
    if (!/^[0-9a-fA-F]{64}$/.test(publicKey)) {
      res.status(400).json({ error: 'Invalid public key format' });
      return;
    }
    const hours = Math.min(Number(req.query['hours'] ?? 24), 672);
    const adverts = await getNodeAdverts(publicKey, hours);
    res.json(adverts);
  } catch (err) {
    console.error('[api] GET /nodes/:id/adverts', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/packets/recent?limit=200
router.get('/packets/recent', async (req, res) => {
  try {
    const limit = Math.min(Number(req.query['limit'] ?? 200), 1000);
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const raw = String(req.query['raw'] ?? '').trim();
    const packets = raw === '1'
      ? await getRecentPacketEvents(limit, network, observer)
      : await getRecentPackets(limit, network, observer);
    res.json(packets);
  } catch (err) {
    console.error('[api] GET /packets/recent', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-beta/resolve?hash=<packetHash>&network=teesside|ukmesh|all
router.get('/path-beta/resolve', PATH_BETA_LIMITER, async (req, res) => {
  try {
    const packetHash = String(req.query['hash'] ?? '').trim();
    if (!packetHash) {
      res.status(400).json({ error: 'Missing hash query parameter' });
      return;
    }
    if (!/^[0-9a-fA-F]{1,128}$/.test(packetHash)) {
      res.status(400).json({ error: 'Invalid hash format' });
      return;
    }
    const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
    const observer = normalizeObserverQuery(req.query['observer']);
    const ck = `r|${packetHash}|${network}|${observer ?? ''}`;
    const hit = getResolveCache(ck);
    if (hit) { res.json(hit); return; }
    const resolved = await resolvePool.run<unknown>({ type: 'resolve', packetHash, network, observer });
    if (!resolved) {
      res.status(404).json({ error: 'Packet not found' });
      return;
    }
    setResolveCache(ck, resolved);
    res.json(resolved);
  } catch (err) {
    console.error('[api] GET /path-beta/resolve', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-beta/resolve-multi?hash=<packetHash>&network=teesside|ukmesh|all
router.get('/path-beta/resolve-multi', PATH_BETA_LIMITER, async (req, res) => {
  try {
    const packetHash = String(req.query['hash'] ?? '').trim();
    if (!packetHash) {
      res.status(400).json({ error: 'Missing hash query parameter' });
      return;
    }
    if (!/^[0-9a-fA-F]{1,128}$/.test(packetHash)) {
      res.status(400).json({ error: 'Invalid hash format' });
      return;
    }
    const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
    const ck = `m|${packetHash}|${network}`;
    const hit = getResolveCache(ck);
    if (hit) { res.json(hit); return; }
    const resolved = await resolvePool.run<unknown>({ type: 'resolveMulti', packetHash, network });
    if (!resolved) {
      res.status(404).json({ error: 'Packet not found' });
      return;
    }
    setResolveCache(ck, resolved);
    res.json(resolved);
  } catch (err) {
    console.error('[api] GET /path-beta/resolve-multi', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-beta/history?network=teesside|ukmesh|all
router.get('/path-beta/history', PATH_HISTORY_LIMITER, async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const scope = requestedNetwork === 'all' ? 'all' : (requestedNetwork ?? 'teesside');

    const historyCached = pathHistoryCache.get(scope);
    if (historyCached && Date.now() - historyCached.ts < PATH_HISTORY_CACHE_TTL_MS) {
      res.json(historyCached.data);
      return;
    }

    const cached = await getPathHistoryCache(scope);
    let responseData: unknown;
    if (!cached) {
      responseData = {
        ok: true,
        scope,
        windowStart: null,
        updatedAt: null,
        packetCount: 0,
        resolvedPacketCount: 0,
        maxCount: 0,
        segments: [],
      };
    } else {
      const segments = Array.isArray(cached.segment_counts) ? cached.segment_counts : [];
      const maxCount = segments.reduce((max, segment) => Math.max(max, Number(segment.count ?? 0)), 0);
      responseData = {
        ok: true,
        scope,
        windowStart: cached.window_start,
        updatedAt: cached.updated_at,
        packetCount: cached.packet_count,
        resolvedPacketCount: cached.resolved_packet_count,
        maxCount,
        segments,
      };
    }

    pathHistoryCache.set(scope, { ts: Date.now(), data: responseData });
    res.json(responseData);
  } catch (err) {
    console.error('[api] GET /path-beta/history', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/stats
router.get('/stats', async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const filters = networkFilters(network, observer);
    const statsCacheKey = `${network ?? 'all'}:${observer ?? ''}`;
    const statsCached = statsCache.get(statsCacheKey);
    if (statsCached && Date.now() - statsCached.ts < STATS_CACHE_TTL_MS) {
      res.json(statsCached.data);
      return;
    }

    const [mqttCount, packetCount, staleCount, mapNodeCount, totalNodeCount, longestHopCount, nodesDayCount] = await Promise.all([
      // When a specific network is requested the outer filter already excludes all other networks,
      // so the test_active CTE (designed to strip test-only nodes from a mixed-network view) is
      // redundant and extremely expensive — it scans the entire rx_idx across all chunks.
      // Only use it when fetching across all networks (network === undefined).
      network != null
        ? query(`SELECT COUNT(DISTINCT rx_node_id) AS count
                 FROM packets
                 WHERE time > NOW() - INTERVAL '10 minutes'
                   AND rx_node_id IS NOT NULL
                   ${filters.packets}`, filters.params)
        : query(`
          WITH test_active AS (
            SELECT rx_node_id FROM packets WHERE rx_node_id IS NOT NULL AND rx_node_id <> ''
              AND time > NOW() - INTERVAL '7 days'
            GROUP BY rx_node_id HAVING MAX(time) = MAX(time) FILTER (WHERE network = 'test')
          )
          SELECT COUNT(DISTINCT rx_node_id) AS count
          FROM packets
          WHERE time > NOW() - INTERVAL '10 minutes'
            AND rx_node_id IS NOT NULL
            AND rx_node_id NOT IN (SELECT rx_node_id FROM test_active)
            ${filters.packets}
        `, filters.params),
      query(`SELECT COUNT(*) AS count FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE lat IS NOT NULL AND lon IS NOT NULL
               AND (role IS NULL OR role = 2)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND last_seen <= NOW() - INTERVAL '7 days'
               AND last_seen >  NOW() - INTERVAL '14 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE lat IS NOT NULL AND lon IS NOT NULL
               AND (role IS NULL OR role = 2)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND last_seen > NOW() - INTERVAL '14 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE (name IS NULL OR name NOT LIKE '%🚫%')
               AND (role IS NULL OR role != 4)
               ${filters.nodes}`, filters.params),
      // Regex-in-WHERE was forcing a full seq scan (payload->>'hash' can't be indexed).
      // Just fetch payload->>'hash' without a WHERE regex — it's null for the rare rows
      // that lack it, which is fine for a display-only stat.
      query(`SELECT hop_count AS count, payload->>'hash' AS hash
             FROM packets
             WHERE hop_count IS NOT NULL
               AND time > NOW() - INTERVAL '30 days'
               ${filters.packets}
             ORDER BY hop_count DESC LIMIT 1`, filters.params),
      query(`SELECT COUNT(DISTINCT src_node_id) AS count
             FROM packets
             WHERE time > NOW() - INTERVAL '24 hours'
               AND src_node_id IS NOT NULL
               ${filters.packets}`, filters.params),
    ]);
    const statsData = {
      mqttNodes:      Number(mqttCount.rows[0]?.count ?? 0),
      staleNodes:     Number(staleCount.rows[0]?.count ?? 0),
      packetsDay:     Number(packetCount.rows[0]?.count ?? 0),
      mapNodes:       Number(mapNodeCount.rows[0]?.count ?? 0),
      nodesDay:       Number(nodesDayCount.rows[0]?.count ?? 0),
      totalNodes:     Number(totalNodeCount.rows[0]?.count ?? 0),
      longestHop:     Number(longestHopCount.rows[0]?.count ?? 0),
      longestHopHash: (longestHopCount.rows[0]?.hash as string | undefined) ?? null,
    };
    statsCache.set(statsCacheKey, { ts: Date.now(), data: statsData });
    res.json(statsData);
  } catch (err) {
    console.error('[api] GET /stats', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});


// GET /api/coverage — all stored viewshed polygons (excludes hidden 🚫 nodes)
router.get('/coverage', COVERAGE_LIMITER, async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const coverageCacheKey = `${network ?? 'all'}:${observer ?? ''}`;

    const coverageCached = coverageCache.get(coverageCacheKey);
    if (coverageCached && Date.now() - coverageCached.ts < COVERAGE_CACHE_TTL_MS) {
      res.json(coverageCached.data);
      return;
    }

    const filters = networkFilters(network, observer);
    const result = await query(
      `SELECT nc.node_id, nc.geom, nc.strength_geoms, nc.antenna_height_m, nc.radius_m, nc.calculated_at
       FROM node_coverage nc
       JOIN nodes n ON n.node_id = nc.node_id
       WHERE (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         AND (n.role IS NULL OR n.role = 2)
         ${filters.nodesAlias('n')}`,
      filters.params
    );
    coverageCache.set(coverageCacheKey, { ts: Date.now(), data: result.rows });
    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /coverage', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/planned-nodes
router.get('/planned-nodes', async (_req, res) => {
  try {
    const result = await query(
      'SELECT id, owner_pubkey, name, lat, lon, height_m, notes, created_at FROM planned_nodes ORDER BY created_at DESC'
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /planned-nodes', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-learning
router.get('/path-learning', PATH_LEARNING_LIMITER, async (req, res) => {
  try {
    const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
    const limit = Math.min(12000, Math.max(1000, Number(req.query['limit'] ?? 6000)));
    const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
      query<{
        prefix: string;
        receiver_region: string;
        prev_prefix: string | null;
        node_id: string;
        probability: number;
        count: number;
      }>(
        `SELECT prefix, receiver_region, prev_prefix, node_id, probability, count
         FROM path_prefix_priors
         WHERE network = $1
         ORDER BY count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        from_node_id: string;
        to_node_id: string;
        receiver_region: string;
        probability: number;
        count: number;
      }>(
        `SELECT from_node_id, to_node_id, receiver_region, probability, count
         FROM path_transition_priors
         WHERE network = $1
         ORDER BY count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        from_node_id: string;
        to_node_id: string;
        receiver_region: string;
        hour_bucket: number;
        observed_count: number;
        expected_count: number;
        missing_count: number;
        directional_support: number;
        recency_score: number;
        reliability: number;
        itm_path_loss_db: number | null;
        score: number;
        consistency_penalty: number;
      }>(
        `SELECT from_node_id, to_node_id, receiver_region, hour_bucket,
                observed_count, expected_count, missing_count, directional_support,
                recency_score, reliability, itm_path_loss_db, score, consistency_penalty
         FROM path_edge_priors
         WHERE network = $1
         ORDER BY score DESC, observed_count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        receiver_region: string;
        hour_bucket: number;
        motif_len: number;
        node_ids: string;
        probability: number;
        count: number;
      }>(
        `SELECT receiver_region, hour_bucket, motif_len, node_ids, probability, count
         FROM path_motif_priors
         WHERE network = $1
         ORDER BY count DESC
         LIMIT $2`,
        [network, limit],
      ),
      query<{
        evaluated_packets: number;
        top1_accuracy: number;
        mean_pred_confidence: number;
        confidence_scale: number;
        confidence_bias: number;
        recommended_threshold: number;
      }>(
        `SELECT evaluated_packets, top1_accuracy, mean_pred_confidence, confidence_scale, confidence_bias, recommended_threshold
         FROM path_model_calibration
         WHERE network = $1`,
        [network],
      ),
    ]);

    const calibration = calibrationRows.rows[0] ?? {
      evaluated_packets: 0,
      top1_accuracy: 0,
      mean_pred_confidence: 0,
      confidence_scale: 1,
      confidence_bias: 0,
      recommended_threshold: 0.5,
    };

    res.json({
      network,
      calibration,
      prefixPriors: prefixRows.rows,
      transitionPriors: transitionRows.rows,
      edgePriors: edgeRows.rows,
      motifPriors: motifRows.rows,
    });
  } catch (err) {
    console.error('[api] GET /path-learning', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/health — public health overview with worker status and history
router.get('/health', async (_req, res) => {
  try {
    const data = await getWorkerHealthOverview();
    res.json(data);
  } catch (err) {
    console.error('[api] GET /health', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/owner/login — login with MQTT username/password and issue encrypted cookie session
router.post('/owner/login', OWNER_LOGIN_LIMITER, async (req, res) => {
  try {
    const body = req.body as { mqttUsername?: string; mqttPassword?: string } | undefined;
    const mqttUsername = String(body?.mqttUsername ?? '').trim();
    const mqttPassword = String(body?.mqttPassword ?? '').trim();
    if (!mqttUsername || !mqttPassword) {
      res.status(400).json({ error: 'Missing MQTT username or password' });
      return;
    }
    if (mqttUsername.length > MQTT_USERNAME_MAX_LEN || mqttPassword.length > MQTT_PASSWORD_MAX_LEN) {
      res.status(400).json({ error: 'MQTT username or password is too long' });
      return;
    }
    if (hasControlChars(mqttUsername) || hasControlChars(mqttPassword)) {
      res.status(400).json({ error: 'MQTT username or password contains invalid characters' });
      return;
    }
    if (!/^[a-zA-Z0-9_\-.@]+$/.test(mqttUsername)) {
      res.status(400).json({ error: 'Invalid MQTT username format' });
      return;
    }
    const authOk = await verifyMqttCredentials(mqttUsername, mqttPassword);
    if (!authOk) {
      res.status(403).json({ error: 'Invalid MQTT credentials' });
      return;
    }

    let mappedNodeIds = await resolveOwnerNodeIds(mqttUsername);
    if (mappedNodeIds.length < 1) {
      mappedNodeIds = await autoLinkOwnerNodeIds(mqttUsername);
    }

    const dashboard = await buildOwnerDashboard(mappedNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      res.status(403).json({ error: 'No active node found for this MQTT username yet' });
      return;
    }

    const token = encryptOwnerSession({
      nodeIds: mappedNodeIds,
      exp: Date.now() + OWNER_SESSION_TTL_MS,
      mqttUsername,
    });
    res.cookie(OWNER_COOKIE_NAME, token, {
      httpOnly: true,
      secure: isSecureRequest(req),
      sameSite: 'strict',
      path: '/',
      maxAge: OWNER_SESSION_TTL_MS,
    });
    res.json({ ok: true, dashboard });
  } catch (err) {
    console.error('[api] POST /owner/login', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/owner/session — resolve dashboard from encrypted cookie
router.get('/owner/session', async (req, res) => {
  try {
    const session = getOwnerSession(req);
    if (!session) {
      res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
      res.status(401).json({ error: 'Not logged in' });
      return;
    }
    const sessionNodeIds = session.nodeIds;
    if (!sessionNodeIds) return;

    const dashboard = await buildOwnerDashboard(sessionNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
      res.status(401).json({ error: 'No active node found for this MQTT username yet' });
      return;
    }

    res.json({ ok: true, dashboard, mqttUsername: session.mqttUsername ?? null });
  } catch (err) {
    console.error('[api] GET /owner/session', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/owner/live?nodeId=<hex> — live owner-focused data for map + packet feed
router.get('/owner/live', async (req, res) => {
  try {
    const ownedNodeIds = await requireOwnerSession(req, res);
    if (!ownedNodeIds) return;
    if (ownedNodeIds.length < 1) {
      res.status(404).json({ error: 'No owned nodes found' });
      return;
    }

    const requestedNodeId = String(req.query['nodeId'] ?? '').trim().toLowerCase();
    const selectedNodeId = requestedNodeId
      ? ownedNodeIds.find((id) => id.toLowerCase() === requestedNodeId)
      : ownedNodeIds[0];
    if (!selectedNodeId) {
      res.status(403).json({ error: 'Node is not owned by this session' });
      return;
    }

    const cacheEntry = ownerLiveCache.get(selectedNodeId);
    if (cacheEntry && Date.now() - cacheEntry.ts < OWNER_LIVE_CACHE_TTL_MS) {
      res.json(cacheEntry.data);
      return;
    }

    const [
      ownerNodeResult,
      incomingResult,
      packetResult,
      heardByResult,
      linkHealthResult,
      advertTrendResult,
      telemetryResult,
    ] = await Promise.all([
      query<{
        node_id: string;
        name: string | null;
        network: string;
        iata: string | null;
        advert_count: number | null;
        last_seen: string | null;
        lat: number | null;
        lon: number | null;
        role: number | null;
      }>(
        `SELECT node_id, name, network, iata, advert_count, last_seen, lat, lon, role
         FROM nodes
         WHERE LOWER(node_id) = LOWER($1)
         LIMIT 1`,
        [selectedNodeId],
      ),
      query<{
        node_id: string;
        name: string | null;
        network: string | null;
        iata: string | null;
        lat: number | null;
        lon: number | null;
        packets_24h: number;
        last_seen: string | null;
      }>(
        `SELECT
           p.src_node_id AS node_id,
           n.name,
           n.network,
           n.iata,
           n.lat,
           n.lon,
           COUNT(*)::int AS packets_24h,
           MAX(p.time)::text AS last_seen
         FROM packets p
         LEFT JOIN nodes n ON n.node_id = p.src_node_id
         WHERE LOWER(p.rx_node_id) = LOWER($1)
           AND p.hop_count = 0
           AND p.src_node_id IS NOT NULL
           AND LOWER(p.src_node_id) <> LOWER($1)
           AND p.time > NOW() - INTERVAL '24 hours'
         GROUP BY p.src_node_id, n.name, n.network, n.iata, n.lat, n.lon
         ORDER BY packets_24h DESC
         LIMIT 250`,
        [selectedNodeId],
      ),
      query<{
        time: string;
        packet_type: number | null;
        route_type: number | null;
        hop_count: number | null;
        packet_hash: string | null;
        src_node_id: string | null;
        src_node_name: string | null;
        sender: string | null;
        body: string | null;
      }>(
        `WITH ranked AS (
           SELECT
             p.time,
             p.packet_type,
             p.route_type,
             p.hop_count,
             p.packet_hash,
             p.src_node_id,
             p.payload,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(
                 p.packet_hash,
                 CONCAT_WS(':',
                   COALESCE(p.src_node_id, ''),
                   COALESCE(p.packet_type::text, ''),
                   COALESCE(p.route_type::text, ''),
                   COALESCE(p.hop_count::text, ''),
                   COALESCE(p.payload->'decrypted'->>'sender', ''),
                   COALESCE(p.payload->'decrypted'->>'text', ''),
                   DATE_TRUNC('second', p.time)::text
                 )
               )
               ORDER BY p.time DESC
             ) AS rn
           FROM packets p
           WHERE LOWER(p.rx_node_id) = LOWER($1)
         )
         SELECT
           r.time::text AS time,
           r.packet_type,
           r.route_type,
           r.hop_count,
           r.packet_hash,
           r.src_node_id,
           src.name AS src_node_name,
           COALESCE(r.payload->'decrypted'->>'sender', src.name, r.src_node_id) AS sender,
           COALESCE(
             r.payload->'decrypted'->>'text',
             r.payload->'decrypted'->>'message',
             r.payload->'decrypted'->>'body',
             r.payload->'decoded'->>'text',
             r.payload->>'message'
           ) AS body
         FROM ranked r
         LEFT JOIN nodes src ON src.node_id = r.src_node_id
         WHERE r.rn = 1
         ORDER BY r.time DESC
         LIMIT 9`,
        [selectedNodeId],
      ),
      query<{
        node_id: string;
        name: string | null;
        network: string | null;
        iata: string | null;
        lat: number | null;
        lon: number | null;
        packets_24h: number;
        packets_7d: number;
        last_seen: string | null;
        best_hops: number | null;
      }>(
        `SELECT
           p.rx_node_id AS node_id,
           n.name,
           n.network,
           n.iata,
           n.lat,
           n.lon,
           COUNT(DISTINCT CASE WHEN p.time > NOW() - INTERVAL '24 hours' THEN p.packet_hash END)::int AS packets_24h,
           COUNT(DISTINCT p.packet_hash)::int AS packets_7d,
           MAX(p.time)::text AS last_seen,
           MIN(p.hop_count) AS best_hops
         FROM packets p
         LEFT JOIN nodes n ON LOWER(n.node_id) = LOWER(p.rx_node_id)
         WHERE LOWER(p.src_node_id) = LOWER($1)
           AND p.rx_node_id IS NOT NULL
           AND LOWER(p.rx_node_id) <> LOWER($1)
           AND p.time > NOW() - INTERVAL '7 days'
         GROUP BY p.rx_node_id, n.name, n.network, n.iata, n.lat, n.lon
         ORDER BY packets_24h DESC, packets_7d DESC, last_seen DESC
         LIMIT 20`,
        [selectedNodeId],
      ),
      query<{
        peer_node_id: string;
        peer_name: string | null;
        peer_network: string | null;
        owner_to_peer: number;
        peer_to_owner: number;
        observed_count: number;
        itm_path_loss_db: number | null;
        itm_viable: boolean | null;
        force_viable: boolean;
        last_observed: string | null;
      }>(
        `SELECT
           CASE WHEN LOWER(nl.node_a_id) = LOWER($1) THEN nl.node_b_id ELSE nl.node_a_id END AS peer_node_id,
           peer.name AS peer_name,
           peer.network AS peer_network,
           CASE WHEN LOWER(nl.node_a_id) = LOWER($1) THEN nl.count_a_to_b ELSE nl.count_b_to_a END AS owner_to_peer,
           CASE WHEN LOWER(nl.node_a_id) = LOWER($1) THEN nl.count_b_to_a ELSE nl.count_a_to_b END AS peer_to_owner,
           nl.observed_count,
           nl.itm_path_loss_db,
           nl.itm_viable,
           nl.force_viable,
           nl.last_observed::text AS last_observed
         FROM node_links nl
         JOIN nodes peer ON LOWER(peer.node_id) = LOWER(CASE WHEN LOWER(nl.node_a_id) = LOWER($1) THEN nl.node_b_id ELSE nl.node_a_id END)
         WHERE (LOWER(nl.node_a_id) = LOWER($1)
            OR LOWER(nl.node_b_id) = LOWER($1))
           AND (
             nl.force_viable = true
             OR nl.itm_viable = true
             OR (nl.itm_path_loss_db IS NOT NULL AND nl.itm_path_loss_db <= 137.88)
           )
         ORDER BY
           COALESCE(nl.itm_viable, false) DESC,
           nl.force_viable DESC,
           nl.observed_count DESC,
           nl.itm_path_loss_db ASC NULLS LAST
         LIMIT 12`,
        [selectedNodeId],
      ),
      query<{
        bucket: string;
        adverts: number;
      }>(
        `SELECT
           time_bucket('1 hour', time)::text AS bucket,
           COUNT(DISTINCT packet_hash)::int AS adverts
         FROM packets
         WHERE LOWER(src_node_id) = LOWER($1)
           AND packet_type = 4
           AND time > NOW() - INTERVAL '24 hours'
         GROUP BY bucket
         ORDER BY bucket`,
        [selectedNodeId],
      ),
      query<{
        time: string;
        battery_mv: number | null;
        uptime_secs: string | null;
        tx_air_secs: string | null;
        rx_air_secs: string | null;
        channel_utilization: number | null;
        air_util_tx: number | null;
        uptime_ms: number | null;
        rx_publish_calls: number | null;
        tx_publish_calls: number | null;
      }>(
        `SELECT
           time::text AS time,
           battery_mv,
           uptime_secs::text AS uptime_secs,
           tx_air_secs::text AS tx_air_secs,
           rx_air_secs::text AS rx_air_secs,
           channel_utilization,
           air_util_tx,
           CASE
             WHEN jsonb_typeof(stats->'uptime_ms') = 'number' THEN (stats->>'uptime_ms')::double precision
             ELSE NULL
           END AS uptime_ms,
           CASE
             WHEN jsonb_typeof(stats->'rx_publish_calls') = 'number' THEN (stats->>'rx_publish_calls')::double precision
             ELSE NULL
           END AS rx_publish_calls,
           CASE
             WHEN jsonb_typeof(stats->'tx_publish_calls') = 'number' THEN (stats->>'tx_publish_calls')::double precision
             ELSE NULL
           END AS tx_publish_calls
         FROM node_status_samples
         WHERE LOWER(node_id) = LOWER($1)
           AND time > NOW() - INTERVAL '24 hours'
         ORDER BY time ASC`,
        [selectedNodeId],
      ),
    ]);

    const ownerNode = ownerNodeResult.rows[0];
    if (!ownerNode) {
      res.status(404).json({ error: 'Owner node not found' });
      return;
    }

    const heardBy = heardByResult.rows.map((row) => ({
      ...row,
      packets_24h: Number(row.packets_24h ?? 0),
      packets_7d: Number(row.packets_7d ?? 0),
      best_hops: row.best_hops == null ? null : Number(row.best_hops),
      last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
    }));

    const linkHealth = linkHealthResult.rows.map((row) => ({
      ...row,
      owner_to_peer: Number(row.owner_to_peer ?? 0),
      peer_to_owner: Number(row.peer_to_owner ?? 0),
      observed_count: Number(row.observed_count ?? 0),
      itm_path_loss_db: row.itm_path_loss_db == null ? null : Number(row.itm_path_loss_db),
      itm_viable: row.itm_viable == null ? null : Boolean(row.itm_viable),
      force_viable: Boolean(row.force_viable),
      last_observed: row.last_observed ? new Date(row.last_observed).toISOString() : null,
    }));

    const advertTrend24h = (() => {
      const byHour = new Map<string, number>();
      for (const row of advertTrendResult.rows) {
        byHour.set(new Date(row.bucket).toISOString(), Number(row.adverts ?? 0));
      }
      const series: Array<{ bucket: string; adverts: number }> = [];
      const now = new Date();
      now.setUTCMinutes(0, 0, 0);
      for (let i = 23; i >= 0; i--) {
        const bucket = new Date(now.getTime() - i * 60 * 60 * 1000).toISOString();
        series.push({ bucket, adverts: byHour.get(bucket) ?? 0 });
      }
      return series;
    })();

    const telemetry24h = (() => {
      const ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH = 0.12;
      const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
      const bucketMs = 5 * 60 * 1000;
      type Point = {
        bucket: string;
        batteryPct: number | null;
        batteryMv: number | null;
        uptimeSecs: number | null;
        channelUtilPct: number | null;
        airUtilTxPct: number | null;
      };

      const samples = telemetryResult.rows.map((row) => ({
        timeMs: new Date(row.time).getTime(),
        batteryMv: row.battery_mv == null ? null : Number(row.battery_mv),
        uptimeSecs: row.uptime_secs == null ? null : Number(row.uptime_secs),
        uptimeMs: row.uptime_ms == null ? null : Number(row.uptime_ms),
        txAirSecs: row.tx_air_secs == null ? null : Number(row.tx_air_secs),
        rxAirSecs: row.rx_air_secs == null ? null : Number(row.rx_air_secs),
        channelUtilization: row.channel_utilization == null ? null : Number(row.channel_utilization),
        airUtilTx: row.air_util_tx == null ? null : Number(row.air_util_tx),
        rxPublishCalls: row.rx_publish_calls == null ? null : Number(row.rx_publish_calls),
        txPublishCalls: row.tx_publish_calls == null ? null : Number(row.tx_publish_calls),
      }));

      const bucketed = new Map<number, Point>();
      for (let i = 0; i < samples.length; i++) {
        const sample = samples[i]!;
        const prev = i > 0 ? samples[i - 1]! : null;
        const batteryPct = sample.batteryMv == null
          ? null
          : clamp(((sample.batteryMv - 3200) / 1000) * 100, 0, 100);

        let channelUtilPct = sample.channelUtilization;
        let airUtilTxPct = sample.airUtilTx;

        if ((channelUtilPct == null || airUtilTxPct == null) && prev) {
          const uptimeDelta = (sample.uptimeSecs ?? 0) - (prev.uptimeSecs ?? 0);
          const txDelta = (sample.txAirSecs ?? 0) - (prev.txAirSecs ?? 0);
          const rxDelta = (sample.rxAirSecs ?? 0) - (prev.rxAirSecs ?? 0);
          if (uptimeDelta > 0 && txDelta >= 0 && rxDelta >= 0) {
            if (airUtilTxPct == null) {
              airUtilTxPct = clamp((txDelta / uptimeDelta) * 100, 0, 100);
            }
            if (channelUtilPct == null) {
              channelUtilPct = clamp(((txDelta + rxDelta) / uptimeDelta) * 100, 0, 100);
            }
          }
        }

        if ((channelUtilPct == null || airUtilTxPct == null) && prev) {
          const currentUptimeMs = sample.uptimeMs;
          const previousUptimeMs = prev.uptimeMs;
          const deltaUptimeSeconds =
            currentUptimeMs != null && previousUptimeMs != null && currentUptimeMs > previousUptimeMs
              ? (currentUptimeMs - previousUptimeMs) / 1000
              : 0;
          const deltaRxCalls =
            sample.rxPublishCalls != null && prev.rxPublishCalls != null
              ? Math.max(0, sample.rxPublishCalls - prev.rxPublishCalls)
              : 0;
          const deltaTxCalls =
            sample.txPublishCalls != null && prev.txPublishCalls != null
              ? Math.max(0, sample.txPublishCalls - prev.txPublishCalls)
              : 0;

          if (deltaUptimeSeconds > 0) {
            if (airUtilTxPct == null) {
              airUtilTxPct = clamp(
                (deltaTxCalls * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
                0,
                100,
              );
            }
            if (channelUtilPct == null) {
              channelUtilPct = clamp(
                ((deltaTxCalls + deltaRxCalls) * ESTIMATED_AIRTIME_SECONDS_PER_PUBLISH / deltaUptimeSeconds) * 100,
                0,
                100,
              );
            }
          }
        }

        const bucketStartMs = Math.floor(sample.timeMs / bucketMs) * bucketMs;
        bucketed.set(bucketStartMs, {
          bucket: new Date(bucketStartMs).toISOString(),
          batteryPct: batteryPct == null ? null : Number(batteryPct.toFixed(1)),
          batteryMv: sample.batteryMv,
          uptimeSecs: sample.uptimeSecs == null ? null : Math.max(0, Math.round(sample.uptimeSecs)),
          channelUtilPct: channelUtilPct == null ? null : Number(channelUtilPct.toFixed(2)),
          airUtilTxPct: airUtilTxPct == null ? null : Number(airUtilTxPct.toFixed(2)),
        });
      }

      return Array.from(bucketed.values()).sort((a, b) => a.bucket.localeCompare(b.bucket));
    })();

    const alerts: Array<{ level: 'info' | 'warn' | 'error'; message: string }> = [];
    const ownerLastSeenMs = ownerNode.last_seen ? new Date(ownerNode.last_seen).getTime() : 0;
    const minsSinceSeen = ownerLastSeenMs ? Math.max(0, Math.round((Date.now() - ownerLastSeenMs) / 60000)) : null;
    const adverts24h = advertTrend24h.reduce((sum, point) => sum + point.adverts, 0);
    const viableLinks = linkHealth.filter((link) => link.itm_viable || link.force_viable);
    const roleLabel = ownerNode.role === 1 ? 'Companion' : ownerNode.role === 3 ? 'Room Server' : 'Repeater';
    if (minsSinceSeen == null) alerts.push({ level: 'error', message: `No last-seen timestamp is available for this ${roleLabel.toLowerCase()}.` });
    else if (minsSinceSeen >= 120) alerts.push({ level: 'error', message: `${roleLabel} has not been seen for ${minsSinceSeen} minutes.` });
    else if (minsSinceSeen >= 30) alerts.push({ level: 'warn', message: `${roleLabel} has been quiet for ${minsSinceSeen} minutes.` });
    else alerts.push({ level: 'info', message: `${roleLabel} is active and has checked in recently.` });

    if (adverts24h < 1) alerts.push({ level: 'warn', message: `No advert packets from this ${roleLabel.toLowerCase()} were recorded in the last 24 hours.` });
    if (heardBy.length < 1) alerts.push({ level: 'warn', message: `No other nodes have heard this ${roleLabel.toLowerCase()} in the last 7 days.` });
    if (viableLinks.length < 1) alerts.push({ level: 'warn', message: `No viable RF links are currently stored for this ${roleLabel.toLowerCase()}.` });

    const responseData = {
      nodeId: selectedNodeId,
      ownerNode: {
        ...ownerNode,
        advert_count: Number(ownerNode.advert_count ?? 0),
        last_seen: ownerNode.last_seen ? new Date(ownerNode.last_seen).toISOString() : null,
      },
      incomingPeers: incomingResult.rows.map((row) => ({
        ...row,
        packets_24h: Number(row.packets_24h ?? 0),
        last_seen: row.last_seen ? new Date(row.last_seen).toISOString() : null,
      })),
      heardBy,
      linkHealth,
      advertTrend24h,
      telemetry24h,
      alerts,
      recentPackets: packetResult.rows.map((row) => ({
        ...row,
        time: new Date(row.time).toISOString(),
      })),
    };
    ownerLiveCache.set(selectedNodeId, { ts: Date.now(), data: responseData });
    res.json(responseData);
  } catch (err) {
    console.error('[api] GET /owner/live', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/owner/logout — clear cookie
router.post('/owner/logout', async (_req, res) => {
  res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
  res.json({ ok: true });
});

// POST /api/telemetry/frontend-error — lightweight frontend error reporting
router.post('/telemetry/frontend-error', async (req, res) => {
  try {
    const body = req.body as {
      kind?: string;
      message?: string;
      stack?: string;
      page?: string;
      userAgent?: string;
    };

    const message = String(body.message ?? '').slice(0, 500);
    if (!message) {
      res.status(400).json({ error: 'Missing message' });
      return;
    }

    const ALLOWED_KINDS = new Set(['error', 'warning', 'unhandledrejection', 'crash']);
    const kind = ALLOWED_KINDS.has(String(body.kind)) ? String(body.kind) : 'error';

    await query(
      `INSERT INTO frontend_error_events (kind, message, stack, page, user_agent)
       VALUES ($1, $2, $3, $4, $5)`,
      [
        kind,
        message,
        body.stack ? String(body.stack).slice(0, 4000) : null,
        body.page ? String(body.page).slice(0, 300) : null,
        body.userAgent ? String(body.userAgent).slice(0, 500) : null,
      ],
    );

    res.json({ ok: true });
  } catch (err) {
    console.error('[api] POST /telemetry/frontend-error', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

async function computeChartsData(network: string | undefined, observer: string | undefined): Promise<unknown> {
  const filters = networkFilters(network, observer);

  const PAYLOAD_LABELS: Record<number, string> = {
    0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
    4: 'Advert', 5: 'GroupText', 6: 'GroupData',
    7: 'AnonReq', 8: 'Path', 9: 'Trace', 11: 'Control',
  };

  const [
    phResult, pdResult, rhResult, rdResult,
    ptResult, rpResult, hdResult, pcResult, sumResult, orSummaryResult, orSeriesResult,
    pathHashWidthsResult, multibyteSummaryResult,
  ] = await Promise.all([
      // packets per rolling hour — last 24h (sampled every 5 minutes)
      query(`
        WITH buckets AS (
          SELECT generate_series(
            date_trunc('minute', NOW() - INTERVAL '24 hours'),
            date_trunc('minute', NOW()),
            INTERVAL '5 minutes'
          ) AS bucket
        )
        SELECT b.bucket AS hour, COALESCE(c.count, 0) AS count
        FROM buckets b
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::int AS count
          FROM packets p
          WHERE p.time > b.bucket - INTERVAL '1 hour'
            AND p.time <= b.bucket
            ${filters.packetsAlias('p')}
        ) c ON TRUE
        ORDER BY b.bucket
      `, filters.params),
      // packets per day — last 7d (all observed packet rows)
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      // unique radios heard per rolling hour — last 24h (sampled every 5 minutes)
      query(`
        WITH buckets AS (
          SELECT generate_series(
            date_trunc('minute', NOW() - INTERVAL '24 hours'),
            date_trunc('minute', NOW()),
            INTERVAL '5 minutes'
          ) AS bucket
        )
        SELECT b.bucket AS hour, COALESCE(c.count, 0) AS count
        FROM buckets b
        LEFT JOIN LATERAL (
          SELECT COUNT(DISTINCT p.src_node_id)::int AS count
          FROM packets p
          WHERE p.time > b.bucket - INTERVAL '1 hour'
            AND p.time <= b.bucket
            AND p.src_node_id IS NOT NULL
            ${filters.packetsAlias('p')}
        ) c ON TRUE
        ORDER BY b.bucket
      `, filters.params),
      // unique radios heard per day — last 7d
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(DISTINCT src_node_id) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' AND src_node_id IS NOT NULL ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      // packet types — last 24h (all observed packet rows)
      query(`
        SELECT packet_type, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}
        GROUP BY packet_type ORDER BY count DESC
      `, filters.params),
      // total known repeaters at each hour — cumulative count from nodes.created_at — last 7d
      query(`
        WITH hours AS (
          SELECT generate_series(
            date_trunc('hour', NOW() - INTERVAL '7 days'),
            date_trunc('hour', NOW()),
            INTERVAL '1 hour'
          ) AS hour
        )
        SELECT h.hour,
          (SELECT COUNT(*)
           FROM nodes n
           WHERE (n.role IS NULL OR n.role = 2)
             AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
             ${filters.nodesAlias('n')}
             AND n.created_at <= h.hour + INTERVAL '1 hour') AS count
        FROM hours h
        ORDER BY h.hour
      `, filters.params),
      // hop count distribution — last 7d (all observed packet rows)
      query(`
        SELECT hop_count AS hops, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days'
          AND hop_count IS NOT NULL
          ${filters.packets}
        GROUP BY hop_count ORDER BY hop_count
      `, filters.params),
      // top repeated first-2-hex prefix collisions across known repeaters
      query(`
        WITH prefix_counts AS (
          SELECT SUBSTRING(LOWER(n.node_id) FROM 1 FOR 2) AS prefix, COUNT(*)::int AS node_count
          FROM nodes n
          WHERE n.node_id ~ '^[0-9A-Fa-f]{64}$'
            AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
            AND (n.role IS NULL OR n.role = 2)
            ${filters.nodesAlias('n')}
          GROUP BY 1
          HAVING COUNT(*) > 1
        )
        SELECT
          prefix,
          node_count AS repeats
        FROM prefix_counts
        ORDER BY node_count DESC, prefix ASC
        LIMIT 10
      `, filters.params),
      // summary
      query(`
        SELECT
          (SELECT COUNT(*) FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}) AS total_24h,
          (SELECT COUNT(*) FROM packets WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}) AS total_7d,
          (SELECT COUNT(DISTINCT src_node_id) FROM packets WHERE time > NOW() - INTERVAL '24 hours' AND src_node_id IS NOT NULL ${filters.packets}) AS unique_radios_24h,
          (SELECT COUNT(*) FROM nodes n WHERE (n.role IS NULL OR n.role = 2) AND n.last_seen > NOW() - INTERVAL '7 days' ${filters.nodesAlias('n')}) AS active_repeaters,
          (SELECT COUNT(*) FROM nodes n WHERE (n.role IS NULL OR n.role = 2) AND n.last_seen <= NOW() - INTERVAL '7 days' AND n.last_seen > NOW() - INTERVAL '14 days' ${filters.nodesAlias('n')}) AS stale_repeaters
      `, filters.params),
      // observer regions summary — last 7d (de-duped per region by packet_hash)
      query(`
        SELECT
          COALESCE(NULLIF(TRIM(UPPER(n.iata)), ''), 'UNK') AS iata,
          COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.time > NOW() - INTERVAL '24 hours') AS packets_24h,
          COUNT(DISTINCT p.packet_hash) AS packets_7d,
          COUNT(DISTINCT LOWER(p.rx_node_id)) FILTER (WHERE n.last_seen > NOW() - INTERVAL '1 minute') AS active_observers,
          COUNT(DISTINCT LOWER(p.rx_node_id)) AS observers,
          MAX(p.time)::text AS last_packet_at
        FROM packets p
        LEFT JOIN nodes n ON LOWER(n.node_id) = LOWER(p.rx_node_id)
        WHERE p.time > NOW() - INTERVAL '7 days'
          AND p.rx_node_id IS NOT NULL
          AND p.rx_node_id <> ''
          AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
          ${filters.packetsAlias('p')}
        GROUP BY 1
        ORDER BY packets_7d DESC, iata ASC
      `, filters.params),
      // observer regions sparkline series — last 7d (de-duped per region by packet_hash)
      query(`
        SELECT
          COALESCE(NULLIF(TRIM(UPPER(n.iata)), ''), 'UNK') AS iata,
          time_bucket('1 day', p.time) AS day,
          COUNT(DISTINCT p.packet_hash) AS count
        FROM packets p
        LEFT JOIN nodes n ON LOWER(n.node_id) = LOWER(p.rx_node_id)
        WHERE p.time > NOW() - INTERVAL '7 days'
          AND p.rx_node_id IS NOT NULL
          AND p.rx_node_id <> ''
          AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
          ${filters.packetsAlias('p')}
        GROUP BY 1, 2
        ORDER BY iata ASC, day ASC
      `, filters.params),
      query<{
        hash_hex_len: string;
        hop_count: string;
      }>(
        `SELECT length(h)::text AS hash_hex_len, COUNT(*)::text AS hop_count
         FROM packets p
         CROSS JOIN LATERAL unnest(p.path_hashes) AS h
         WHERE p.time > NOW() - INTERVAL '24 hours'
           ${filters.packetsAlias('p')}
         GROUP BY 1`,
        filters.params,
      ),
      query<{
        latest_multibyte_at: string | null;
        latest_multibyte_hash: string | null;
        multibyte_packets_24h: string;
        fully_decoded_multibyte_24h: string;
        latest_fully_decoded_at: string | null;
        latest_fully_decoded_hash: string | null;
        latest_fully_decoded_hops: string | null;
        latest_fully_decoded_path: string | null;
        latest_fully_decoded_nodes: Array<{
          ord: number;
          node_id: string;
          name: string | null;
          lat: number | null;
          lon: number | null;
          last_seen: string | null;
        }> | null;
        longest_fully_decoded_at: string | null;
        longest_fully_decoded_hash: string | null;
        longest_fully_decoded_hops: string | null;
        longest_fully_decoded_path: string | null;
        longest_fully_decoded_nodes: Array<{
          ord: number;
          node_id: string;
          name: string | null;
          lat: number | null;
          lon: number | null;
          last_seen: string | null;
        }> | null;
      }>(
        `WITH multibyte AS (
           SELECT DISTINCT ON (p.packet_hash)
             p.packet_hash,
             p.time,
             p.path_hashes
           FROM packets p
           WHERE p.time > NOW() - INTERVAL '24 hours'
             AND p.path_hash_size_bytes > 1
             AND COALESCE(array_length(p.path_hashes, 1), 0) > 0
             ${filters.packetsAlias('p')}
           ORDER BY p.packet_hash,
                    COALESCE(array_length(p.path_hashes, 1), 0) DESC,
                    COALESCE(p.hop_count, 0) DESC,
                    p.time DESC
         ),
         hop_matches AS (
           SELECT
             m.packet_hash,
             h.hash,
             h.ord,
             COUNT(DISTINCT n.node_id)::int AS match_count,
             MIN(n.node_id) FILTER (WHERE n.node_id IS NOT NULL) AS matched_node_id
           FROM multibyte m
           CROSS JOIN LATERAL unnest(m.path_hashes) WITH ORDINALITY AS h(hash, ord)
           LEFT JOIN nodes n
             ON UPPER(LEFT(n.node_id, LENGTH(h.hash))) = UPPER(h.hash)
            AND (n.role IS NULL OR n.role = 2)
            AND n.lat IS NOT NULL
            AND n.lon IS NOT NULL
           GROUP BY 1, 2, 3
         ),
         packet_eval AS (
           SELECT
             m.packet_hash,
             MAX(m.time)::text AS packet_time,
             BOOL_AND(hm.match_count = 1) AS all_unique,
             COUNT(hm.ord)::int AS hop_count,
             COUNT(DISTINCT hm.matched_node_id) FILTER (WHERE hm.matched_node_id IS NOT NULL)::int AS unique_nodes,
             string_agg(COALESCE(n.name, hm.matched_node_id, hm.hash), ' -> ' ORDER BY hm.ord) AS decoded_path,
             jsonb_agg(
               jsonb_build_object(
                 'ord', hm.ord,
                 'node_id', hm.matched_node_id,
                 'name', n.name,
                 'lat', n.lat,
                 'lon', n.lon,
                 'last_seen', n.last_seen::text
               )
               ORDER BY hm.ord
             ) AS decoded_nodes
           FROM multibyte m
           JOIN hop_matches hm ON hm.packet_hash = m.packet_hash
           LEFT JOIN nodes n ON LOWER(n.node_id) = LOWER(hm.matched_node_id)
           GROUP BY m.packet_hash
         ),
         latest_multibyte AS (
           SELECT m.packet_hash, m.time
           FROM multibyte m
           ORDER BY m.time DESC, m.packet_hash DESC
           LIMIT 1
         ),
         latest_fully_decoded AS (
           SELECT pe.packet_hash, pe.packet_time, pe.hop_count, pe.decoded_path, pe.decoded_nodes
           FROM packet_eval pe
           WHERE pe.all_unique AND pe.unique_nodes = pe.hop_count
           ORDER BY pe.packet_time DESC, pe.packet_hash DESC
           LIMIT 1
         ),
         longest_fully_decoded AS (
           SELECT pe.packet_hash, pe.packet_time, pe.hop_count, pe.decoded_path, pe.decoded_nodes
           FROM packet_eval pe
           WHERE pe.all_unique AND pe.unique_nodes = pe.hop_count
           ORDER BY pe.hop_count DESC, pe.packet_time DESC, pe.packet_hash DESC
           LIMIT 1
         )
         SELECT
           (SELECT MAX(time)::text FROM multibyte) AS latest_multibyte_at,
           (SELECT packet_hash FROM latest_multibyte) AS latest_multibyte_hash,
           (SELECT COUNT(*)::text FROM multibyte) AS multibyte_packets_24h,
           (SELECT COUNT(*)::text FROM packet_eval WHERE all_unique AND unique_nodes = hop_count) AS fully_decoded_multibyte_24h,
           (SELECT packet_time FROM latest_fully_decoded) AS latest_fully_decoded_at,
           (SELECT packet_hash FROM latest_fully_decoded) AS latest_fully_decoded_hash,
           (SELECT hop_count::text FROM latest_fully_decoded) AS latest_fully_decoded_hops,
           (SELECT decoded_path FROM latest_fully_decoded) AS latest_fully_decoded_path,
           (SELECT decoded_nodes FROM latest_fully_decoded) AS latest_fully_decoded_nodes,
           (SELECT packet_time FROM longest_fully_decoded) AS longest_fully_decoded_at,
           (SELECT packet_hash FROM longest_fully_decoded) AS longest_fully_decoded_hash,
           (SELECT hop_count::text FROM longest_fully_decoded) AS longest_fully_decoded_hops,
           (SELECT decoded_path FROM longest_fully_decoded) AS longest_fully_decoded_path,
           (SELECT decoded_nodes FROM longest_fully_decoded) AS longest_fully_decoded_nodes`,
        filters.params,
      ),
    ]);

    const peakRow = phResult.rows.reduce(
      (best: any, r: any) => (Number(r.count) > Number(best?.count ?? 0) ? r : best),
      null
    );

    const fmtHour = (ts: Date | string) => {
      const d = new Date(ts);
      return `${d.getHours().toString().padStart(2, '0')}:00`;
    };
    const fmtHourMinute = (ts: Date | string) => {
      const d = new Date(ts);
      return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
    };
    const fmtDay = (ts: Date | string) => {
      const d = new Date(ts);
      return d.toLocaleDateString('en-GB', { weekday: 'short', month: 'short', day: 'numeric' });
    };

    const observerRegionsByIata = new Map<string, {
      iata: string;
      activeObservers: number;
      observers: number;
      packets24h: number;
      packets7d: number;
      lastPacketAt: string | null;
      series: { day: string; count: number }[];
    }>();

    for (const row of orSummaryResult.rows) {
      const iata = String(row.iata ?? 'UNK');
      observerRegionsByIata.set(iata, {
        iata,
        activeObservers: Number(row.active_observers ?? 0),
        observers: Number(row.observers ?? 0),
        packets24h: Number(row.packets_24h ?? 0),
        packets7d: Number(row.packets_7d ?? 0),
        lastPacketAt: row.last_packet_at ?? null,
        series: [],
      });
    }

    for (const row of orSeriesResult.rows) {
      const iata = String(row.iata ?? 'UNK');
      const region = observerRegionsByIata.get(iata);
      if (!region) continue;
      region.series.push({
        day: fmtDay(row.day),
        count: Number(row.count ?? 0),
      });
    }

    const widthToBucket: Record<number, 'one_byte' | 'two_byte' | 'three_byte'> = {
      2: 'one_byte',
      4: 'two_byte',
      6: 'three_byte',
    };
    const pathHashStats = {
      one_byte: 0,
      two_byte: 0,
      three_byte: 0,
    };

    for (const row of pathHashWidthsResult.rows) {
      const width = Number(row.hash_hex_len ?? 0);
      const bucket = widthToBucket[width];
      if (!bucket) continue;
      pathHashStats[bucket] += Number(row.hop_count ?? 0);
    }

    const multibyteRow = multibyteSummaryResult.rows[0];
    const latestFullyDecodedNodes = maskDecodedPathNodes(multibyteRow?.latest_fully_decoded_nodes);
    const longestFullyDecodedNodes = maskDecodedPathNodes(multibyteRow?.longest_fully_decoded_nodes);

  return {
    packetsPerHour:  phResult.rows.map(r => ({ hour: fmtHourMinute(r.hour), count: Number(r.count) })),
    packetsPerDay:   pdResult.rows.map(r => ({ day: fmtDay(r.day), count: Number(r.count) })),
    radiosPerHour:   rhResult.rows.map(r => ({ hour: fmtHourMinute(r.hour), count: Number(r.count) })),
    radiosPerDay:    rdResult.rows.map(r => ({ day: fmtDay(r.day), count: Number(r.count) })),
    packetTypes:     ptResult.rows.map(r => ({ label: PAYLOAD_LABELS[Number(r.packet_type)] ?? `Type${r.packet_type}`, count: Number(r.count) })),
    repeatersPerDay: rpResult.rows.map(r => ({ hour: fmtDay(r.hour), count: Number(r.count ?? 0) })),
    hopDistribution: hdResult.rows.map(r => ({ hops: Number(r.hops), count: Number(r.count) })),
    topChatters: [],
    prefixCollisions: pcResult.rows.map(r => ({
      prefix: String(r.prefix ?? '').toUpperCase(),
      repeats: Number(r.repeats),
    })),
    observerRegions: Array.from(observerRegionsByIata.values()),
    pathHashes: {
      last24hHops: pathHashStats,
      multibytePackets24h: Number(multibyteRow?.multibyte_packets_24h ?? 0),
      fullyDecodedMultibyte24h: Number(multibyteRow?.fully_decoded_multibyte_24h ?? 0),
      latestMultibyteAt: multibyteRow?.latest_multibyte_at ?? null,
      latestMultibyteHash: multibyteRow?.latest_multibyte_hash ?? null,
      latestFullyDecodedAt: multibyteRow?.latest_fully_decoded_at ?? null,
      latestFullyDecodedHash: multibyteRow?.latest_fully_decoded_hash ?? null,
      latestFullyDecodedHops: Number(multibyteRow?.latest_fully_decoded_hops ?? 0) || null,
      latestFullyDecodedPath: multibyteRow?.latest_fully_decoded_path ?? null,
      latestFullyDecodedNodes,
      longestFullyDecodedAt: multibyteRow?.longest_fully_decoded_at ?? null,
      longestFullyDecodedHash: multibyteRow?.longest_fully_decoded_hash ?? null,
      longestFullyDecodedHops: Number(multibyteRow?.longest_fully_decoded_hops ?? 0) || null,
      longestFullyDecodedPath: multibyteRow?.longest_fully_decoded_path ?? null,
      longestFullyDecodedNodes,
    },
    summary: {
      totalPackets24h:  Number(sumResult.rows[0].total_24h),
      totalPackets7d:   Number(sumResult.rows[0].total_7d),
      uniqueRadios24h:  Number(sumResult.rows[0].unique_radios_24h),
      activeRepeaters:  Number(sumResult.rows[0].active_repeaters ?? 0),
      staleRepeaters:   Number(sumResult.rows[0].stale_repeaters ?? 0),
      peakHour:         peakRow ? fmtHour(peakRow.hour) : null,
      peakHourCount:    peakRow ? Number(peakRow.count) : 0,
    },
  };
}

async function getCachedChartsData(network: string | undefined, observer: string | undefined): Promise<unknown> {
  const key = `${network ?? 'all'}:${observer ?? ''}`;
  const cached = chartsCache.get(key);
  if (cached && Date.now() - cached.ts < CHARTS_CACHE_TTL_MS) return cached.data;

  // Deduplicate concurrent requests for the same key
  const inflight = chartsInflight.get(key);
  if (inflight) return inflight;

  const promise = computeChartsData(network, observer).then((data) => {
    chartsCache.set(key, { ts: Date.now(), data });
    chartsInflight.delete(key);
    return data;
  }).catch((err) => {
    chartsInflight.delete(key);
    throw err;
  });

  chartsInflight.set(key, promise);
  return promise;
}

// Pre-warm charts cache for common networks on startup, then refresh every 30 minutes.
// This ensures the first visitor always gets a cached response.
{
  const CHARTS_WARMUP_NETWORKS = (process.env['WARMUP_NETWORKS'] ?? 'teesside,ukmesh')
    .split(',').map((s: string) => s.trim()).filter(Boolean);

  const warmCharts = () => {
    for (const net of CHARTS_WARMUP_NETWORKS) {
      getCachedChartsData(net, undefined).catch(() => { /* best-effort */ });
    }
  };

  // Delay slightly so the DB pool is ready
  setTimeout(warmCharts, 5_000);
  setInterval(warmCharts, CHARTS_CACHE_TTL_MS);
}

// GET /api/stats/charts
router.get('/stats/charts', STATS_CHARTS_LIMITER, async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    res.json(await getCachedChartsData(network, observer));
  } catch (err) {
    console.error('[api] GET /stats/charts', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/observer-activity — MQTT observer nodes with 24h receive counts
router.get('/observer-activity', EXPENSIVE_LIMITER, async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const params: unknown[] = [];
    const conditions: string[] = [`p.time > NOW() - INTERVAL '24 hours'`];
    if (network) {
      params.push(network);
      conditions.push(`n.network = $${params.length}`);
    }
    const where = conditions.join(' AND ');
    const result = await query<{ node_id: string; name: string | null; rx_24h: string; tx_24h: string }>(
      `SELECT
         n.node_id,
         n.name,
         COUNT(p.packet_hash) FILTER (WHERE p.rx_node_id  = n.node_id) AS rx_24h,
         COUNT(p.packet_hash) FILTER (WHERE p.src_node_id = n.node_id) AS tx_24h
       FROM nodes n
       JOIN packets p ON (p.rx_node_id = n.node_id OR p.src_node_id = n.node_id)
       WHERE ${where}
       GROUP BY n.node_id, n.name
       HAVING COUNT(p.packet_hash) FILTER (WHERE p.rx_node_id = n.node_id) > 0
       ORDER BY rx_24h DESC`,
      params,
    );
    res.json(result.rows.map(r => ({ ...r, rx_24h: Number(r.rx_24h), tx_24h: Number(r.tx_24h) })));
  } catch (err) {
    console.error('[api] GET /observer-activity', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/cross-network-connectivity
// Detects real cross-network propagation by matching the same packet_hash seen on
// both sides. Hop count determines origin side: fewer hops = closer to source.
//   Outbound: MME observers see it with fewer hops, non-MME also received it within 120s
//   Inbound:  non-MME observers see it with fewer hops, MME also received it within 120s
router.get('/cross-network-connectivity', EXPENSIVE_LIMITER, async (_req, res) => {
  const result = await query<{ last_inbound: string | null; last_outbound: string | null }>(`
    WITH mme_observers AS (
      SELECT DISTINCT rx_node_id AS node_id
      FROM packets
      WHERE time > NOW() - INTERVAL '24 hours'
        AND network = 'teesside'
        AND rx_node_id IS NOT NULL
    ),
    cross_heard AS (
      SELECT
        p.packet_hash,
        MIN(p.hop_count) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers))     AS mme_min_hops,
        MIN(p.hop_count) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) AS other_min_hops,
        MIN(p.time)      FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers))     AS mme_first_seen,
        MIN(p.time)      FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) AS other_first_seen
      FROM packets p
      WHERE p.time > NOW() - INTERVAL '2 hours'
        AND p.hop_count IS NOT NULL
        AND p.rx_node_id IS NOT NULL
        AND p.packet_hash IS NOT NULL
      GROUP BY p.packet_hash
      HAVING
        MIN(p.hop_count) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers))     IS NOT NULL
        AND MIN(p.hop_count) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers)) IS NOT NULL
        AND ABS(EXTRACT(EPOCH FROM (
          MIN(p.time) FILTER (WHERE p.rx_node_id IN (SELECT node_id FROM mme_observers)) -
          MIN(p.time) FILTER (WHERE p.rx_node_id NOT IN (SELECT node_id FROM mme_observers))
        ))) <= 120
    )
    SELECT
      MAX(mme_first_seen)   FILTER (WHERE other_min_hops < mme_min_hops) AS last_inbound,
      MAX(other_first_seen) FILTER (WHERE mme_min_hops  < other_min_hops) AS last_outbound
    FROM cross_heard
    WHERE mme_min_hops != other_min_hops
  `);

  const lastInbound  = result.rows[0]?.last_inbound  ?? null;
  const lastOutbound = result.rows[0]?.last_outbound ?? null;

  res.json({
    inbound:     !!lastInbound,
    outbound:    !!lastOutbound,
    lastInbound,
    lastOutbound,
    windowHours: 2,
    checkedAt:   new Date().toISOString(),
  });
});

// GET /api/radio-history?target=<nodeName>&limit=<n> — proxies radio bot POST /history
router.get('/radio-history', async (req, res) => {
  const radioBotUrl = process.env['RADIO_BOT_URL'] ?? 'http://meshcore-radio-bot:3011';
  const target = String(req.query['target'] ?? '').trim();
  const limit  = Math.min(Number(req.query['limit'] ?? 168), 500);
  if (!target) { res.status(400).json({ error: 'target required' }); return; }
  try {
    const upstream = await fetch(`${radioBotUrl}/history`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ target, limit }),
    });
    if (!upstream.ok) { res.status(502).json({ error: 'radio bot unavailable' }); return; }
    res.json(await upstream.json());
  } catch {
    res.status(503).json({ error: 'radio bot unreachable' });
  }
});

// GET /api/radio-stats — proxies radio bot GET /state (port 3011)
router.get('/radio-stats', async (_req, res) => {
  const radioBotUrl = process.env['RADIO_BOT_URL'] ?? 'http://meshcore-radio-bot:3011';
  try {
    const upstream = await fetch(`${radioBotUrl}/state`);
    if (!upstream.ok) {
      res.status(502).json({ error: 'radio bot unavailable' });
      return;
    }
    const data = await upstream.json();
    res.json(data);
  } catch {
    res.status(503).json({ error: 'radio bot unreachable' });
  }
});

export default router;
