import { Request, Response, Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { isIP } from 'node:net';
import mqtt from 'mqtt';
import { getNodes, getNodeHistory, getNodeAdverts, getPathHistoryCache, getRecentPacketEvents, getRecentPackets, query } from '../db/index.js';
import { addOwnerNodeForUsername, getBestNodeForMqttUsername, getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { resolveRequestNetwork } from '../http/requestScope.js';
import { getResolveCache, setResolveCache } from '../path-beta/resolveCache.js';
import { resolvePool } from '../path-beta/resolvePool.js';
import healthRoutes from './routes/health.js';
import { registerMiscRoutes } from './routes/misc.js';
import { registerNodeRoutes } from './routes/nodes.js';
import nodeStatusRoutes from './routes/nodeStatus.js';
import radioRoutes from './routes/radio.js';
import { registerCoverageRoutes } from './routes/coverage.js';
import { registerOwnerRoutes } from './routes/owner.js';
import { registerPathingRoutes } from './routes/pathing.js';
import { registerStatsRoutes } from './routes/stats.js';
import { registerTelemetryRoutes } from './routes/telemetry.js';
import { normalizeObserverQuery } from './utils/observer.js';

const router = Router();
router.use(healthRoutes);
router.use(nodeStatusRoutes);
router.use(radioRoutes);
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
const CROSS_NETWORK_CACHE_TTL_MS  = 60_000;  // dashboard polls every minute; avoid re-running the join-heavy query
const statsCache         = new Map<string, { ts: number; data: unknown }>();
const inferredNodesCache = new Map<string, { ts: number; data: unknown }>();
const pathHistoryCache   = new Map<string, { ts: number; data: unknown }>();
const coverageCache      = new Map<string, { ts: number; data: unknown }>();
const chartsCache        = new Map<string, { ts: number; data: unknown }>();
const crossNetworkCache  = new Map<string, { ts: number; data: unknown }>();
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
      .map((value) => String(value).trim().toUpperCase())
      .filter((value) => /^[0-9A-F]{64}$/.test(value));
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
       WHERE node_id = ANY($1::text[])
       ORDER BY last_seen DESC NULLS LAST`,
      [nodeIds],
    ),
    query<{ packets_24h: number; packets_7d: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours')::int AS packets_24h,
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '7 days')::int AS packets_7d
       FROM packets
       WHERE src_node_id = ANY($1::text[])`,
      [nodeIds],
    ),
    query<{ packets_24h: number }>(
      `SELECT
         COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '24 hours')::int AS packets_24h
       FROM packets
       WHERE rx_node_id = ANY($1::text[])`,
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
    packetConditions.push(`COALESCE(rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
  }
  if (observerParam) packetConditions.push(`rx_node_id = ${observerParam}`);

  const nodeConditions = (alias?: string) => {
    const prefix = alias ? `${alias}.` : '';
    const conditions: string[] = [];
    if (networkParam) conditions.push(`${prefix}network = ${networkParam}`);
    else conditions.push(`${prefix}network IS DISTINCT FROM 'test'`);
    if (observerParam) {
      conditions.push(
        `(
          ${prefix}node_id = ${observerParam}
          OR EXISTS (
            SELECT 1
            FROM packets p
            WHERE p.rx_node_id = ${observerParam}
              ${networkParam ? `AND p.network = ${networkParam}` : ''}
              AND p.src_node_id = ${prefix}node_id
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
        conditions.push(`COALESCE(${prefix}rx_node_id, '') NOT IN (SELECT node_id FROM nodes WHERE network = 'test')`);
      }
      if (observerParam) conditions.push(`${prefix}rx_node_id = ${observerParam}`);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
    nodes: nodeConditions().length > 0 ? `AND ${nodeConditions().join(' AND ')}` : '',
    nodesAlias: (alias: string) => {
      const conditions = nodeConditions(alias);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
  };
}

registerCoverageRoutes(router, {
  coverageCache,
  coverageCacheTtlMs: COVERAGE_CACHE_TTL_MS,
  coverageLimiter: COVERAGE_LIMITER,
  networkFilters,
  query,
});
registerNodeRoutes(router, {
  getNodes,
  getNodeHistory,
  getNodeAdverts,
  query,
  requireLocalOnly,
  networkFilters,
  inferredNodesCache,
  inferredNodesCacheTtlMs: INFERRED_NODES_CACHE_TTL_MS,
});
registerMiscRoutes(router, {
  query,
  getRecentPackets,
  getRecentPacketEvents,
});
registerOwnerRoutes(router, {
  ownerCookieName: OWNER_COOKIE_NAME,
  ownerLiveCacheTtlMs: OWNER_LIVE_CACHE_TTL_MS,
  ownerLiveCache,
  ownerSessionTtlMs: OWNER_SESSION_TTL_MS,
  mqttUsernameMaxLen: MQTT_USERNAME_MAX_LEN,
  mqttPasswordMaxLen: MQTT_PASSWORD_MAX_LEN,
  ownerLoginLimiter: OWNER_LOGIN_LIMITER,
  hasControlChars,
  verifyMqttCredentials,
  resolveOwnerNodeIds,
  autoLinkOwnerNodeIds,
  buildOwnerDashboard,
  encryptOwnerSession,
  isSecureRequest,
  getOwnerSession,
  requireOwnerSession,
  query,
});
registerPathingRoutes(router, {
  pathBetaLimiter: PATH_BETA_LIMITER,
  pathHistoryLimiter: PATH_HISTORY_LIMITER,
  pathLearningLimiter: PATH_LEARNING_LIMITER,
  pathHistoryCache,
  pathHistoryCacheTtlMs: PATH_HISTORY_CACHE_TTL_MS,
  getResolveCache,
  setResolveCache,
  resolvePool,
  getPathHistoryCache,
  query,
});
registerStatsRoutes(router, {
  statsCache,
  statsCacheTtlMs: STATS_CACHE_TTL_MS,
  chartsCache,
  chartsCacheTtlMs: CHARTS_CACHE_TTL_MS,
  chartsInflight,
  crossNetworkCache,
  crossNetworkCacheTtlMs: CROSS_NETWORK_CACHE_TTL_MS,
  expensiveLimiter: EXPENSIVE_LIMITER,
  statsChartsLimiter: STATS_CHARTS_LIMITER,
  networkFilters,
  query,
  maskDecodedPathNodes,
});
registerTelemetryRoutes(router, { query });

export default router;
