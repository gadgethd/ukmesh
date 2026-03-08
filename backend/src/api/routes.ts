import { Request, Response, Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import mqtt from 'mqtt';
import { getNodes, getNodeHistory, getRecentPacketEvents, getRecentPackets, query, MIN_LINK_OBSERVATIONS } from '../db/index.js';
import { addOwnerNodeForUsername, getMappedOwnerNodeIds, getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { getWorkerHealthOverview } from '../health/status.js';
import { resolveRequestNetwork } from '../http/requestScope.js';
import { resolveBetaPathForPacketHash, resolveMultiObserverBetaPath } from '../path-beta/resolver.js';

const router = Router();
const OWNER_COOKIE_NAME = 'meshcore_owner_session';
const OWNER_LIVE_CACHE_TTL_MS = 5_000;
const ownerLiveCache = new Map<string, { ts: number; data: unknown }>();
const OWNER_SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const OWNER_LOGIN_LIMITER = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 8,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many login attempts, try again in 15 minutes' },
});

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

  const mappedNodeIds = await getMappedOwnerNodeIds();
  const res = await query<{ node_id: string }>(
    `SELECT n.node_id
     FROM nodes n
     WHERE n.role = 2
       AND COALESCE(n.network, '') <> 'test'
       AND n.last_seen > NOW() - INTERVAL '30 minutes'
       AND NOT (LOWER(n.node_id) = ANY($1::text[]))
       AND EXISTS (
         SELECT 1
         FROM packets p
         WHERE p.rx_node_id = n.node_id
           AND p.time > NOW() - INTERVAL '30 minutes'
           AND p.topic LIKE ('%/' || n.node_id || '/packets')
       )
     ORDER BY n.last_seen DESC
     LIMIT 2`,
    [mappedNodeIds],
  );

  if (res.rows.length !== 1) return [];

  const nodeId = res.rows[0]?.node_id?.trim().toLowerCase();
  if (!nodeId || !/^[0-9a-f]{64}$/.test(nodeId)) return [];

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
      },
      roadmap: [
        'Per-node packet history for owner nodes',
        'Advert and heartbeat trend views',
        'RSSI and SNR trend views from observer reports',
        'Node placement planner (coming next)',
      ],
    };
  }

  const [ownedNodes, packetSummary] = await Promise.all([
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
  nodes: string;
  nodesAlias: (alias: string) => string;
};

function normalizeObserverQuery(value: unknown): string | undefined {
  const observer = String(value ?? '').trim().toLowerCase();
  return observer && /^[0-9a-f]{64}$/.test(observer) ? observer : undefined;
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
  else packetConditions.push(`network IS DISTINCT FROM 'test'`);
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
    nodes: nodeConditions().length > 0 ? `AND ${nodeConditions().join(' AND ')}` : '',
    nodesAlias: (alias: string) => {
      const conditions = nodeConditions(alias);
      return conditions.length > 0 ? `AND ${conditions.join(' AND ')}` : '';
    },
  };
}

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
router.get('/path-beta/resolve', async (req, res) => {
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
    const resolved = await resolveBetaPathForPacketHash(packetHash, network, observer);
    if (!resolved) {
      res.status(404).json({ error: 'Packet not found' });
      return;
    }
    res.json(resolved);
  } catch (err) {
    console.error('[api] GET /path-beta/resolve', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-beta/resolve-multi?hash=<packetHash>&network=teesside|ukmesh|all
router.get('/path-beta/resolve-multi', async (req, res) => {
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
    const resolved = await resolveMultiObserverBetaPath(packetHash, network);
    if (!resolved) {
      res.status(404).json({ error: 'Packet not found' });
      return;
    }
    res.json(resolved);
  } catch (err) {
    console.error('[api] GET /path-beta/resolve-multi', (err as Error).message);
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
    const [mqttCount, packetCount, staleCount, mapNodeCount, totalNodeCount, longestHopCount] = await Promise.all([
      query(`
        WITH test_active AS (
          SELECT rx_node_id FROM packets WHERE rx_node_id IS NOT NULL AND rx_node_id <> ''
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
      query(`SELECT hop_count AS count,
               COALESCE(
                 CASE WHEN payload->>'hash' ~ '^[0-9A-Fa-f]{16}$' THEN payload->>'hash' END,
                 CASE WHEN packet_hash   ~ '^[0-9A-Fa-f]{16}$' THEN packet_hash END
               ) AS hash
             FROM packets
             WHERE hop_count IS NOT NULL
               AND (payload->>'hash' ~ '^[0-9A-Fa-f]{16}$'
                    OR packet_hash   ~ '^[0-9A-Fa-f]{16}$')
               ${filters.packets}
             ORDER BY hop_count DESC LIMIT 1`, filters.params),
    ]);
    res.json({
      mqttNodes:      Number(mqttCount.rows[0]?.count ?? 0),
      staleNodes:     Number(staleCount.rows[0]?.count ?? 0),
      packetsDay:     Number(packetCount.rows[0]?.count ?? 0),
      mapNodes:       Number(mapNodeCount.rows[0]?.count ?? 0),
      totalNodes:     Number(totalNodeCount.rows[0]?.count ?? 0),
      longestHop:     Number(longestHopCount.rows[0]?.count ?? 0),
      longestHopHash: (longestHopCount.rows[0]?.hash as string | undefined) ?? null,
    });
  } catch (err) {
    console.error('[api] GET /stats', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});


// GET /api/coverage — all stored viewshed polygons (excludes hidden 🚫 nodes)
router.get('/coverage', async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
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
router.get('/path-learning', async (req, res) => {
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

// GET /api/path-sim/latest — latest path simulation run summary
router.get('/path-sim/latest', async (req, res) => {
  try {
    const network = resolveRequestNetwork(req.query['network'], req.headers);
    const hasNetwork = Boolean(network && network !== 'all');
    const result = await query<{
      id: number;
      started_at: string;
      completed_at: string | null;
      network: string;
      packets_total: number;
      packets_eligible: number;
      packets_fully_resolved: number;
      packets_unresolved: number;
      truncated_searches: number;
      permutation_histogram: Record<string, number>;
      remaining_hops_histogram: Record<string, number>;
      summary: Record<string, unknown>;
    }>(
      `SELECT id, started_at::text, completed_at::text, network,
              packets_total, packets_eligible, packets_fully_resolved, packets_unresolved, truncated_searches,
              permutation_histogram, remaining_hops_histogram, summary
       FROM path_simulation_runs
       ${hasNetwork ? 'WHERE network = $1' : ''}
       ORDER BY started_at DESC
       LIMIT 1`,
      hasNetwork ? [network] : [],
    );
    if (result.rows.length < 1) {
      res.status(404).json({ error: 'No path simulation runs found yet' });
      return;
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[api] GET /path-sim/latest', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-sim/history?limit=20&network=all
router.get('/path-sim/history', async (req, res) => {
  try {
    const network = resolveRequestNetwork(req.query['network'], req.headers);
    const hasNetwork = Boolean(network && network !== 'all');
    const limit = Math.min(200, Math.max(1, Number(req.query['limit'] ?? 20)));
    const result = await query<{
      id: number;
      started_at: string;
      completed_at: string | null;
      network: string;
      packets_total: number;
      packets_eligible: number;
      packets_fully_resolved: number;
      packets_unresolved: number;
      truncated_searches: number;
      permutation_histogram: Record<string, number>;
      remaining_hops_histogram: Record<string, number>;
      summary: Record<string, unknown>;
    }>(
      `SELECT id, started_at::text, completed_at::text, network,
              packets_total, packets_eligible, packets_fully_resolved, packets_unresolved, truncated_searches,
              permutation_histogram, remaining_hops_histogram, summary
       FROM path_simulation_runs
       ${hasNetwork ? 'WHERE network = $1' : ''}
       ORDER BY started_at DESC
       LIMIT ${hasNetwork ? '$2' : '$1'}`,
      hasNetwork ? [network, limit] : [limit],
    );
    res.json({
      count: result.rows.length,
      runs: result.rows,
    });
  } catch (err) {
    console.error('[api] GET /path-sim/history', (err as Error).message);
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
    const mqttUsername = String(body?.mqttUsername ?? '').trim().slice(0, 32);
    const mqttPassword = String(body?.mqttPassword ?? '').trim().slice(0, 32);
    if (!mqttUsername || !mqttPassword) {
      res.status(400).json({ error: 'Missing MQTT username or password' });
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
      sameSite: 'lax',
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
      }>(
        `SELECT node_id, name, network, iata, advert_count, last_seen, lat, lon
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
         LIMIT 5`,
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

    const alerts: Array<{ level: 'info' | 'warn' | 'error'; message: string }> = [];
    const ownerLastSeenMs = ownerNode.last_seen ? new Date(ownerNode.last_seen).getTime() : 0;
    const minsSinceSeen = ownerLastSeenMs ? Math.max(0, Math.round((Date.now() - ownerLastSeenMs) / 60000)) : null;
    const adverts24h = advertTrend24h.reduce((sum, point) => sum + point.adverts, 0);
    const viableLinks = linkHealth.filter((link) => link.itm_viable || link.force_viable);
    if (minsSinceSeen == null) alerts.push({ level: 'error', message: 'No last-seen timestamp is available for this repeater.' });
    else if (minsSinceSeen >= 120) alerts.push({ level: 'error', message: `Repeater has not been seen for ${minsSinceSeen} minutes.` });
    else if (minsSinceSeen >= 30) alerts.push({ level: 'warn', message: `Repeater has been quiet for ${minsSinceSeen} minutes.` });
    else alerts.push({ level: 'info', message: 'Repeater is active and has checked in recently.' });

    if (adverts24h < 1) alerts.push({ level: 'warn', message: 'No advert packets from this repeater were recorded in the last 24 hours.' });
    if (heardBy.length < 1) alerts.push({ level: 'warn', message: 'No other nodes have heard this repeater in the last 7 days.' });
    if (viableLinks.length < 1) alerts.push({ level: 'warn', message: 'No viable RF links are currently stored for this repeater.' });

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

// GET /api/stats/charts
router.get('/stats/charts', async (req, res) => {
  try {
    const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserverQuery(req.query['observer']);
    const filters = networkFilters(network, observer);

    const PAYLOAD_LABELS: Record<number, string> = {
      0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
      4: 'Advert', 5: 'GroupText', 6: 'GroupData',
      7: 'AnonReq', 8: 'Path', 9: 'Trace',
    };

    const [
      phResult, pdResult, rhResult, rdResult,
      ptResult, rpResult, hdResult, pcResult, sumResult,
    ] = await Promise.all([
      // packets per hour — last 24h (all observed packet rows)
      query(`
        SELECT time_bucket('1 hour', time) AS hour, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}
        GROUP BY hour ORDER BY hour
      `, filters.params),
      // packets per day — last 7d (all observed packet rows)
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(*) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      // unique radios heard per hour — last 24h (distinct transmitting nodes)
      query(`
        SELECT time_bucket('1 hour', time) AS hour, COUNT(DISTINCT src_node_id) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' AND src_node_id IS NOT NULL ${filters.packets}
        GROUP BY hour ORDER BY hour
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
    ]);

    const peakRow = phResult.rows.reduce(
      (best: any, r: any) => (Number(r.count) > Number(best?.count ?? 0) ? r : best),
      null
    );

    const fmtHour = (ts: Date | string) => {
      const d = new Date(ts);
      return `${d.getHours().toString().padStart(2, '0')}:00`;
    };
    const fmtDay = (ts: Date | string) => {
      const d = new Date(ts);
      return d.toLocaleDateString('en-GB', { weekday: 'short', month: 'short', day: 'numeric' });
    };

    res.json({
      packetsPerHour:  phResult.rows.map(r => ({ hour: fmtHour(r.hour), count: Number(r.count) })),
      packetsPerDay:   pdResult.rows.map(r => ({ day: fmtDay(r.day), count: Number(r.count) })),
      radiosPerHour:   rhResult.rows.map(r => ({ hour: fmtHour(r.hour), count: Number(r.count) })),
      radiosPerDay:    rdResult.rows.map(r => ({ day: fmtDay(r.day), count: Number(r.count) })),
      packetTypes:     ptResult.rows.map(r => ({ label: PAYLOAD_LABELS[Number(r.packet_type)] ?? `Type${r.packet_type}`, count: Number(r.count) })),
      repeatersPerDay: rpResult.rows.map(r => ({ hour: fmtDay(r.hour), count: Number(r.count ?? 0) })),
      hopDistribution: hdResult.rows.map(r => ({ hops: Number(r.hops), count: Number(r.count) })),
      // Back-compat for cached older frontend bundles that still read topChatters.
      topChatters: [],
      prefixCollisions: pcResult.rows.map(r => ({
        prefix: String(r.prefix ?? '').toUpperCase(),
        repeats: Number(r.repeats),
      })),
      summary: {
        totalPackets24h:  Number(sumResult.rows[0].total_24h),
        totalPackets7d:   Number(sumResult.rows[0].total_7d),
        uniqueRadios24h:  Number(sumResult.rows[0].unique_radios_24h),
        activeRepeaters:  Number(sumResult.rows[0].active_repeaters ?? 0),
        staleRepeaters:   Number(sumResult.rows[0].stale_repeaters ?? 0),
        peakHour:         peakRow ? fmtHour(peakRow.hour) : null,
        peakHourCount:    peakRow ? Number(peakRow.count) : 0,
      },
    });
  } catch (err) {
    console.error('[api] GET /stats/charts', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
