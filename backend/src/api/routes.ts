import { Request, Response, Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import mqtt from 'mqtt';
import { getNodes, getNodeHistory, getRecentPackets, query, MIN_LINK_OBSERVATIONS } from '../db/index.js';
import { getWorkerHealthOverview } from '../health/status.js';

const router = Router();
const OWNER_COOKIE_NAME = 'meshcore_owner_session';
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
};

function getOwnerCookieKey(): Buffer {
  const secret = process.env['OWNER_COOKIE_SECRET']
    ?? [
      process.env['DATABASE_URL'],
      process.env['MQTT_PASSWORD'],
      process.env['CLOUDFLARE_TUNNEL_TOKEN'],
      'meshcore-owner-cookie-fallback',
    ].filter(Boolean).join('|');
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
    return { nodeIds, exp: parsed.exp };
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
  return proto.includes('https');
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

function networkFilters(network?: string): NetworkFilters {
  return {
    params: network ? [network] : [],
    packets: network ? 'AND network = $1' : '',
    nodes: network ? 'AND network = $1' : '',
    nodesAlias: (alias: string) => (network ? `AND ${alias}.network = $1` : ''),
  };
}

// GET /api/nodes — all known nodes
router.get('/nodes', async (_req, res) => {
  try {
    const nodes = await getNodes();
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
    const hours = Math.min(Number(req.query['hours'] ?? 24), 672); // max 28 days
    const history = await getNodeHistory(req.params['id']!, hours);
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
    const packets = await getRecentPackets(limit);
    res.json(packets);
  } catch (err) {
    console.error('[api] GET /packets/recent', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/stats
router.get('/stats', async (req, res) => {
  try {
    const network = req.query['network'] as string | undefined;
    const filters = networkFilters(network);
    const [mqttCount, packetCount, staleCount, mapNodeCount, totalNodeCount, longestHopCount] = await Promise.all([
      query(`SELECT COUNT(DISTINCT rx_node_id) AS count FROM packets WHERE time > NOW() - INTERVAL '10 minutes' AND rx_node_id IS NOT NULL ${filters.packets}`, filters.params),
      query(`SELECT COUNT(DISTINCT packet_hash) AS count FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}`, filters.params),
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
    const network = req.query['network'] as string | undefined;
    const filters = networkFilters(network);
    const result = await query(
      `SELECT nc.node_id, nc.geom, nc.antenna_height_m, nc.radius_m, nc.calculated_at
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
    const network = (req.query['network'] as string | undefined) ?? 'teesside';
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
    const usernameMap = parseOwnerMqttUsernameMap();
    if (usernameMap.size < 1) {
      res.status(503).json({ error: 'Owner login is not configured on the server' });
      return;
    }
    const mappedNodeIds = usernameMap.get(mqttUsername);
    if (!mappedNodeIds || mappedNodeIds.length < 1) {
      res.status(403).json({ error: 'Invalid MQTT credentials' });
      return;
    }

    const authOk = await verifyMqttCredentials(mqttUsername, mqttPassword);
    if (!authOk) {
      res.status(403).json({ error: 'Invalid MQTT credentials' });
      return;
    }

    const dashboard = await buildOwnerDashboard(mappedNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      res.status(403).json({ error: 'No active node found for this MQTT username yet' });
      return;
    }

    const token = encryptOwnerSession({
      nodeIds: mappedNodeIds,
      exp: Date.now() + OWNER_SESSION_TTL_MS,
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
    const sessionNodeIds = await requireOwnerSession(req, res);
    if (!sessionNodeIds) return;

    const dashboard = await buildOwnerDashboard(sessionNodeIds);
    if (dashboard.totals.ownedNodes < 1) {
      res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
      res.status(401).json({ error: 'No active node found for this MQTT username yet' });
      return;
    }

    res.json({ ok: true, dashboard });
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

    const [ownerNodeResult, incomingResult, packetResult] = await Promise.all([
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
         WHERE node_id = $1
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
    ]);

    const ownerNode = ownerNodeResult.rows[0];
    if (!ownerNode) {
      res.status(404).json({ error: 'Owner node not found' });
      return;
    }

    res.json({
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
      recentPackets: packetResult.rows.map((row) => ({
        ...row,
        time: new Date(row.time).toISOString(),
      })),
    });
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

    await query(
      `INSERT INTO frontend_error_events (kind, message, stack, page, user_agent)
       VALUES ($1, $2, $3, $4, $5)`,
      [
        String(body.kind ?? 'error').slice(0, 40),
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
    const network = req.query['network'] as string | undefined;
    const filters = networkFilters(network);

    const PAYLOAD_LABELS: Record<number, string> = {
      0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
      4: 'Advert', 5: 'GroupText', 6: 'GroupData',
      7: 'AnonReq', 8: 'Path', 9: 'Trace',
    };

    const [
      phResult, pdResult, rhResult, rdResult,
      ptResult, rpResult, hdResult, pcResult, sumResult,
    ] = await Promise.all([
      // packets per hour — last 24h (deduplicated by hash)
      query(`
        SELECT time_bucket('1 hour', time) AS hour, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}
        GROUP BY hour ORDER BY hour
      `, filters.params),
      // packets per day — last 7d (deduplicated by hash)
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(DISTINCT packet_hash) AS count
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
      // packet types — last 24h (deduplicated by hash)
      query(`
        SELECT packet_type, COUNT(DISTINCT packet_hash) AS count
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
      // hop count distribution — last 7d (deduplicated by hash)
      query(`
        SELECT hop_count AS hops, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days'
          AND hop_count IS NOT NULL
          ${filters.packets}
        GROUP BY hop_count ORDER BY hop_count
      `, filters.params),
      // top repeated first-2-hex prefix collisions across known nodes
      query(`
        WITH prefix_counts AS (
          SELECT SUBSTRING(LOWER(n.node_id) FROM 1 FOR 2) AS prefix, COUNT(*)::int AS node_count
          FROM nodes n
          WHERE n.node_id ~ '^[0-9A-Fa-f]{64}$'
            AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
            AND (n.role IS NULL OR n.role != 4)
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
          (SELECT COUNT(DISTINCT packet_hash) FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}) AS total_24h,
          (SELECT COUNT(DISTINCT packet_hash) FROM packets WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}) AS total_7d,
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
