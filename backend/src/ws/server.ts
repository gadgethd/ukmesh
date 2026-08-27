import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { Redis } from 'ioredis';
import type { WSMessage, LivePacket } from '../types/index.js';
import { getNodes, getRecentPackets, getRecentMessages, getViableLinks } from '../db/index.js';
import {
  PublicAllScopeForbiddenError,
  resolvePublicNetworkScope,
} from '../http/requestScope.js';
import { networkMatchesScope } from '../networks.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';
import {
  BoundedAsyncGate,
  BoundedTaskQueueFullError,
  websocketAdmissionDecision,
} from './limits.js';
import { isPrivateNode } from '../api/utils/privateNode.js';
import { PublicWsPrivacyIndex } from './privacy.js';
import { trustedClientIp } from '../http/trustedProxy.js';
import {
  websocketAdmissionsTotal,
  websocketClients,
} from '../metrics.js';
import { selectInitialViableLinks } from './initialStateLinks.js';

const REDIS_CHANNEL = 'meshcore:live';
const LOG_WS_PACKETS = process.env['LOG_WS_PACKETS'] === '1';
const WS_INITIAL_STATE_ENABLED = process.env['WS_INITIAL_STATE_ENABLED'] === '1';

function boundedEnvInteger(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

const WS_MAX_PAYLOAD_BYTES = boundedEnvInteger('WS_MAX_PAYLOAD_BYTES', 64 * 1024, 1_024, 1_048_576);
const WS_MAX_QUEUE_BYTES = boundedEnvInteger('WS_MAX_QUEUE_BYTES', 8 * 1_048_576, 16 * 1024, 32 * 1_048_576);
const WS_MAX_BUFFERED_BYTES = boundedEnvInteger('WS_MAX_BUFFERED_BYTES', 32 * 1_048_576, WS_MAX_QUEUE_BYTES, 128 * 1_048_576);
const WS_MAX_CONNECTIONS = boundedEnvInteger('WS_MAX_CONNECTIONS', 500, 1, 10_000);
const WS_MAX_CONNECTIONS_PER_IP = boundedEnvInteger('WS_MAX_CONNECTIONS_PER_IP', 20, 1, 1_000);
const WS_HANDSHAKES_PER_IP_PER_MINUTE = boundedEnvInteger('WS_HANDSHAKES_PER_IP_PER_MINUTE', 30, 1, 10_000);
const WS_MAX_PENDING_HANDSHAKES = boundedEnvInteger('WS_MAX_PENDING_HANDSHAKES', 32, 1, 1_000);
const WS_INITIAL_STATE_CONCURRENCY = boundedEnvInteger('WS_INITIAL_STATE_CONCURRENCY', 8, 1, 128);
const WS_INITIAL_STATE_QUEUE_MAX = boundedEnvInteger('WS_INITIAL_STATE_QUEUE_MAX', 64, 0, 1_000);
const WS_INITIAL_STATE_MAX_BYTES = boundedEnvInteger('WS_INITIAL_STATE_MAX_BYTES', 4 * 1_048_576, 16 * 1024, 32 * 1_048_576);
const WS_INITIAL_STATE_MAX_VIABLE_LINKS = boundedEnvInteger('WS_INITIAL_STATE_MAX_VIABLE_LINKS', 20_000, 1, 100_000);
const WS_INITIAL_STATE_DB_TIMEOUT_MS = boundedEnvInteger('WS_INITIAL_STATE_DB_TIMEOUT_MS', 8_000, 1_000, 60_000);
const WS_HEARTBEAT_INTERVAL_MS = boundedEnvInteger('WS_HEARTBEAT_INTERVAL_MS', 30_000, 5_000, 120_000);

type InitialStateRow = Record<string, unknown>;

function newestTimestamp(...values: unknown[]): unknown {
  let newest: unknown;
  let newestMs = Number.NEGATIVE_INFINITY;
  for (const value of values) {
    if (value == null) continue;
    const parsed = Date.parse(String(value));
    if (Number.isFinite(parsed) && parsed > newestMs) {
      newest = value;
      newestMs = parsed;
    }
  }
  return newest;
}

function slimInitialNode(raw: InitialStateRow): InitialStateRow {
  const lastSeen = newestTimestamp(
    raw['last_seen'],
    raw['last_rx_at'],
    raw['last_status_at'],
    raw['last_path_evidence_at'],
  );
  return {
    node_id: raw['node_id'],
    name: raw['name'],
    lat: raw['lat'],
    lon: raw['lon'],
    iata: raw['observer_iata'] ?? raw['iata'],
    observer_iata: raw['observer_iata'],
    role: raw['role'],
    last_seen: lastSeen,
    last_rx_at: raw['last_rx_at'],
    last_status_at: raw['last_status_at'],
    is_online: raw['is_online'],
    hardware_model: raw['hardware_model'],
    advert_count: raw['advert_count'],
    elevation_m: raw['elevation_m'],
  };
}

function slimInitialPacket(raw: InitialStateRow): InitialStateRow {
  return {
    time: raw['time'],
    packet_hash: raw['packet_hash'],
    rx_node_id: raw['rx_node_id'],
    src_node_id: raw['src_node_id'],
    observer_node_ids: raw['observer_node_ids'],
    iata: raw['iata'],
    observer_iatas: raw['observer_iatas'],
    topic: raw['topic'],
    packet_type: raw['packet_type'],
    hop_count: raw['hop_count'],
    rssi: raw['rssi'],
    snr: raw['snr'],
    summary: raw['summary'],
    advert_count: raw['advert_count'],
    path_hashes: raw['path_hashes'],
    path_hash_size_bytes: raw['path_hash_size_bytes'],
    rx_count: raw['rx_count'],
    tx_count: raw['tx_count'],
  };
}

function estimatedJsonBytes(value: unknown, stopAfter: number): number {
  let bytes = 0;
  const stack: unknown[] = [value];
  while (stack.length > 0 && bytes <= stopAfter) {
    const current = stack.pop();
    if (current == null) {
      bytes += 4;
    } else if (typeof current === 'string') {
      bytes += Buffer.byteLength(current, 'utf8') + 2;
    } else if (typeof current === 'number') {
      bytes += String(current).length + 4;
    } else if (typeof current === 'boolean') {
      bytes += current ? 4 : 5;
    } else if (Array.isArray(current)) {
      bytes += current.length + 2;
      stack.push(...current);
    } else if (typeof current === 'object') {
      const entries = Object.entries(current);
      bytes += entries.length + 2;
      for (const [key, entry] of entries) {
        bytes += Buffer.byteLength(key) + 3;
        stack.push(entry);
      }
    }
  }
  return bytes;
}

function initialStateEstimateExceedsLimit(entry: InitialStateEntry): boolean {
  return estimatedJsonBytes({
    type: 'initial_state',
    data: {
      nodes: entry.nodes,
      packets: entry.packets,
      messages: entry.messages,
      viable_links: entry.viableLinks,
    },
    ts: Date.now(),
  }, WS_INITIAL_STATE_MAX_BYTES) > WS_INITIAL_STATE_MAX_BYTES;
}

let pub: Redis;
let sub: Redis;
// Viable links change slowly (based on historical packet accumulation).
// Keep the scoped link snapshot off the connect path for five minutes per
// network/observer combination.
const VIABLE_LINK_CACHE_TTL_MS = 5 * 60_000;
const viableLinksCache = new BoundedTtlMap<string, {
  ts: number;
  data: Awaited<ReturnType<typeof getViableLinks>>;
}>({
  name: 'ws_viable_links',
  maxEntries: 50,
  maxWeight: 16 * 1024 * 1024,
  ttlMs: VIABLE_LINK_CACHE_TTL_MS,
});

/**
 * Initial-state cache — all connecting clients share one cached snapshot per
 * network/observer key, refreshed every 30 seconds in the background.
 * Eliminates the DB pool spike caused by N clients each firing 3 queries on connect.
 */
const INITIAL_STATE_TTL_MS = 60_000; // 60 s — live WS updates keep clients current
type InitialStateEntry = {
  ts: number;
  nodes: InitialStateRow[];
  packets: InitialStateRow[];
  messages: InitialStateRow[];
  viableLinks: Awaited<ReturnType<typeof getViableLinks>>;
};
const initialStateCache = new BoundedTtlMap<string, InitialStateEntry>({
  name: 'ws_initial_state',
  maxEntries: 128,
  maxWeight: 48 * 1024 * 1024,
  ttlMs: INITIAL_STATE_TTL_MS,
});
const initialStateInflight = new Map<string, Promise<InitialStateEntry>>();
const publicPrivacy = new PublicWsPrivacyIndex();
let privacyRefreshInFlight: Promise<void> | null = null;

function refreshPrivacyIndex(): Promise<void> {
  if (privacyRefreshInFlight) return privacyRefreshInFlight;
  const tracked = Promise.all([getNodes('ukmesh'), getNodes('test')])
    .then(([productionNodes, testNodes]) => {
      publicPrivacy.replace([...productionNodes, ...testNodes]);
    })
    .catch((error: unknown) => {
      console.error('[ws] privacy index refresh failed:', (error as Error).message);
    })
    .finally(() => {
      if (privacyRefreshInFlight === tracked) privacyRefreshInFlight = null;
    });
  privacyRefreshInFlight = tracked;
  return tracked;
}

async function fetchInitialState(network: string | undefined, observer: string | undefined): Promise<InitialStateEntry> {
  const key = `${network ?? ''}:${observer ?? ''}`;
  const cached = initialStateCache.get(key);
  if (cached && (Date.now() - cached.ts) < INITIAL_STATE_TTL_MS) return cached;

  // If a fetch is already in flight for this key, share it — don't pile on the DB.
  const existing = initialStateInflight.get(key);
  if (existing) return existing;
  if (initialStateInflight.size >= 128) {
    throw new Error('WS_INITIAL_STATE_OVERLOADED');
  }

  const promise = (async () => {
    try {
      // The public synthetic check allows 10 seconds. Abort every database read
      // before that deadline so a pathological cold query cannot retain an
      // initial-state gate slot (and a pool connection) for minutes.
      const signal = AbortSignal.timeout(WS_INITIAL_STATE_DB_TIMEOUT_MS);
      // getRecentPackets: 5-minute window, all types (fast, CTE aggregation ~16 ms).
      // getRecentMessages: last 200 GRP (type=5) from Postgres so the feed can
      //   seed a proper message cache on first load instead of relying on live traffic.
      const [rawNodes, packets, messages, rawViableLinks] = await Promise.all([
        getNodes(network, observer, 'slim', signal),
        getRecentPackets(7, network, observer, 'full', signal),
        getRecentMessages(200, network, observer, signal),
        getCachedViableLinks(network, observer, signal),
      ]);
      const nodes = rawNodes
        .filter((node) => !isPrivateNode(node.name))
        .map((node) => slimInitialNode(node as InitialStateRow));
      const slimPackets = packets.map((packet) => slimInitialPacket(packet as InitialStateRow));
      const slimMessages = messages.map((message) => slimInitialPacket(message as InitialStateRow));
      const viableLinks = selectInitialViableLinks(
        rawViableLinks.filter((link) => (
          !publicPrivacy.hasNode(link.node_a_id)
          && !publicPrivacy.hasNode(link.node_b_id)
        )),
        WS_INITIAL_STATE_MAX_VIABLE_LINKS,
      );
      const entry: InitialStateEntry = {
        ts: Date.now(),
        nodes,
        packets: slimPackets,
        messages: slimMessages,
        viableLinks,
      };
      initialStateCache.set(key, entry);
      return entry;
    } finally {
      initialStateInflight.delete(key);
    }
  })();

  initialStateInflight.set(key, promise);
  return promise;
}

type ClientScope = {
  network?: string;
  observer?: string;
  nodeIds: Set<string>;
};

function normalizeObserver(value: string | null): string | undefined {
  const trimmed = String(value ?? '').trim().toLowerCase();
  return trimmed && /^[0-9a-f]{64}$/.test(trimmed) ? trimmed : undefined;
}

function cacheKey(network?: string, observer?: string): string {
  return `${network ?? 'all'}|${observer ?? 'all'}`;
}

async function getCachedViableLinks(
  network?: string,
  observer?: string,
  signal?: AbortSignal,
) {
  const key = cacheKey(network, observer);
  const cached = viableLinksCache.get(key);
  if (cached && (Date.now() - cached.ts) < VIABLE_LINK_CACHE_TTL_MS) return cached.data;
  const data = await getViableLinks(network, observer, signal);
  viableLinksCache.set(key, { ts: Date.now(), data });
  return data;
}

function packetMatchesScope(packet: Partial<LivePacket>, scope: ClientScope): boolean {
  if (scope.network && packet.network && !networkMatchesScope(packet.network, scope.network)) return false;
  if (!scope.network && !scope.observer && packet.network === 'test') return false;
  if (scope.observer) {
    // rxNodeId is a hex public key — always lowercase; no allocation needed
    return (packet.rxNodeId ?? '') === scope.observer;
  }
  return true;
}

function nodeMatchesScope(nodeId: string | undefined, scope: ClientScope): boolean {
  if (!nodeId) return false;
  // IDs are pre-normalised to lowercase at broadcast time; no allocation needed
  if (scope.observer && nodeId === scope.observer) return true;
  return scope.nodeIds.has(nodeId);
}

function shouldSendMessage(msg: WSMessage, scope: ClientScope): boolean {
  if (msg.type === 'packet') {
    return packetMatchesScope(msg.data as Partial<LivePacket>, scope);
  }

  if (msg.type === 'node_update') {
    const data = msg.data as { nodeId?: string; network?: string; observerId?: string };
    if (scope.network && data.network && !networkMatchesScope(data.network, scope.network)) return false;
    if (!scope.network && !scope.observer && data.network === 'test') return false;
    if (!scope.network && !scope.observer) return true;
    if (scope.observer && data.observerId && data.observerId !== scope.observer && !nodeMatchesScope(data.nodeId, scope)) {
      return false;
    }
    return nodeMatchesScope(data.nodeId, scope);
  }

  if (msg.type === 'node_upsert') {
    const data = msg.data as { node_id?: string; network?: string; observer_id?: string };
    if (scope.network && data.network && !networkMatchesScope(data.network, scope.network)) return false;
    if (!scope.network && !scope.observer && data.network === 'test') return false;
    if (!scope.network && !scope.observer) return true;
    if (scope.observer) {
      if (data.observer_id && data.observer_id === scope.observer) return true;
      if (data.observer_id && data.observer_id !== scope.observer && !nodeMatchesScope(data.node_id, scope)) {
        return false;
      }
    }
    return nodeMatchesScope(data.node_id, scope);
  }

  if (msg.type === 'link_update') {
    const data = msg.data as { node_a_id?: string; node_b_id?: string };
    return nodeMatchesScope(data.node_a_id, scope) || nodeMatchesScope(data.node_b_id, scope);
  }

  return true;
}

function trackScopedNodes(msg: WSMessage, scope: ClientScope): void {
  if (msg.type === 'packet') {
    // rxNodeId/srcNodeId are hex public keys — always lowercase
    const data = msg.data as Partial<LivePacket>;
    if (data.rxNodeId)  scope.nodeIds.add(data.rxNodeId);
    if (data.srcNodeId) scope.nodeIds.add(data.srcNodeId);
    return;
  }

  if (msg.type === 'node_upsert') {
    // node_id is pre-normalised to lowercase at broadcast time
    const data = msg.data as { node_id?: string };
    if (data.node_id) scope.nodeIds.add(data.node_id);
  }
}

export function initWebSocketServer(httpServer: Server): WebSocketServer {
  void refreshPrivacyIndex();
  const privacyRefreshTimer = setInterval(() => void refreshPrivacyIndex(), 5 * 60_000);
  privacyRefreshTimer.unref();
  // Two separate clients: one for pub, one for sub
  // Do NOT use lazyConnect — let ioredis manage the connect lifecycle
  pub = new Redis(getRedisUrl(), getRedisConnectionOptions());
  sub = new Redis(getRedisUrl(), getRedisConnectionOptions());

  pub.on('error', (e: Error) => console.error('[redis/pub] error', e.message));
  sub.on('error', (e: Error) => console.error('[redis/sub] error', e.message));

  // Subscribe only after the connection is ready to avoid
  // the INFO ready-check conflicting with subscriber mode
  sub.on('ready', () => {
    sub.subscribe(REDIS_CHANNEL, (err) => {
      if (err) console.error('[redis/sub] subscribe error', err.message);
      else console.log('[redis/sub] subscribed to', REDIS_CHANNEL);
    });
  });

  const ALLOWED_ORIGINS = (process.env['ALLOWED_ORIGINS'] ?? '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  const initialStateGate = new BoundedAsyncGate(
    WS_INITIAL_STATE_CONCURRENCY,
    WS_INITIAL_STATE_QUEUE_MAX,
  );
  let pendingHandshakes = 0;
  const connectionsByPeer = new Map<string, number>();
  const handshakeWindows = new Map<string, { startedAt: number; count: number }>();
  const pendingHandshakeTimers = new WeakMap<IncomingMessage['socket'], NodeJS.Timeout>();
  const missedPongs = new WeakMap<WebSocket, number>();

  const releasePendingHandshake = (req: IncomingMessage): void => {
    const timer = pendingHandshakeTimers.get(req.socket);
    if (!timer) return;
    clearTimeout(timer);
    pendingHandshakeTimers.delete(req.socket);
    pendingHandshakes = Math.max(0, pendingHandshakes - 1);
  };

  // Pre-warm the initial state cache for common networks at startup so the
  // first connecting client doesn't pay the cold DB cost.
  const WARMUP_NETWORKS = (process.env['WARMUP_NETWORKS'] ?? '')
    .split(',').map(s => s.trim()).filter(Boolean);

  const warmInitialState = () => {
    for (const net of WARMUP_NETWORKS) {
      fetchInitialState(net, undefined).catch(() => { /* best-effort */ });
    }
  };

  if (WARMUP_NETWORKS.length > 0) {
    setTimeout(warmInitialState, 10_000).unref();
    setInterval(warmInitialState, INITIAL_STATE_TTL_MS).unref();
  }

  const wss = new WebSocketServer({
    server: httpServer,
    path: '/ws',
    maxPayload: WS_MAX_PAYLOAD_BYTES,
    verifyClient: (info, done) => {
      const origin = info.origin;
      if (origin && !ALLOWED_ORIGINS.includes(origin)) {
        websocketAdmissionsTotal.inc({ outcome: 'origin_denied' });
        done(false, 403, 'Forbidden');
        return;
      }
      try {
        const reqUrl = new URL(info.req.url ?? '/', 'http://localhost');
        resolvePublicNetworkScope(reqUrl.searchParams.get('network'), info.req.headers);
      } catch (error) {
        if (!(error instanceof PublicAllScopeForbiddenError)) throw error;
        websocketAdmissionsTotal.inc({ outcome: 'scope_denied' });
        done(false, 400, 'The all-network scope is not available');
        return;
      }
      const peer = trustedClientIp(info.req);
      const now = Date.now();
      if (handshakeWindows.size >= 4_096 && !handshakeWindows.has(peer)) {
        const oldest = handshakeWindows.keys().next().value as string | undefined;
        if (oldest) handshakeWindows.delete(oldest);
      }
      let peerWindow = handshakeWindows.get(peer);
      if (!peerWindow || now - peerWindow.startedAt >= 60_000) {
        peerWindow = { startedAt: now, count: 0 };
        handshakeWindows.set(peer, peerWindow);
      }
      peerWindow.count += 1;
      if (
        peerWindow.count > WS_HANDSHAKES_PER_IP_PER_MINUTE
        || (connectionsByPeer.get(peer) ?? 0) >= WS_MAX_CONNECTIONS_PER_IP
      ) {
        websocketAdmissionsTotal.inc({ outcome: 'rate_limited' });
        done(false, 429, 'Too Many Requests', { 'Retry-After': '5' });
        return;
      }
      const admission = websocketAdmissionDecision(
        { activeConnections: wss.clients.size, pendingHandshakes },
        {
          maxConnections: WS_MAX_CONNECTIONS,
          maxPendingHandshakes: WS_MAX_PENDING_HANDSHAKES,
        },
      );
      if (!admission.allowed) {
        websocketAdmissionsTotal.inc({ outcome: 'capacity_denied' });
        done(false, admission.statusCode, admission.reason, { 'Retry-After': '5' });
        return;
      }
      pendingHandshakes += 1;
      const timer = setTimeout(() => releasePendingHandshake(info.req), 10_000);
      timer.unref();
      pendingHandshakeTimers.set(info.req.socket, timer);
      websocketAdmissionsTotal.inc({ outcome: 'accepted' });
      done(true);
    },
  });

  const clientScopes = new Map<WebSocket, ClientScope>();
  type QueuedClientMessages = {
    messages: string[];
    byteLength: number;
  };
  const messageQueue = new Map<WebSocket, QueuedClientMessages>();
  let flushTimeout: ReturnType<typeof setTimeout> | null = null;

  const disconnectSlowClient = (client: WebSocket, reason: string) => {
    messageQueue.delete(client);
    console.warn(`[ws] disconnecting slow client: ${reason}`);
    client.terminate();
  };

  wss.on('connection', async (ws: WebSocket, req: IncomingMessage) => {
    releasePendingHandshake(req);
    const peer = trustedClientIp(req);
    connectionsByPeer.set(peer, (connectionsByPeer.get(peer) ?? 0) + 1);
    ws.once('close', () => {
      const remaining = Math.max(0, (connectionsByPeer.get(peer) ?? 1) - 1);
      if (remaining === 0) connectionsByPeer.delete(peer);
      else connectionsByPeer.set(peer, remaining);
    });
    missedPongs.set(ws, 0);
    ws.on('pong', () => missedPongs.set(ws, 0));
    console.log('[ws] client connected, total:', wss.clients.size);
    websocketClients.set(wss.clients.size);

    // Derive scope from query params (?network=teesside&observer=<pubkey>)
    const reqUrl  = new URL(req.url ?? '/', 'http://localhost');
    const network = resolvePublicNetworkScope(reqUrl.searchParams.get('network'), req.headers);
    const observer = normalizeObserver(reqUrl.searchParams.get('observer'));
    const scope: ClientScope = {
      network,
      observer,
      nodeIds: new Set<string>(),
    };
    clientScopes.set(ws, scope);

    // Synthetic handshake probes explicitly opt out of the expensive initial
    // state. The WebSocket control-frame ping/pong still proves the full HTTP
    // upgrade and bidirectional socket path without touching PostgreSQL.
    const skipInitialState = reqUrl.searchParams.get('initial_state') === '0';
    if (skipInitialState) {
      // No application payload is required for a control-frame probe.
    } else if (WS_INITIAL_STATE_ENABLED) {
      // Send initial state: served from cache so concurrent connects don't exhaust the DB pool.
      try {
        const entry = await initialStateGate.run(
          () => fetchInitialState(network, observer),
        );
        if (initialStateEstimateExceedsLimit(entry)) {
          console.warn('[ws] initial state estimate exceeds configured limit', {
            network,
            observer,
            nodes: entry.nodes.length,
            packets: entry.packets.length,
            messages: entry.messages.length,
            viableLinks: entry.viableLinks.length,
          });
          ws.close(1013, 'initial state is temporarily unavailable');
          return;
        }
        const { nodes, packets, messages, viableLinks } = entry;
        for (const node of nodes) {
          const nodeId = String((node as { node_id?: string }).node_id ?? '').toLowerCase();
          if (nodeId) scope.nodeIds.add(nodeId);
        }
        for (const packet of packets) {
          const rxNodeId = String((packet as { rx_node_id?: string }).rx_node_id ?? '').toLowerCase();
          const srcNodeId = String((packet as { src_node_id?: string }).src_node_id ?? '').toLowerCase();
          if (rxNodeId) scope.nodeIds.add(rxNodeId);
          if (srcNodeId) scope.nodeIds.add(srcNodeId);
        }
        const initMsg: WSMessage = {
          type: 'initial_state',
          data: { nodes, packets, messages, viable_links: viableLinks },
          ts: Date.now(),
        };
        const serialized = JSON.stringify(initMsg);
        if (Buffer.byteLength(serialized) > WS_INITIAL_STATE_MAX_BYTES) {
          ws.close(1013, 'initial state is temporarily unavailable');
          return;
        }
        if (ws.readyState === WebSocket.OPEN) ws.send(serialized);
      } catch (err) {
        console.error('[ws] initial state error', (err as Error).message);
        ws.close(
          1013,
          err instanceof BoundedTaskQueueFullError
            ? 'initial state is busy'
            : 'initial state is temporarily unavailable',
        );
      }
    } else {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'initial_state',
          data: { nodes: [], packets: [], messages: [], viable_links: [] },
          ts: Date.now(),
        } satisfies WSMessage));
      }
    }

    ws.on('close', () => {
      clientScopes.delete(ws);
      messageQueue.delete(ws);
      websocketClients.set(wss.clients.size);
      console.log('[ws] client disconnected, total:', wss.clients.size);
    });

    ws.on('error', (err) => {
      console.error('[ws] client error', err.message);
    });
  });

  const heartbeatTimer = setInterval(() => {
    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      const missed = (missedPongs.get(client) ?? 0) + 1;
      if (missed > 2) {
        client.terminate();
        continue;
      }
      missedPongs.set(client, missed);
      client.ping();
    }
  }, WS_HEARTBEAT_INTERVAL_MS);
  heartbeatTimer.unref();
  wss.on('close', () => {
    clearInterval(heartbeatTimer);
    clearInterval(privacyRefreshTimer);
  });

  const flushMessageQueue = () => {
    if (messageQueue.size === 0) {
      flushTimeout = null;
      return;
    }

    for (const [client, queued] of messageQueue) {
      if (client.readyState !== WebSocket.OPEN || queued.messages.length === 0) continue;
      if (client.bufferedAmount > WS_MAX_BUFFERED_BYTES) {
        disconnectSlowClient(client, `socket buffer exceeded ${WS_MAX_BUFFERED_BYTES} bytes`);
        continue;
      }

      // Send all queued messages - join with newlines for efficiency
      client.send(queued.messages.join('\n'));
    }

    messageQueue.clear();
    flushTimeout = null;
  };

  const scheduleFlush = () => {
    if (flushTimeout !== null) return;
    flushTimeout = setTimeout(flushMessageQueue, 16);
  };

  // Fan-out Redis messages to all connected WS clients - now batched
  sub.on('message', (_channel: string, messageStr: string) => {
    if (wss.clients.size === 0) return;
    let parsed: WSMessage | null = null;
    try {
      parsed = JSON.parse(messageStr) as WSMessage;
    } catch {
      return;
    }
    parsed = publicPrivacy.filterMessage(parsed);
    if (!parsed) return;
    messageStr = JSON.stringify(parsed);

    if (LOG_WS_PACKETS && parsed?.type === 'packet') {
      console.log('[ws-sub] received packet:', (parsed.data as LivePacket)?.packetHash);
    }

    const messageByteLength = Buffer.byteLength(messageStr);
    if (messageByteLength > WS_MAX_QUEUE_BYTES) {
      console.warn(`[ws] skipping oversized live message (${messageByteLength} bytes)`);
      return;
    }

    const isLiveFeedPacket = parsed?.type === 'packet'
      && Number((parsed.data as { packetType?: number } | null)?.packetType) === 5;

    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      const scope = clientScopes.get(client);
      if (parsed && scope && !shouldSendMessage(parsed, scope)) continue;
      if (parsed && scope) trackScopedNodes(parsed, scope);

      // GRP chat packets power the live feed — send immediately so the UI
      // never waits on the 16ms batch timer. Other events stay batched.
      if (isLiveFeedPacket) {
        if (client.bufferedAmount > WS_MAX_BUFFERED_BYTES) {
          disconnectSlowClient(client, `socket buffer exceeded ${WS_MAX_BUFFERED_BYTES} bytes`);
          continue;
        }
        // Flush any already-queued events first so ordering stays intact.
        const existing = messageQueue.get(client);
        if (existing && existing.messages.length > 0) {
          client.send(existing.messages.join('\n') + '\n' + messageStr);
          messageQueue.delete(client);
        } else {
          client.send(messageStr);
        }
        continue;
      }

      // Queue messages briefly for batched fan-out, with per-client bounds so
      // a stalled browser cannot retain an unbounded live-event backlog.
      const existing = messageQueue.get(client);
      if (
        client.bufferedAmount > WS_MAX_BUFFERED_BYTES
        || (existing?.byteLength ?? 0) + messageByteLength > WS_MAX_QUEUE_BYTES
      ) {
        disconnectSlowClient(client, `live queue exceeded ${WS_MAX_QUEUE_BYTES} bytes`);
        continue;
      }
      if (existing) {
        existing.messages.push(messageStr);
        existing.byteLength += messageByteLength;
      } else {
        messageQueue.set(client, { messages: [messageStr], byteLength: messageByteLength });
      }
    }

    if (!isLiveFeedPacket) scheduleFlush();
  });

  return wss;
}

export async function closeWebSocketServer(wss: WebSocketServer): Promise<void> {
  for (const client of wss.clients) client.close(1001, 'server restart');
  const deadline = Date.now() + 5_000;
  while (wss.clients.size > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  for (const client of wss.clients) client.terminate();
  await new Promise<void>((resolve) => {
    wss.close(() => resolve());
  });
  await Promise.all([
    pub?.quit().catch(() => undefined),
    sub?.quit().catch(() => undefined),
  ]);
  websocketClients.set(0);
}

export function broadcastPacket(packet: LivePacket): void {
  if (publicPrivacy.packetHasPrivateParticipant(packet)) return;
  const msg: WSMessage = { type: 'packet', data: packet, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}

export function broadcastNodeUpdate(nodeId: string, meta?: { network?: string; observerId?: string }): void {
  if (!publicPrivacy.isReady || publicPrivacy.hasNode(nodeId)) return;
  // Normalise IDs to lowercase once here so shouldSendMessage() never needs to allocate
  const msg: WSMessage = {
    type: 'node_update',
    data: {
      nodeId:     nodeId.toLowerCase(),
      network:    meta?.network,
      observerId: meta?.observerId?.toLowerCase(),
      ts:         Date.now(),
    },
    ts: Date.now(),
  };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}

export function broadcastNodeUpsert(node: Record<string, unknown>): void {
  const candidate = { type: 'node_upsert', data: node, ts: Date.now() } as WSMessage;
  if (!publicPrivacy.filterMessage(candidate)) return;
  // Normalise IDs to lowercase once here so shouldSendMessage() never needs to allocate
  const normalised: Record<string, unknown> = {
    ...node,
    node_id:     typeof node['node_id']     === 'string' ? node['node_id'].toLowerCase()     : node['node_id'],
    observer_id: typeof node['observer_id'] === 'string' ? node['observer_id'].toLowerCase() : node['observer_id'],
    public_key:  typeof node['public_key']  === 'string' ? node['public_key'].toLowerCase()  : node['public_key'],
  };
  const msg: WSMessage = { type: 'node_upsert', data: normalised, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}
