import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { Redis } from 'ioredis';
import type { WSMessage, LivePacket } from '../types/index.js';
import { getNodes, getRecentPackets, getRecentMessages, getViableLinks } from '../db/index.js';
import { resolveRequestNetwork } from '../http/requestScope.js';

const REDIS_CHANNEL = 'meshcore:live';

let pub: Redis;
let sub: Redis;
// Viable links change slowly (based on historical packet accumulation).
// 5-minute TTL means the expensive correlated-subquery runs at most once per
// 5 minutes per network/observer combo instead of once per 30 seconds.
const VIABLE_LINK_CACHE_TTL_MS = 5 * 60_000;
const VIABLE_LINK_CACHE_MAX = 50;
const viableLinksCache = new Map<string, { ts: number; data: Awaited<ReturnType<typeof getViableLinks>> }>();

// Periodically evict stale cache entries so they don't persist indefinitely
// when a network/observer combo stops being requested.
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of viableLinksCache) {
    if (now - entry.ts > VIABLE_LINK_CACHE_TTL_MS) viableLinksCache.delete(key);
  }
}, VIABLE_LINK_CACHE_TTL_MS);

/**
 * Initial-state cache — all connecting clients share one cached snapshot per
 * network/observer key, refreshed every 30 seconds in the background.
 * Eliminates the DB pool spike caused by N clients each firing 3 queries on connect.
 */
const INITIAL_STATE_TTL_MS = 60_000; // 60 s — live WS updates keep clients current
type InitialStateEntry = {
  ts: number;
  nodes: Awaited<ReturnType<typeof getNodes>>;
  packets: Awaited<ReturnType<typeof getRecentPackets>>;
  messages: Awaited<ReturnType<typeof getRecentMessages>>;
  viableLinks: Awaited<ReturnType<typeof getViableLinks>>;
};
const initialStateCache = new Map<string, InitialStateEntry>();
const initialStateInflight = new Map<string, Promise<InitialStateEntry>>();

async function fetchInitialState(network: string | undefined, observer: string | undefined): Promise<InitialStateEntry> {
  const key = `${network ?? ''}:${observer ?? ''}`;
  const cached = initialStateCache.get(key);
  if (cached && (Date.now() - cached.ts) < INITIAL_STATE_TTL_MS) return cached;

  // If a fetch is already in flight for this key, share it — don't pile on the DB.
  const existing = initialStateInflight.get(key);
  if (existing) return existing;

  const promise = (async () => {
    try {
      // getRecentPackets: 5-minute window, all types (fast, CTE aggregation ~16 ms).
      // getRecentMessages: last 200 GRP (type=5) from Postgres so the feed can
      //   seed a proper message cache on first load instead of relying on live traffic.
      const [nodes, packets, messages, viableLinks] = await Promise.all([
        getNodes(network, observer),
        getRecentPackets(7, network, observer),
        getRecentMessages(200, network, observer),
        getCachedViableLinks(network, observer),
      ]);
      const entry: InitialStateEntry = { ts: Date.now(), nodes, packets, messages, viableLinks };
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

async function getCachedViableLinks(network?: string, observer?: string) {
  const key = cacheKey(network, observer);
  const cached = viableLinksCache.get(key);
  if (cached && (Date.now() - cached.ts) < VIABLE_LINK_CACHE_TTL_MS) return cached.data;
  const data = await getViableLinks(network, observer);
  if (viableLinksCache.size >= VIABLE_LINK_CACHE_MAX) {
    // Evict the oldest entry
    const oldest = Array.from(viableLinksCache.entries()).sort((a, b) => a[1].ts - b[1].ts)[0];
    if (oldest) viableLinksCache.delete(oldest[0]);
  }
  viableLinksCache.set(key, { ts: Date.now(), data });
  return data;
}

function packetMatchesScope(packet: Partial<LivePacket>, scope: ClientScope): boolean {
  if (scope.network && packet.network && packet.network !== scope.network) return false;
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
    if (scope.network && data.network && data.network !== scope.network) return false;
    if (!scope.network && !scope.observer && data.network === 'test') return false;
    if (!scope.network && !scope.observer) return true;
    if (scope.observer && data.observerId && data.observerId !== scope.observer && !nodeMatchesScope(data.nodeId, scope)) {
      return false;
    }
    return nodeMatchesScope(data.nodeId, scope);
  }

  if (msg.type === 'node_upsert') {
    const data = msg.data as { node_id?: string; network?: string; observer_id?: string };
    if (scope.network && data.network && data.network !== scope.network) return false;
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

  if (msg.type === 'coverage_update') {
    const data = msg.data as { node_id?: string };
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
  const redisUrl = process.env['REDIS_URL'] ?? 'redis://redis:6379';

  // Two separate clients: one for pub, one for sub
  // Do NOT use lazyConnect — let ioredis manage the connect lifecycle
  pub = new Redis(redisUrl);
  sub = new Redis(redisUrl);

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

  // Pre-warm the initial state cache for common networks at startup so the
  // first connecting client doesn't pay the cold DB cost.
  const WARMUP_NETWORKS = (process.env['WARMUP_NETWORKS'] ?? 'teesside,ukmesh')
    .split(',').map(s => s.trim()).filter(Boolean);
  process.nextTick(() => {
    for (const net of WARMUP_NETWORKS) {
      fetchInitialState(net, undefined).catch(() => { /* best-effort */ });
    }
  });

  const wss = new WebSocketServer({
    server: httpServer,
    path: '/ws',
    verifyClient: ({ origin }: { origin: string }) => {
      // No origin header = non-browser client (allow); otherwise must be whitelisted
      return !origin || ALLOWED_ORIGINS.includes(origin);
    },
  });

  const clientScopes = new Map<WebSocket, ClientScope>();

  wss.on('connection', async (ws: WebSocket, req: IncomingMessage) => {
    console.log('[ws] client connected, total:', wss.clients.size);

    // Derive scope from query params (?network=teesside&observer=<pubkey>)
    const reqUrl  = new URL(req.url ?? '/', 'http://localhost');
    const requestedNetwork = resolveRequestNetwork(reqUrl.searchParams.get('network'), req.headers);
    const network = requestedNetwork === 'all' ? undefined : requestedNetwork;
    const observer = normalizeObserver(reqUrl.searchParams.get('observer'));
    const scope: ClientScope = {
      network,
      observer,
      nodeIds: new Set<string>(),
    };
    clientScopes.set(ws, scope);

    // Send initial state: served from cache so concurrent connects don't exhaust the DB pool.
    try {
      const { nodes, packets, messages, viableLinks } = await fetchInitialState(network, observer);
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
      const viablePairs = viableLinks.map((l) => [l.node_a_id, l.node_b_id] as [string, string]);
      const initMsg: WSMessage = {
        type: 'initial_state',
        data: { nodes, packets, messages, viable_pairs: viablePairs, viable_links: viableLinks },
        ts: Date.now(),
      };
      ws.send(JSON.stringify(initMsg));
    } catch (err) {
      console.error('[ws] initial state error', (err as Error).message);
    }

    ws.on('close', () => {
      clientScopes.delete(ws);
      console.log('[ws] client disconnected, total:', wss.clients.size);
    });

    ws.on('error', (err) => {
      console.error('[ws] client error', err.message);
    });
  });

  // Message queue for batching - flushes every 50ms instead of per-message
  const messageQueue: Map<WebSocket, string[]> = new Map();
  let flushTimeout: ReturnType<typeof setTimeout> | null = null;

  const flushMessageQueue = () => {
    if (messageQueue.size === 0) {
      flushTimeout = null;
      return;
    }

    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      const messages = messageQueue.get(client);
      if (!messages || messages.length === 0) continue;

      // Send all queued messages - join with newlines for efficiency
      const combined = messages.join('\n');
      client.send(combined);
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

    // Log packet messages
    if (parsed?.type === 'packet') {
      console.log('[ws-sub] received packet:', (parsed.data as LivePacket)?.packetHash);
    }

    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      const scope = clientScopes.get(client);
      if (parsed && scope && !shouldSendMessage(parsed, scope)) continue;
      if (parsed && scope) trackScopedNodes(parsed, scope);

      // Queue message instead of sending immediately
      const existing = messageQueue.get(client);
      if (existing) {
        existing.push(messageStr);
      } else {
        messageQueue.set(client, [messageStr]);
      }
    }
    
    scheduleFlush();
  });

  return wss;
}

export function broadcastPacket(packet: LivePacket): void {
  const msg: WSMessage = { type: 'packet', data: packet, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}

export function broadcastNodeUpdate(nodeId: string, meta?: { network?: string; observerId?: string }): void {
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
