import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { Redis } from 'ioredis';
import type { WSMessage, LivePacket } from '../types/index.js';
import { getNodes, getLastNPackets, getViableLinks } from '../db/index.js';
import { resolveRequestNetwork } from '../http/requestScope.js';

const REDIS_CHANNEL = 'meshcore:live';

let pub: Redis;
let sub: Redis;
const VIABLE_LINK_CACHE_TTL_MS = 30_000;
const VIABLE_LINK_CACHE_MAX = 50;
const viableLinksCache = new Map<string, { ts: number; data: Awaited<ReturnType<typeof getViableLinks>> }>();

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
    return String(packet.rxNodeId ?? '').toLowerCase() === scope.observer;
  }
  return true;
}

function nodeMatchesScope(nodeId: string | undefined, scope: ClientScope): boolean {
  if (!nodeId) return false;
  const normalized = nodeId.toLowerCase();
  if (scope.observer && normalized === scope.observer) return true;
  return scope.nodeIds.has(normalized);
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
    if (scope.observer && data.observerId && data.observerId.toLowerCase() !== scope.observer && !nodeMatchesScope(data.nodeId, scope)) {
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
      if (data.observer_id && data.observer_id.toLowerCase() === scope.observer) return true;
      if (data.observer_id && data.observer_id.toLowerCase() !== scope.observer && !nodeMatchesScope(data.node_id, scope)) {
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
    const data = msg.data as Partial<LivePacket>;
    if (data.rxNodeId) scope.nodeIds.add(data.rxNodeId.toLowerCase());
    if (data.srcNodeId) scope.nodeIds.add(data.srcNodeId.toLowerCase());
    return;
  }

  if (msg.type === 'node_upsert') {
    const data = msg.data as { node_id?: string };
    if (data.node_id) scope.nodeIds.add(data.node_id.toLowerCase());
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

    // Send initial state: known nodes + last 5 minutes of packets
    try {
      const [nodes, packets, viableLinks] = await Promise.all([
        getNodes(network, observer), getLastNPackets(7, network, observer), getCachedViableLinks(network, observer),
      ]);
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
        data: { nodes, packets, viable_pairs: viablePairs, viable_links: viableLinks },
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

  // Fan-out Redis messages to all connected WS clients
  sub.on('message', (_channel: string, messageStr: string) => {
    if (wss.clients.size === 0) return;
    let parsed: WSMessage | null = null;
    try {
      parsed = JSON.parse(messageStr) as WSMessage;
    } catch {
      return;
    }
    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      const scope = clientScopes.get(client);
      if (parsed && scope && !shouldSendMessage(parsed, scope)) continue;
      if (parsed && scope) trackScopedNodes(parsed, scope);
      if (client.readyState === WebSocket.OPEN) {
        client.send(messageStr);
      }
    }
  });

  return wss;
}

export function broadcastPacket(packet: LivePacket): void {
  const msg: WSMessage = { type: 'packet', data: packet, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}

export function broadcastNodeUpdate(nodeId: string, meta?: { network?: string; observerId?: string }): void {
  const msg: WSMessage = { type: 'node_update', data: { nodeId, network: meta?.network, observerId: meta?.observerId, ts: Date.now() }, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}

export function broadcastNodeUpsert(node: Record<string, unknown>): void {
  const msg: WSMessage = { type: 'node_upsert', data: node, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}
