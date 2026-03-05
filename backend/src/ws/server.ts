import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { Redis } from 'ioredis';
import type { WSMessage, LivePacket } from '../types/index.js';
import { getNodes, getLastNPackets, getViableLinks } from '../db/index.js';

const REDIS_CHANNEL = 'meshcore:live';

let pub: Redis;
let sub: Redis;
const VIABLE_LINK_CACHE_TTL_MS = 30_000;
const viableLinksCache = new Map<string, { ts: number; data: Awaited<ReturnType<typeof getViableLinks>> }>();

async function getCachedViableLinks(network?: string) {
  const key = network ?? 'all';
  const cached = viableLinksCache.get(key);
  if (cached && (Date.now() - cached.ts) < VIABLE_LINK_CACHE_TTL_MS) return cached.data;
  const data = await getViableLinks(network);
  viableLinksCache.set(key, { ts: Date.now(), data });
  return data;
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

  wss.on('connection', async (ws: WebSocket, req: IncomingMessage) => {
    console.log('[ws] client connected, total:', wss.clients.size);

    // Derive network filter from query param (?network=teesside)
    const reqUrl  = new URL(req.url ?? '/', 'http://localhost');
    const network = reqUrl.searchParams.get('network') ?? undefined;

    // Send initial state: known nodes + last 5 minutes of packets
    try {
      const [nodes, packets, viableLinks] = await Promise.all([
        getNodes(network), getLastNPackets(7, network), getCachedViableLinks(network),
      ]);
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
      console.log('[ws] client disconnected, total:', wss.clients.size);
    });

    ws.on('error', (err) => {
      console.error('[ws] client error', err.message);
    });
  });

  // Fan-out Redis messages to all connected WS clients
  sub.on('message', (_channel: string, messageStr: string) => {
    if (wss.clients.size === 0) return;
    for (const client of wss.clients) {
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

export function broadcastNodeUpdate(nodeId: string): void {
  const msg: WSMessage = { type: 'node_update', data: { nodeId, ts: Date.now() }, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}

export function broadcastNodeUpsert(node: Record<string, unknown>): void {
  const msg: WSMessage = { type: 'node_upsert', data: node, ts: Date.now() };
  void pub.publish(REDIS_CHANNEL, JSON.stringify(msg));
}
