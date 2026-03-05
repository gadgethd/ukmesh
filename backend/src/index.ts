import 'node:process';
import http from 'node:http';
import express from 'express';
import cors from 'cors';
import { rateLimit } from 'express-rate-limit';
import { initDb, query } from './db/index.js';
import { startMqttClient, onPacket, onNodeSeen, onNodeUpsert } from './mqtt/client.js';
import { initWebSocketServer, broadcastPacket, broadcastNodeUpdate, broadcastNodeUpsert } from './ws/server.js';
import apiRoutes from './api/routes.js';
import { queueViewshedJob, queueLinkJob } from './queue/publisher.js';

const ALLOWED_ORIGINS = (process.env['ALLOWED_ORIGINS'] ?? '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const PORT = Number(process.env['PORT'] ?? 3000);

async function main() {
  // 1. Initialise DB schema + retention policy
  await initDb();

  // Queue viewshed jobs for any node with a position but no coverage yet
  // (catches nodes that existed before the worker was added)
  {
    const uncovered = await query<{ node_id: string; lat: number; lon: number }>(
      `SELECT n.node_id, n.lat, n.lon FROM nodes n
       LEFT JOIN node_coverage nc ON n.node_id = nc.node_id
       WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL AND nc.node_id IS NULL
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         AND (n.role IS NULL OR n.role = 2)`
    );
    if (uncovered.rows.length > 0) {
      console.log(`[app] queuing ${uncovered.rows.length} node(s) for viewshed`);
      // Jobs are pushed here but the Redis pub client isn't ready yet —
      // defer until after initWebSocketServer wires up the Redis client.
      process.nextTick(() => {
        for (const row of uncovered.rows) {
          queueViewshedJob(row.node_id, row.lat, row.lon);
        }
      });
    }
  }

  // 2. Wire up MQTT → WS broadcast
  onPacket((packet) => {
    broadcastPacket(packet);
    if (packet.path?.length && packet.rxNodeId) {
      queueLinkJob(packet.rxNodeId, packet.srcNodeId, packet.path, packet.hopCount);
    }
  });
  onNodeSeen((nodeId) => broadcastNodeUpdate(nodeId));
  onNodeUpsert((node) => {
    broadcastNodeUpsert(node);
    // Queue a viewshed job only for visible repeaters (role=2 or unknown)
    const isHidden      = typeof node.name === 'string' && node.name.includes('🚫');
    const isNonRepeater = typeof node.role === 'number' && node.role !== 2;
    if (!isHidden && !isNonRepeater && typeof node.lat === 'number' && typeof node.lon === 'number') {
      queueViewshedJob(node.node_id as string, node.lat, node.lon);
    }
  });

  // 3. Express app
  const app = express();

  // Trust Cloudflare's forwarded IP so rate limiting works correctly
  app.set('trust proxy', 1);

  // CORS — allow only our own domains for browser cross-origin requests
  app.use(cors({
    origin: (origin, cb) => {
      // No origin = same-origin request (or curl/server-to-server) — allow
      if (!origin || ALLOWED_ORIGINS.includes(origin)) cb(null, true);
      else cb(new Error('CORS: origin not allowed'));
    },
  }));

  app.use(express.json());

  // Rate limit: 120 requests / IP / minute on all API endpoints
  app.use('/api', rateLimit({
    windowMs: 60_000,
    max: 120,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please slow down' },
  }));

  // API routes
  app.use('/api', apiRoutes);

  // Health check
  app.get('/healthz', (_req, res) => res.json({ status: 'ok', ts: Date.now() }));

  // 4. HTTP server + WebSocket
  const httpServer = http.createServer(app);
  initWebSocketServer(httpServer);

  // 5. Start MQTT client
  startMqttClient();

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`[app] listening on http://0.0.0.0:${PORT}`);
  });
}

main().catch((err) => {
  console.error('[app] fatal startup error:', err);
  process.exit(1);
});
