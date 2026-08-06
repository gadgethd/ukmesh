import 'node:process';
import http from 'node:http';
import express from 'express';
import compression from 'compression';
import cors from 'cors';
import { closeDb, initDb, pool, query } from './db/index.js';
import { refreshNodeIdentityAliases } from './db/nodeIdentity.js';
import { closeOwnerAuthDb, getOwnerAclReadiness, initOwnerAuthDb } from './db/ownerAuth.js';
import { getMqttRuntimeStatus, startMqttClient, stopMqttClient, onPacket, onNodeSeen, onNodeUpsert } from './mqtt/client.js';
import {
  startMqttConnectionMonitor,
  stopMqttConnectionMonitor,
} from './mqtt/connectionMonitor.js';
import { initWebSocketServer, closeWebSocketServer, broadcastPacket, broadcastNodeUpdate, broadcastNodeUpsert } from './ws/server.js';
import apiRoutes from './api/routes.js';
import { initSpamMessageAnalyzer, stopSpamMessageAnalyzer } from './spam/analyzer.js';
import {
  closeQueuePublisher,
  queueLinkJob,
  stopQueueAdmission,
} from './queue/publisher.js';
import { createBackendSiteRoutes } from './backend-site/routes.js';
import { isTrustedProxyPeer } from './http/trustedProxy.js';
import { startOwnerAuthorizationReconciler, stopOwnerAuthorizationReconciler } from './owner/ownerAclReconciler.js';
import { getAnalysisWorkloadStates } from './analysis/runState.js';
import { applySecurityHeaders } from './security/operatorAuth.js';
import { closeMetricsServer, startMetricsServer } from './metricsServer.js';
import { resolvePool } from './path-beta/resolvePool.js';
import {
  LifecycleCoordinator,
  LifecycleDeadlineError,
} from './lifecycle/coordinator.js';
import {
  apiErrorMiddleware,
  requestContextMiddleware,
} from './api/errors.js';
import { getWorkerHealthOverview } from './health/status.js';
import {
  gracefulShutdownTotal,
  observeHttpRequest,
} from './metrics.js';
import { closeOperatorOperations } from './operations/operatorOperations.js';
import { createGlobalApiLimiter } from './api/bootstrap/limiters.js';
import { createHopReachCompatibilityRoutes } from './api/hopreachCompatibility.js';

const ALLOWED_ORIGINS = (process.env['ALLOWED_ORIGINS'] ?? '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const PORT = Number(process.env['PORT'] ?? 3000);
const METRICS_PORT = Number(process.env['METRICS_PORT'] ?? 9091);
const HSTS_HEADER = 'max-age=31536000; includeSubDomains; preload';
const MQTT_INGEST_ENABLED = !['0', 'false', 'no'].includes(
  String(process.env['MQTT_INGEST_ENABLED'] ?? 'true').trim().toLowerCase(),
);
const WS_ENABLED = process.env['WS_ENABLED'] !== '0';
const SHUTDOWN_DEADLINE_MS = Math.min(
  30_000,
  Math.max(1_000, Number(process.env['SHUTDOWN_DEADLINE_MS'] ?? 30_000) || 30_000),
);
const NODE_IDENTITY_REFRESH_INTERVAL_MS = 30 * 60_000;
const lifecycle = new LifecycleCoordinator(SHUTDOWN_DEADLINE_MS);
let shutdownExitCode = 0;
let forceCloseHttpConnections = () => {};
let nodeIdentityRefreshTimer: NodeJS.Timeout | null = null;

lifecycle.register({
  name: 'queue-admission',
  stage: 10,
  close: stopQueueAdmission,
});
lifecycle.register({
  name: 'mqtt-connection-monitor',
  stage: 10,
  close: stopMqttConnectionMonitor,
});
lifecycle.register({
  name: 'spam-analyzer',
  stage: 10,
  close: stopSpamMessageAnalyzer,
});
lifecycle.register({
  name: 'owner-authorization-reconciler',
  stage: 10,
  close: stopOwnerAuthorizationReconciler,
});
lifecycle.register({
  name: 'mqtt-ingest',
  stage: 10,
  close: () => stopMqttClient(Math.max(1_000, SHUTDOWN_DEADLINE_MS - 5_000)),
});
lifecycle.register({
  name: 'path-resolve-workers',
  stage: 30,
  close: () => resolvePool.close(),
});
lifecycle.register({
  name: 'queue-publisher',
  stage: 30,
  close: closeQueuePublisher,
});
lifecycle.register({
  name: 'operator-operations',
  stage: 30,
  close: closeOperatorOperations,
});
lifecycle.register({
  name: 'metrics-server',
  stage: 30,
  close: closeMetricsServer,
});
lifecycle.register({
  name: 'owner-auth-database',
  stage: 40,
  close: closeOwnerAuthDb,
});
lifecycle.register({
  name: 'application-database',
  stage: 40,
  close: closeDb,
});
lifecycle.register({
  name: 'node-identity-refresh',
  stage: 10,
  close: () => {
    if (nodeIdentityRefreshTimer) clearInterval(nodeIdentityRefreshTimer);
    nodeIdentityRefreshTimer = null;
  },
});

async function shutdown(reason: string, exitCode: number): Promise<void> {
  shutdownExitCode = Math.max(shutdownExitCode, exitCode);
  if (!lifecycle.isDraining) console.log(`[app] draining after ${reason}`);
  try {
    await lifecycle.drain(reason);
    gracefulShutdownTotal.inc({ outcome: 'success' });
    console.log('[app] graceful shutdown complete');
    process.exit(shutdownExitCode);
  } catch (error) {
    gracefulShutdownTotal.inc({
      outcome: error instanceof LifecycleDeadlineError ? 'deadline' : 'failure',
    });
    forceCloseHttpConnections();
    const diagnostics = lifecycle.snapshot();
    if (error instanceof LifecycleDeadlineError) {
      console.error('[app] graceful shutdown timed out', diagnostics);
    } else {
      console.error('[app] graceful shutdown failed:', error, diagnostics);
    }
    process.exit(1);
  }
}

process.on('SIGTERM', () => { void shutdown('SIGTERM', 0); });
process.on('SIGINT', () => { void shutdown('SIGINT', 0); });
process.on('unhandledRejection', (reason) => {
  console.error('[app] unhandled rejection:', reason);
  void shutdown('unhandledRejection', 1);
});
process.on('uncaughtException', (error) => {
  console.error('[app] uncaught exception:', error);
  void shutdown('uncaughtException', 1);
});

async function main() {
  // 1. Initialise DB schema + retention policy
  await initDb();
  const identityRefresh = await refreshNodeIdentityAliases(pool);
  console.log('[node-identity] refreshed', identityRefresh);
  nodeIdentityRefreshTimer = setInterval(() => {
    void refreshNodeIdentityAliases(pool)
      .then((result) => console.log('[node-identity] refreshed', result))
      .catch((error: unknown) => {
        console.error('[node-identity] refresh failed:', error instanceof Error ? error.message : error);
      });
  }, NODE_IDENTITY_REFRESH_INTERVAL_MS);
  nodeIdentityRefreshTimer.unref();
  await initOwnerAuthDb();
  await startOwnerAuthorizationReconciler();

  // 2. Start the audit-only MQTT connection monitor.
  if (MQTT_INGEST_ENABLED) {
    startMqttConnectionMonitor();
  } else {
    console.warn('[mqtt] ingest disabled by MQTT_INGEST_ENABLED');
  }

  // 3. Wire up MQTT → WS broadcast
  onPacket((packet) => {
    broadcastPacket(packet);
    if (packet.path?.length && packet.rxNodeId) {
      void queueLinkJob(
        packet.packetHash,
        packet.rxNodeId,
        packet.srcNodeId,
        packet.path,
        packet.hopCount,
        packet.pathHashSizeBytes,
      ).then((admission) => {
        if (admission && ['full', 'oversized', 'worker_unavailable'].includes(admission.status)) {
          console.warn('[link-queue] live observation not admitted', admission.status, packet.packetHash);
        }
      }).catch((error: Error) => console.error('[link-queue] live admission failed', error.message));
    }
  });
  onNodeSeen((nodeId, meta) => broadcastNodeUpdate(nodeId, meta));
  onNodeUpsert((node) => {
    broadcastNodeUpsert(node);
  });

  // 3. Express app
  const app = express();
  app.use(requestContextMiddleware);
  app.use(observeHttpRequest);

  // Only the fixed Compose Nginx peers may supply client identity. Broad
  // private-range trust lets a direct container or host caller spoof quotas.
  app.set('trust proxy', (ip: string) => isTrustedProxyPeer(ip));

  // Gzip compression for bounded JSON and static responses.
  app.use(compression());

  // CORS — allow only our own domains for browser cross-origin requests
  app.use(cors({
    origin: (origin, cb) => {
      // No origin = same-origin request (or curl/server-to-server) — allow
      if (!origin || ALLOWED_ORIGINS.includes(origin)) cb(null, true);
      else cb(new Error('CORS: origin not allowed'));
    },
  }));

  // Security headers
  app.use((req, res, next) => {
    res.setHeader('Strict-Transport-Security', HSTS_HEADER);
    applySecurityHeaders(req, res);
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader(
      'Content-Security-Policy',
      "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data: blob: https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com https://openstreetmap.org https://*.openstreetmap.org; connect-src 'self' wss: https:; font-src 'self' data:",
    );
    next();
  });

  // Internal CoreScope-shaped compatibility API for the HopReach calculator.
  // Mounted outside /api so the public Nginx proxy never exposes it. Its one
  // bulk calibration request needs a larger, still-bounded JSON body.
  app.use(
    '/hopreach',
    express.json({ limit: '512kb' }),
    createHopReachCompatibilityRoutes(query),
  );

  app.use(express.json({ limit: '50kb' }));

  // Local-only backend/operator site.
  app.use('/', createBackendSiteRoutes({
    query,
    getHealthOverview: getWorkerHealthOverview,
  }));

  // Defaults to 120 requests / IP / minute. Isolated load environments may
  // raise the bounded configuration without changing production policy.
  app.use('/api', createGlobalApiLimiter());

  // API routes
  app.use('/api', apiRoutes);

  // Health check
  app.get('/healthz', (_req, res) => res.json({ status: 'ok', ts: Date.now() }));
  app.get('/readyz', async (_req, res) => {
    const checks = {
      database: false,
      mqtt: MQTT_INGEST_ENABLED ? getMqttRuntimeStatus() : { state: 'disabled', changedAt: new Date().toISOString() },
      ownerAuthorization: {
        mode: String(process.env['OWNER_AUTHORIZATION_MODE'] ?? 'shadow'),
        aclMode: String(process.env['OWNER_ACL_MODE'] ?? 'shadow'),
        desiredGeneration: null as string | null,
        renderedGeneration: null as string | null,
        appliedGeneration: null as string | null,
        lastVerifiedAt: null as string | null,
        lastError: null as string | null,
      },
      analysis: [] as Awaited<ReturnType<typeof getAnalysisWorkloadStates>>,
    };
    try {
      await query('SELECT 1');
      checks.database = true;
      Object.assign(checks.ownerAuthorization, await getOwnerAclReadiness());
      checks.analysis = await getAnalysisWorkloadStates();
    } catch (err) {
      console.error('[readyz] database check failed:', (err as Error).message);
    }
    const ownerAclReady = checks.ownerAuthorization.aclMode !== 'apply'
      || (
        checks.ownerAuthorization.desiredGeneration !== null
        && checks.ownerAuthorization.desiredGeneration === checks.ownerAuthorization.appliedGeneration
        && checks.ownerAuthorization.lastError === null
      );
    const ready = !lifecycle.isDraining
      && checks.database
      && ownerAclReady
      && (!MQTT_INGEST_ENABLED || checks.mqtt.state === 'connected');
    res.status(ready ? 200 : 503).json({ status: ready ? 'ready' : 'degraded', checks, ts: Date.now() });
  });
  app.use(apiErrorMiddleware);

  // 4. HTTP server + WebSocket
  const httpServer = http.createServer(app);
  let httpClosePromise: Promise<void> | null = null;
  lifecycle.register({
    name: 'http-admission',
    stage: 10,
    close: () => {
      httpServer.closeIdleConnections();
      if (!httpClosePromise) {
        httpClosePromise = new Promise<void>((resolve, reject) => {
          httpServer.close((error) => {
            if ((error as NodeJS.ErrnoException | undefined)?.code === 'ERR_SERVER_NOT_RUNNING') {
              resolve();
            } else if (error) {
              reject(error);
            } else {
              resolve();
            }
          });
        });
      }
    },
  });
  lifecycle.register({
    name: 'http-connections',
    stage: 20,
    close: async () => {
      await httpClosePromise;
    },
  });
  forceCloseHttpConnections = () => httpServer.closeAllConnections();
  const webSocketServer = WS_ENABLED
    ? initWebSocketServer(httpServer)
    : null;
  if (webSocketServer) {
    lifecycle.register({
      name: 'websocket-server',
      stage: 20,
      close: () => closeWebSocketServer(webSocketServer),
    });
  }
  if (!WS_ENABLED) {
    console.warn('[ws] disabled by WS_ENABLED');
  }

  // 5. Serve the API before long-running ingest warmup. Spam detector caches
  // may scan historical packets, and coupling that work to the listener made
  // otherwise healthy deploys fail their HTTP checks for minutes.
  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`[app] listening on http://0.0.0.0:${PORT}`);
  });
  startMetricsServer(METRICS_PORT);

  if (MQTT_INGEST_ENABLED) {
    void startMqttClient().catch((err: unknown) => {
      console.error('[mqtt] failed to start client:', err instanceof Error ? err.message : err);
    });
  }

  // 6. Periodic message-spam analyzer (decoded channel messages -> incidents).
  //    Fire-and-forget so the heavy first pass never delays the listen above.
  void initSpamMessageAnalyzer().catch((err: unknown) => {
    console.error('[spam-msg] failed to start analyzer:', err instanceof Error ? err.message : err);
  });
}

main().catch((err) => {
  console.error('[app] fatal startup error:', err);
  void shutdown('startup failure', 1);
});
