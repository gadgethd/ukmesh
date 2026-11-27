import http from 'node:http';
import express from 'express';
import compression from 'compression';
import cors from 'cors';
import { initDb, analyticsPool, pool, query } from '../db/runtime.js';
import { refreshNodeIdentityAliases } from '../db/nodeIdentity.js';
import { refreshNodeMapSuppressions } from '../nodes/mapSuppressions.js';
import {
  getAppliedOwnerAclArtifact,
  getOwnerAuthorizationInventory,
  getOwnerAuthorizationSnapshot,
} from '../owner/ownerAuth.js';
import { initWebSocketServer, closeWebSocketServer } from '../ws/server.js';
import { subscribeLive } from '../ws/liveBus.js';
import { mqttIngestEnabled, readRemoteIngestHealth } from '../ingest/health.js';
import { getMqttConnectionMonitorStatus, startMqttConnectionMonitor, stopMqttConnectionMonitor } from '../mqtt/connectionMonitor.js';
import { createPathResolutionRuntime } from '../path-beta/liveResolution.js';
import { closeRamStore } from '../ram/liveStore.js';
import { createPublicRamSource } from '../ram/publicSource.js';
import { createBeaconPublicAuthority } from '../beacon/publicAuthority.js';
import { createApiRuntime } from './routes.js';
import { initSpamMessageAnalyzer, stopSpamMessageAnalyzer } from '../spam/analyzer.js';
import { createBackendSiteRoutes } from '../backend-site/routes.js';
import { isTrustedProxyPeer } from '../http/trustedProxy.js';
import { getConvergedOwnerAclReadiness, ownerAuthorizationAvailable, startOwnerAuthorizationReconciler, stopOwnerAuthorizationReconciler } from '../owner/ownerAclReconciler.js';
import { createOwnerLastHopPrewarm } from '../owner/ownerLastHopPrewarm.js';
import { applySecurityHeaders } from '../security/operatorAuth.js';
import type { LifecycleCoordinator } from '../lifecycle/coordinator.js';
import {
  apiErrorMiddleware,
  requestContextMiddleware,
} from './errors.js';
import { createHealthRuntime } from '../health/runtime.js';
import { observeHttpRequest } from '../metrics/http.js';
import { closeOperatorOperations } from '../operations/operatorOperations.js';
import { createGlobalApiLimiter } from './bootstrap/limiters.js';
import { createHopReachCompatibilityRoutes } from './hopreachCompatibility.js';
import { createPublicRfProjectionRoutes } from '../rf/publicProjection.js';
import { createRfVisibilityReader } from '../rf/visibility.js';
import { readAclFile } from '../mqtt/aclManager.js';
import { parseOwnerGrantConfig } from '../owner/ownerGrantConfig.js';
import { loadOwnerInventoryBaseline } from '../owner/ownerInventoryBaseline.js';
import { createOwnerAuthorizationReadiness, publicReadinessPayload } from '../owner/ownerReadiness.js';

const ALLOWED_ORIGINS = (process.env['ALLOWED_ORIGINS'] ?? '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const PORT = Number(process.env['PORT'] ?? 3000);
const HSTS_HEADER = 'max-age=31536000; includeSubDomains; preload';
const WS_ENABLED = process.env['WS_ENABLED'] !== '0';
const NODE_IDENTITY_REFRESH_INTERVAL_MS = 30 * 60_000;
const OWNER_LAST_HOP_PREWARM_INTERVAL_MS = Number(process.env['OWNER_LAST_HOP_PREWARM_INTERVAL_MS'] ?? 15 * 60_000);
const OWNER_LAST_HOP_PREWARM_NODE_DELAY_MS = Number(process.env['OWNER_LAST_HOP_PREWARM_NODE_DELAY_MS'] ?? 150);
const OWNER_LAST_HOP_PREWARM_CONCURRENCY = Number(process.env['OWNER_LAST_HOP_PREWARM_CONCURRENCY'] ?? 1);

export async function startApiService(options: {
  lifecycle: LifecycleCoordinator;
  shutdownTimeLeft: () => number;
  combined: boolean;
  setForceClose: (forceClose: () => void) => void;
}) {
  const { lifecycle, shutdownTimeLeft, combined, setForceClose } = options;
  let nodeIdentityRefreshTimer: NodeJS.Timeout | null = null;
  let ownerLastHopPrewarmStop: (() => void) | null = null;

  lifecycle.register({
    name: 'owner-last-hop-prewarm',
    stage: 10,
    close: () => {
      ownerLastHopPrewarmStop?.();
      ownerLastHopPrewarmStop = null;
    },
  });
  lifecycle.register({
    name: 'operator-operations',
    stage: 30,
    close: closeOperatorOperations,
  });
  lifecycle.register({
    name: 'node-identity-refresh',
    stage: 10,
    close: () => {
      if (nodeIdentityRefreshTimer) clearInterval(nodeIdentityRefreshTimer);
      nodeIdentityRefreshTimer = null;
    },
  });

  const pathResolution = createPathResolutionRuntime();
  pathResolution.start();
  lifecycle.register({ name: 'path-resolution', stage: 37, close: pathResolution.close });
  if (!combined) lifecycle.register({ name: 'pending-packet-reader', stage: 30, close: closeRamStore });
  const pathOptions = !combined ? {
    onPathObservation: pathResolution.acceptObservation,
    onPathGap: pathResolution.clearResolvedPaths,
  } : {};
  const healthRuntime = createHealthRuntime();
  healthRuntime.startTelemetry();
  lifecycle.register({ name: 'health-runtime', stage: 30, close: healthRuntime.close });
  await initDb();
  const identityRefresh = await refreshNodeIdentityAliases(analyticsPool, { maxAgeMs: NODE_IDENTITY_REFRESH_INTERVAL_MS });
  console.log('[node-identity] refreshed', identityRefresh);
  // Suppression is a display optimisation: a transient failure here must not
  // stop the API from booting. The 30-minute timer retries.
  await refreshNodeMapSuppressions(pool).catch((error: unknown) => {
    console.error(
      '[node-map] initial suppression refresh failed:',
      error instanceof Error ? error.message : error,
    );
  });
  nodeIdentityRefreshTimer = setInterval(() => {
    void refreshNodeIdentityAliases(analyticsPool, { maxAgeMs: NODE_IDENTITY_REFRESH_INTERVAL_MS })
      .then(async (result) => {
        console.log('[node-identity] refreshed', result);
        await refreshNodeMapSuppressions(pool);
      })
      .catch((error: unknown) => {
        console.error('[node-identity/node-map] refresh failed:', error instanceof Error ? error.message : error);
      });
  }, NODE_IDENTITY_REFRESH_INTERVAL_MS);
  nodeIdentityRefreshTimer.unref();
  // Visibility-dependent chart work must pin its generation only after the
  // initial identity reconciliation has settled the alias mapping.
  const readIngestHealth = combined
    ? (await import('../ingest/service.js')).startIngestService({ lifecycle, shutdownTimeLeft, observePath: pathResolution.acceptObservation }).readHealth
    : readRemoteIngestHealth(process.env['MQTT_INGEST_HEALTH_URL'] ?? 'http://mqtt-ingest:3000/readyz');
  const apiRuntime = createApiRuntime(healthRuntime, pathResolution, readIngestHealth);
  lifecycle.register({ name: 'api-runtime', stage: 30, close: apiRuntime.close });
  apiRuntime.startBackgroundWork();
  startOwnerAuthorizationReconciler();
  if (mqttIngestEnabled()) {
    startMqttConnectionMonitor();
    lifecycle.register({ name: 'mqtt-connection-monitor', stage: 10, close: stopMqttConnectionMonitor });
  }
  lifecycle.register({ name: 'owner-authorization-reconciler', stage: 10, close: stopOwnerAuthorizationReconciler });
  lifecycle.register({ name: 'spam-analyzer', stage: 10, close: stopSpamMessageAnalyzer });
  // Keep the owner dashboard RX-strength cache warm for every active MQTT owner
  // so the panel (and its 7d/30d toggle) never pays for a cold scan on open.
  const ownerService = apiRuntime.ownerService;
  ownerLastHopPrewarmStop = createOwnerLastHopPrewarm({
    listOwnerNodes: () => ownerAuthorizationAvailable() ? getOwnerAuthorizationSnapshot() : Promise.resolve([]),
    refreshLastHop: (ownedNodeIds, nodeId, windowDays, minAgeMs) => (
      ownerService.refreshOwnerLastHopStrength(ownedNodeIds, nodeId, windowDays, minAgeMs)
    ),
    intervalMs: OWNER_LAST_HOP_PREWARM_INTERVAL_MS,
    nodeDelayMs: OWNER_LAST_HOP_PREWARM_NODE_DELAY_MS,
    concurrency: OWNER_LAST_HOP_PREWARM_CONCURRENCY,
  });

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
      "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data: blob: https://tiles.openfreemap.org https://openstreetmap.org https://*.openstreetmap.org; connect-src 'self' wss: https:; font-src 'self' data:",
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

  const publicRfOutput = process.env['RF_PUBLIC_PROJECTION_OUTPUT_DIRECTORY'];
  if (publicRfOutput) {
    app.use('/internal/rf-coverage-public', createPublicRfProjectionRoutes({
      outputDirectory: publicRfOutput,
      readVisibility: createRfVisibilityReader(query),
    }));
  }

  app.use(express.json({ limit: '50kb' }));

  const readOwnerAuthorizationReadiness = createOwnerAuthorizationReadiness({
    config: () => ({
      mode: String(process.env['OWNER_AUTHORIZATION_MODE'] ?? 'enforce'),
      aclMode: String(process.env['OWNER_ACL_MODE'] ?? 'apply'),
      baselinePath: String(process.env['OWNER_AUTH_INVENTORY_BASELINE_PATH'] ?? '').trim(),
      configuredGrants: parseOwnerGrantConfig(String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? '')),
    }),
    readState: getConvergedOwnerAclReadiness,
    readInventory: getOwnerAuthorizationInventory,
    readAcl: readAclFile,
    readBaseline: loadOwnerInventoryBaseline,
    loadAppliedArtifact: getAppliedOwnerAclArtifact,
  });

  // Local-only backend/operator site.
  app.use('/', createBackendSiteRoutes({
    query,
    getHealthOverview: healthRuntime.getWorkerHealthOverview,
    getOwnerAuthorizationReadiness: readOwnerAuthorizationReadiness,
  }));

  // Defaults to 120 requests / IP / minute. Isolated load environments may
  // raise the bounded configuration without changing production policy.
  // Telemetry uses its route budget; dashboard stats have a reserved budget.
  app.use('/api', createGlobalApiLimiter());

  app.use('/api/owner', (req, res, next) => {
    if (ownerAuthorizationAvailable() || req.path === '/csrf' || req.path === '/logout') {
      next();
      return;
    }
    res.setHeader('Cache-Control', 'no-store');
    res.status(503).json({ error: 'Owner authorization is temporarily unavailable', retryable: true });
  });

  // API routes
  app.use('/api', apiRuntime.router);
  app.use('/internal/ingest/public', createPublicRamSource());
  app.use('/internal/beacon', createBeaconPublicAuthority());

  app.get('/healthz', (_req, res) => res.json({ status: 'ok', ts: Date.now() }));
  async function databaseReady(): Promise<boolean> {
    try { await query('SELECT 1', [], AbortSignal.timeout(1_000)); return true; }
    catch { return false; }
  }
  app.get('/api-readyz', async (_req, res) => {
    const ready = !lifecycle.isDraining && await databaseReady();
    res.status(ready ? 200 : 503).json({ status: ready ? 'ready' : 'degraded' });
  });
  app.get('/readyz', async (_req, res) => {
    const [database, mqtt] = await Promise.all([databaseReady(), readIngestHealth()]);
    const checks = {
      ownerAuthorization: {
        mode: String(process.env['OWNER_AUTHORIZATION_MODE'] ?? 'enforce'),
        aclMode: String(process.env['OWNER_ACL_MODE'] ?? 'apply'),
        desiredGeneration: null as string | null,
        renderedGeneration: null as string | null,
        appliedGeneration: null as string | null,
        lastVerifiedAt: null as string | null,
        lastError: null as string | null,
        inventoryBaseline: {
          required: Boolean(process.env['OWNER_AUTH_INVENTORY_BASELINE_PATH']),
          ok: !process.env['OWNER_AUTH_INVENTORY_BASELINE_PATH'],
          mismatches: [] as string[],
          reason: 'owner-baseline-not-checked',
        },
      },
    };
    try {
      if (database && ownerAuthorizationAvailable()) {
        const ownerReadiness = await readOwnerAuthorizationReadiness();
        Object.assign(checks.ownerAuthorization, ownerReadiness.ownerAuthorization);
      }
    } catch (err) {
      console.error('[readyz] owner authorization check failed:', (err as Error).message);
    }
    const ownerAclGenerationReady = checks.ownerAuthorization.aclMode !== 'apply'
      || (
        checks.ownerAuthorization.desiredGeneration !== null
        && checks.ownerAuthorization.desiredGeneration === checks.ownerAuthorization.renderedGeneration
        && checks.ownerAuthorization.desiredGeneration === checks.ownerAuthorization.appliedGeneration
        && checks.ownerAuthorization.lastError === null
      );
    const ownerAclReady = ownerAclGenerationReady
      && checks.ownerAuthorization.inventoryBaseline.ok;
    const ready = !lifecycle.isDraining
      && database
      && ownerAuthorizationAvailable()
      && ownerAclReady
      && mqtt.ready
      && (mqtt.state === 'disabled' || getMqttConnectionMonitorStatus().state === 'healthy');
    // Public readiness is a documented coarse projection. Owner-authorization
    // generations, reconciliation errors and workload identity stay on the
    // local-only operator surface (/local-api/owner-authorization) - they are not needed by
    // an anonymous probe and leak control-plane state.
    res.status(ready ? 200 : 503).json(publicReadinessPayload(ready, database, mqtt.state));
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
  setForceClose(() => httpServer.closeAllConnections());
  const webSocketServer = WS_ENABLED
    ? initWebSocketServer(httpServer, pathOptions)
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
    if (pathOptions.onPathObservation) {
      lifecycle.register({ name: 'path-observations', stage: 20, close: subscribeLive(pathOptions) });
    }
  }

  const ownerAuthorizationMode = String(process.env['OWNER_AUTHORIZATION_MODE'] ?? 'enforce').toLowerCase();
  const ownerAclMode = String(process.env['OWNER_ACL_MODE'] ?? 'apply').toLowerCase();
  if (ownerAuthorizationMode === 'shadow' || ownerAclMode === 'shadow') {
    console.warn(
      `[owner-auth] WARNING: shadow mode enabled (authorization=${ownerAuthorizationMode}, acl=${ownerAclMode}) — DB-authoritative enforcement is OFF`,
    );
  }

  httpServer.listen(PORT, process.env['HTTP_HOST'] ?? '0.0.0.0', () => {
    console.log(`[app] listening on http://0.0.0.0:${PORT}`);
  });

  // Periodic message-spam analyzer (decoded channel messages -> incidents).
  //    Fire-and-forget so the heavy first pass never delays the listen above.
  void initSpamMessageAnalyzer().catch((err: unknown) => {
    console.error('[spam-msg] failed to start analyzer:', err instanceof Error ? err.message : err);
  });
}
