import { Request, Response, Router } from 'express';
import {
  CHARTS_CACHE_TTL_MS,
  COVERAGE_CACHE_TTL_MS,
  CROSS_NETWORK_CACHE_TTL_MS,
  INFERRED_NODES_CACHE_TTL_MS,
  OWNER_LIVE_CACHE_TTL_MS,
  PATH_HISTORY_CACHE_TTL_MS,
  STATS_CACHE_TTL_MS,
  chartsCache,
  chartsInflight,
  coverageCache,
  crossNetworkCache,
  inferredNodesCache,
  ownerLiveCache,
  pathHistoryCache,
  statsCache,
} from './bootstrap/caches.js';
import { getNodes, getNodeHistory, getNodeAdverts, getPathHistoryCache, getRecentPacketEvents, getRecentPackets, query } from '../db/index.js';
import { resolveRequestNetwork } from '../http/requestScope.js';
import { autoLinkOwnerNodeIds, buildOwnerDashboard, listMappedOwnerNodeIds, resolveOwnerNodeIds, verifyMqttCredentials } from '../owner/ownerAccess.js';
import { encryptOwnerSession, getOwnerSession, isSecureRequest } from '../owner/ownerSession.js';
import { getResolveCache, setResolveCache } from '../path-beta/resolveCache.js';
import { resolvePool } from '../path-beta/resolvePool.js';
import { maskDecodedPathNodes } from '../stats/maskDecodedPathNodes.js';
import {
  COVERAGE_LIMITER,
  EXPENSIVE_LIMITER,
  OWNER_LOGIN_LIMITER,
  PATH_BETA_LIMITER,
  PATH_HISTORY_LIMITER,
  PATH_LEARNING_LIMITER,
  STATS_CHARTS_LIMITER,
} from './bootstrap/limiters.js';
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
import { requireLocalOnly } from './utils/localOnly.js';
import { networkFilters } from './utils/networkFilters.js';
import { normalizeObserverQuery } from './utils/observer.js';

const router = Router();
router.use(healthRoutes);
router.use(nodeStatusRoutes);
router.use(radioRoutes);
const OWNER_COOKIE_NAME = 'meshcore_owner_session';
const OWNER_SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const OWNER_LAST_HOP_CACHE_TTL_MS = 60 * 60 * 1000;
const MQTT_USERNAME_MAX_LEN = 128;
const MQTT_PASSWORD_MAX_LEN = 128;

function hasControlChars(value: string): boolean {
  return /[\u0000-\u001F\u007F]/.test(value);
}

function getRouteOwnerSession(req: Request) {
  return getOwnerSession(req, OWNER_COOKIE_NAME);
}

async function requireOwnerSession(req: Request, res: Response): Promise<string[] | null> {
  const session = getRouteOwnerSession(req);
  if (!session) {
    res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
    res.status(401).json({ error: 'Not logged in' });
    return null;
  }
  return session.nodeIds;
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
  ownerLastHopCacheTtlMs: OWNER_LAST_HOP_CACHE_TTL_MS,
  ownerSessionTtlMs: OWNER_SESSION_TTL_MS,
  mqttUsernameMaxLen: MQTT_USERNAME_MAX_LEN,
  mqttPasswordMaxLen: MQTT_PASSWORD_MAX_LEN,
  ownerLoginLimiter: OWNER_LOGIN_LIMITER,
  hasControlChars,
  verifyMqttCredentials,
  resolveOwnerNodeIds,
  autoLinkOwnerNodeIds,
  listMappedOwnerNodeIds,
  buildOwnerDashboard,
  encryptOwnerSession,
  isSecureRequest,
  getOwnerSession: getRouteOwnerSession,
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
