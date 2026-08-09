import { Request, Response, Router } from 'express';
import {
  CHARTS_CACHE_TTL_MS,
  CHARTS_CACHE_STALE_TTL_MS,
  INFERRED_NODES_CACHE_TTL_MS,
  NODE_LINKS_CACHE_TTL_MS,
  OWNER_DASHBOARD_CACHE_TTL_MS,
  OWNER_LIVE_CACHE_TTL_MS,
  PATH_HISTORY_CACHE_TTL_MS,
  STATS_CACHE_TTL_MS,
  chartsCache,
  chartsInflight,
  inferredNodesCache,
  inferredNodesInflight,
  nodeLinksCache,
  nodeLinksInflight,
  ownerLiveCache,
  pathHistoryCache,
  statsCache,
} from './bootstrap/caches.js';
import {
  getMultibytePathSegments,
  getNodes,
  getNodeHistory,
  getNodeAdverts,
  getPacketDetail,
  getPathHistoryCache,
  getPublicVisibilityGeneration,
  getChannelMessageHistory,
  getRecentPacketEvents,
  getRecentPackets,
  analyticsQuery,
  pool,
  query,
} from '../db/index.js';
import {
  autoLinkOwnerNodeIds,
  buildOwnerDashboard,
  invalidateOwnerNodeIdCache,
  resolveOwnerNodeIds,
  verifyMqttCredentials,
} from '../owner/ownerAccess.js';
import { encryptOwnerSession, getOwnerSession, isSecureRequest } from '../owner/ownerSession.js';
import { getResolveCache, setResolveCache, getHeldPath, setHeldPath } from '../path-beta/resolveCache.js';
import { resolvePool } from '../path-beta/resolvePool.js';
import { maskDecodedPathNodes } from '../stats/maskDecodedPathNodes.js';
import {
  COVERAGE_LIMITER,
  EXPENSIVE_LIMITER,
  EXPORT_LIMITER,
  NODES_LIMITER,
  OWNER_LOGIN_LIMITER,
  PACKET_DETAIL_LIMITER,
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
import { registerPlannedCoverageRoutes } from './routes/plannedCoverage.js';
import { registerOwnerRoutes } from './routes/owner.js';
import { registerPathingRoutes } from './routes/pathing.js';
import { registerStatsRoutes } from './routes/stats.js';
import { registerTelemetryRoutes } from './routes/telemetry.js';
import { createNodeRepository } from '../repositories/nodes.js';
import { createPlannedCoverageRepository } from '../repositories/plannedCoverage.js';
import { registerSpamRoutes } from './routes/spam.js';
import { registerTopologyRoutes } from './routes/topology.js';
import { registerActivityTimelineRoutes } from './routes/activityTimeline.js';
import { registerRfValidationRoutes } from './routes/rfValidation.js';
import { registerExportRoutes } from './routes/exports.js';
import { registerProductFeatureRoutes } from './routes/productFeatures.js';
import { requireLocalOnly } from './utils/localOnly.js';
import { networkFilters } from './utils/networkFilters.js';
import { normalizeObserverQuery } from './utils/observer.js';
import {
  PublicAllScopeForbiddenError,
  InvalidPublicNetworkScopeError,
  resolvePublicNetworkScope,
} from '../http/requestScope.js';
import { assertUniqueRouteRegistry } from './routeRegistry.js';
import { assertContractCoverage } from './contracts.js';
import { ApiInputError, wrapAsyncHandlers } from './errors.js';

const router = Router();
// Anonymous cross-network aggregation is not a public API capability. Operator
// diagnostics must use separately authenticated/local-only entry points.
router.use((req, res, next) => {
  try {
    resolvePublicNetworkScope(req.query['network'], req.headers);
    normalizeObserverQuery(req.query['observer']);
  } catch (error) {
    if (
      !(error instanceof PublicAllScopeForbiddenError)
      && !(error instanceof InvalidPublicNetworkScopeError)
      && !(error instanceof ApiInputError)
    ) {
      next(error);
      return;
    }
    res.status(400).json({
      error: error instanceof PublicAllScopeForbiddenError
        ? 'The all-network scope is not available on public endpoints'
        : 'Invalid public request scope',
    });
    return;
  }
  next();
});
router.use(healthRoutes);
router.use(nodeStatusRoutes);
router.use(radioRoutes);
registerProductFeatureRoutes(router, query);
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
  const nodeIds = await resolveOwnerNodeIds(session.mqttUsername);
  if (nodeIds.length < 1) {
    res.clearCookie(OWNER_COOKIE_NAME, { path: '/' });
    res.status(401).json({ error: 'Owner authorization has been revoked' });
    return null;
  }
  return nodeIds;
}

registerCoverageRoutes(router, {
  coverageLimiter: COVERAGE_LIMITER,
  networkFilters,
  query,
});
registerPlannedCoverageRoutes(router, {
  coverageLimiter: COVERAGE_LIMITER,
  plannedCoverageRepository: createPlannedCoverageRepository(pool),
});
registerNodeRoutes(router, {
  getNodes,
  getNodeHistory,
  getNodeAdverts,
  nodeRepository: createNodeRepository(query),
  requireLocalOnly,
  networkFilters,
  getPublicVisibilityGeneration,
  inferredNodesCache,
  inferredNodesInflight,
  inferredNodesCacheTtlMs: INFERRED_NODES_CACHE_TTL_MS,
  nodeLinksCache,
  nodeLinksInflight,
  nodeLinksCacheTtlMs: NODE_LINKS_CACHE_TTL_MS,
  nodesLimiter: NODES_LIMITER,
});
registerMiscRoutes(router, {
  query,
  getRecentPackets,
  getRecentPacketEvents,
  getPacketDetail,
  getChannelMessageHistory,
  getPublicVisibilityGeneration,
  packetDetailLimiter: PACKET_DETAIL_LIMITER,
});
registerOwnerRoutes(router, {
  ownerCookieName: OWNER_COOKIE_NAME,
  ownerLiveCacheTtlMs: OWNER_LIVE_CACHE_TTL_MS,
  ownerLiveCache,
  ownerDashboardCacheTtlMs: OWNER_DASHBOARD_CACHE_TTL_MS,
  ownerLastHopCacheTtlMs: OWNER_LAST_HOP_CACHE_TTL_MS,
  ownerSessionTtlMs: OWNER_SESSION_TTL_MS,
  mqttUsernameMaxLen: MQTT_USERNAME_MAX_LEN,
  mqttPasswordMaxLen: MQTT_PASSWORD_MAX_LEN,
  ownerLoginLimiter: OWNER_LOGIN_LIMITER,
  hasControlChars,
  verifyMqttCredentials,
  resolveOwnerNodeIds,
  autoLinkOwnerNodeIds,
  buildOwnerDashboard,
  encryptOwnerSession,
  isSecureRequest,
  getOwnerSession: getRouteOwnerSession,
  requireOwnerSession,
  invalidateOwnerNodeIdCache,
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
  getHeldPath,
  setHeldPath,
  resolvePool,
  getPathHistoryCache,
  getPublicVisibilityGeneration,
  getMultibytePathSegments,
  query,
});
registerStatsRoutes(router, {
  statsCache,
  statsCacheTtlMs: STATS_CACHE_TTL_MS,
  chartsCache,
  chartsCacheTtlMs: CHARTS_CACHE_TTL_MS,
  chartsSnapshotStaleTtlMs: CHARTS_CACHE_STALE_TTL_MS,
  chartsInflight,
  expensiveLimiter: EXPENSIVE_LIMITER,
  statsChartsLimiter: STATS_CHARTS_LIMITER,
  networkFilters,
  query,
  analyticsQuery,
  getPublicVisibilityGeneration,
  maskDecodedPathNodes,
});
registerTelemetryRoutes(router, { query });
registerSpamRoutes(router, { expensiveLimiter: EXPENSIVE_LIMITER });
registerTopologyRoutes(router, {
  query,
  networkFilters,
  getPublicVisibilityGeneration,
  limiter: EXPENSIVE_LIMITER,
});
registerActivityTimelineRoutes(router, {
  query,
  networkFilters,
  limiter: STATS_CHARTS_LIMITER,
});
registerRfValidationRoutes(router, {
  query,
  networkFilters,
  limiter: EXPENSIVE_LIMITER,
});
registerExportRoutes(router, {
  query,
  networkFilters,
  exportLimiter: EXPORT_LIMITER,
});

assertUniqueRouteRegistry(router);
assertContractCoverage(router);
wrapAsyncHandlers(router);

export default router;
