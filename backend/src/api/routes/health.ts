import { Router } from 'express';
import { getWorkerHealthOverview } from '../../health/status.js';
import { HealthSnapshotCache } from '../../health/snapshot.js';

const router = Router();
const DEEP_HEALTH_INTERVAL_MS = Math.min(
  5 * 60_000,
  Math.max(15_000, Number(process.env['HEALTH_DEEP_INTERVAL_MS'] ?? 60_000) || 60_000),
);
const DEEP_HEALTH_HARD_TTL_MS = Math.max(DEEP_HEALTH_INTERVAL_MS * 2, 5 * 60_000);
const snapshot = new HealthSnapshotCache(
  getWorkerHealthOverview,
  DEEP_HEALTH_HARD_TTL_MS,
);

const refreshTimer = setInterval(() => {
  void snapshot.refresh();
}, DEEP_HEALTH_INTERVAL_MS);
refreshTimer.unref();
setImmediate(() => {
  void snapshot.refresh();
});

router.get('/health', (_req, res) => {
  const current = snapshot.read();
  if (!current.ready) {
    res.setHeader('Cache-Control', 'public, max-age=5');
    res.status(503).json({
      status: 'initializing',
      generatedAt: current.generatedAt == null
        ? null
        : new Date(current.generatedAt).toISOString(),
      lastError: current.lastError,
    });
    return;
  }
  res.setHeader('Cache-Control', 'public, max-age=15, stale-while-revalidate=60');
  res.json(current.data);
});

export default router;
