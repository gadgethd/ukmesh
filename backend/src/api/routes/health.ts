import { Router } from 'express';
import { HEALTH_CACHE_TTL_MS, healthSnapshot } from '../bootstrap/caches.js';
import { toPublicHealthOverview } from '../../health/status.js';

const router = Router();
const refreshTimer = setInterval(() => {
  void healthSnapshot.refresh();
}, HEALTH_CACHE_TTL_MS);
refreshTimer.unref();
setImmediate(() => {
  void healthSnapshot.refresh();
});

router.get('/health', (_req, res) => {
  const current = healthSnapshot.read();
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
  res.setHeader('Cache-Control', 'public, max-age=30, stale-while-revalidate=60');
  res.json(toPublicHealthOverview(current.data));
});

export default router;
