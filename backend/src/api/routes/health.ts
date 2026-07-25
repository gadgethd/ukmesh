import { Router } from 'express';
import { getWorkerHealthOverview } from '../../health/status.js';

const router = Router();
const DEEP_HEALTH_INTERVAL_MS = Math.min(
  5 * 60_000,
  Math.max(15_000, Number(process.env['HEALTH_DEEP_INTERVAL_MS'] ?? 60_000) || 60_000),
);
const DEEP_HEALTH_HARD_TTL_MS = Math.max(DEEP_HEALTH_INTERVAL_MS * 2, 5 * 60_000);
let snapshot: { generatedAt: number; data: unknown } | null = null;
let refreshInFlight: Promise<void> | null = null;

function refreshDeepHealthSnapshot(): Promise<void> {
  if (refreshInFlight) return refreshInFlight;
  const tracked = getWorkerHealthOverview()
    .then((data) => {
      snapshot = { generatedAt: Date.now(), data };
    })
    .catch((err: unknown) => {
      console.error('[health] scheduled deep snapshot failed:', (err as Error).message);
    })
    .finally(() => {
      if (refreshInFlight === tracked) refreshInFlight = null;
    });
  refreshInFlight = tracked;
  return tracked;
}

const refreshTimer = setInterval(() => void refreshDeepHealthSnapshot(), DEEP_HEALTH_INTERVAL_MS);
refreshTimer.unref();
setImmediate(() => void refreshDeepHealthSnapshot());

router.get('/health', (_req, res) => {
  const current = snapshot;
  if (!current || Date.now() - current.generatedAt > DEEP_HEALTH_HARD_TTL_MS) {
    res.setHeader('Cache-Control', 'public, max-age=5');
    res.status(503).json({
      status: 'initializing',
      generatedAt: current ? new Date(current.generatedAt).toISOString() : null,
    });
    return;
  }
  res.setHeader('Cache-Control', 'public, max-age=15, stale-while-revalidate=60');
  res.json(current.data);
});

export default router;
