import type { Request, Router } from 'express';
import type { HealthSnapshotRead } from '../../health/snapshot.js';
import { toPublicHealthOverview } from '../../health/status.js';

type HealthDetail = Parameters<typeof toPublicHealthOverview>[0];

export type HealthRouteDeps = {
  readSnapshot: () => HealthSnapshotRead<HealthDetail>;
  isLocalClient: (req: Request) => boolean;
  hasOperatorAuthorization: (req: Request) => boolean;
};

function minimalPublicStatus(status: string): 'ok' | 'degraded' {
  return status === 'healthy' ? 'ok' : 'degraded';
}

/**
 * UM-03: the full overview (incident codes, component status, worker names,
 * maintenance notes) is operator diagnostics. Anonymous callers only receive a
 * bounded status word, while local/operator callers keep the current response.
 */
export function registerHealthRoutes(router: Router, deps: HealthRouteDeps): void {
  router.get('/health', (req, res) => {
    const current = deps.readSnapshot();
    const detailed = deps.isLocalClient(req) || deps.hasOperatorAuthorization(req);
    if (!current.ready) {
      res.setHeader('Cache-Control', 'public, max-age=5');
      if (!detailed) {
        res.status(503).json({ status: 'degraded' });
        return;
      }
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
    const overview = toPublicHealthOverview(current.data);
    if (!detailed) {
      res.json({ status: minimalPublicStatus(overview.status) });
      return;
    }
    res.json(overview);
  });
}
