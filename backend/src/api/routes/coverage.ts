import type { RequestHandler, Router } from 'express';

export type CoverageRouteDeps = {
  coverageLimiter: RequestHandler;
  // Retained in the dependency shape for one rollback release only. The live
  // route intentionally never calls these or reads node_coverage.
  networkFilters?: unknown;
  query?: unknown;
  coverageModelVersion?: number;
};

const retiredCoverage = (_req: unknown, res: {
  status: (code: number) => { json: (body: unknown) => void };
}): void => {
  res.status(410).json({
    error: 'The legacy viewshed coverage API has been retired',
    replacement: '/rf-coverage/meta.json',
  });
};

/** Rollback-window tombstones for the rejected per-node viewshed contracts. */
export function registerCoverageRoutes(router: Router, deps: CoverageRouteDeps): void {
  router.get('/coverage', deps.coverageLimiter, retiredCoverage);
  router.get('/coverage/:nodeId', deps.coverageLimiter, retiredCoverage);
}
