import { createHash } from 'node:crypto';
import type { RequestHandler, Router } from 'express';
import {
  plannedCoverageAdmissionDecision,
  type PlannedCoverageRepository,
} from '../../repositories/plannedCoverage.js';

export { plannedCoverageAdmissionDecision };

export type PlannedCoverageRouteDeps = {
  coverageLimiter: RequestHandler;
  plannedCoverageRepository: PlannedCoverageRepository;
};

const LEGACY_PLAN_ID_RE = /^plan_[0-9a-f]{16}$/;
const PLAN_HANDLE_RE = /^planv2_[0-9a-f]{64}$/;

// Retained only to decode rollback-era capabilities during this release.
export function plannedCoverageHandleDigest(handle: string): {
  hash: string;
  algorithm: 'sha256' | 'md5';
} | null {
  if (PLAN_HANDLE_RE.test(handle)) {
    return { hash: createHash('sha256').update(handle).digest('hex'), algorithm: 'sha256' };
  }
  if (LEGACY_PLAN_ID_RE.test(handle)) {
    return { hash: createHash('md5').update(handle).digest('hex'), algorithm: 'md5' };
  }
  return null;
}

export function registerPlannedCoverageRoutes(router: Router, deps: PlannedCoverageRouteDeps): void {
  const retired = (_req: unknown, res: {
    status: (code: number) => { json: (body: unknown) => void };
  }): void => {
    res.status(410).json({
      error: 'The planned viewshed API has been retired',
      replacement: '/rf-coverage/meta.json',
    });
  };

  // Keep repository wiring intact for a rollback release, but perform no live
  // reads, writes, cleanup, or queue admission.
  void deps.plannedCoverageRepository;
  router.post('/coverage/planned', deps.coverageLimiter, retired);
  router.get('/coverage/planned/:planId', deps.coverageLimiter, retired);
  router.delete('/coverage/planned/:planId', deps.coverageLimiter, retired);
}
