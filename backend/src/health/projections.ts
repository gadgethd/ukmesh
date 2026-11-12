import type { PrivacyGenerationHealth } from '../privacy/generationHealth.js';

export type HealthProblem = {
  code: string;
  severity: 'warning' | 'critical';
  message: string;
};

export function privacyGenerationProblem(state: PrivacyGenerationHealth): HealthProblem | null {
  return state.healthy ? null : {
    code: 'privacy_generation_unhealthy',
    severity: 'critical',
    message: `Privacy generation check failed: ${state.reason}`,
  };
}

export function databaseVacuumBacklogProblem(tablesNeedingVacuum: number): HealthProblem | null {
  if (!Number.isFinite(tablesNeedingVacuum) || tablesNeedingVacuum <= 0) return null;
  return {
    code: 'database_vacuum_backlog',
    severity: tablesNeedingVacuum >= 3 ? 'critical' : 'warning',
    message: `${Math.floor(tablesNeedingVacuum)} database table(s) exceed the configured vacuum trigger (10,000 dead-row minimum)`,
  };
}

export function privacyRematerializationBacklogProblem(state: {
  pending: number;
  processing: number;
  failed: number;
  failedRetryable: number;
  failedExhausted: number;
  oldestPendingAgeSeconds: number | null;
}): HealthProblem | null {
  const age = state.oldestPendingAgeSeconds ?? 0;
  const severity = age > 6 * 60 * 60 || state.failedExhausted > 0
    ? 'critical'
    : age > 30 * 60 || state.failedRetryable > 0 ? 'warning' : null;
  if (!severity) return null;
  return {
    code: 'privacy_rematerialization_backlog',
    severity,
    message: `Privacy rematerialization pending=${state.pending} processing=${state.processing} `
      + `failed=${state.failed} oldest_pending_age_minutes=${Math.floor(age / 60)}`,
  };
}

/**
 * Anonymous browser diagnostics are reported to operators but are deliberately
 * absent from the authoritative status calculation.
 */
export function summarizeAuthoritativeHealth(
  problems: readonly HealthProblem[],
  frontendErrors: number,
): {
  status: 'healthy' | 'degraded' | 'critical';
  frontendErrors: number;
} {
  return {
    status: problems.some((problem) => problem.severity === 'critical')
      ? 'critical'
      : problems.length > 0 ? 'degraded' : 'healthy',
    frontendErrors: Math.max(0, Math.floor(frontendErrors) || 0),
  };
}

export function toPublicHealthOverview(
  detail: {
    status: string;
    system: { generated_at: string; disk: { used_pct: number | null } };
    maintenance: { active: boolean; message: string | null };
    problems: readonly HealthProblem[];
    ingest: { packet_age_minutes: number | null };
    workers: readonly { status: string }[];
  },
) {
  const storagePct = detail.system.disk.used_pct;
  return {
    status: detail.status,
    generatedAt: detail.system.generated_at,
    maintenance: detail.maintenance,
    incidents: detail.problems.map((problem) => ({
      code: problem.code,
      severity: problem.severity,
    })),
    components: {
      ingest: {
        status: detail.ingest.packet_age_minutes == null
          ? 'unknown'
          : detail.ingest.packet_age_minutes > 30 ? 'stale' : 'ok',
      },
      workers: {
        status: detail.workers.some((worker) => worker.status === 'failed')
          ? 'failed'
          : detail.workers.some((worker) => worker.status === 'running')
            ? 'running'
            : 'idle',
      },
      storage: {
        status: storagePct == null
          ? 'unknown'
          : storagePct >= 90 ? 'critical' : storagePct >= 80 ? 'warning' : 'ok',
      },
    },
  };
}
