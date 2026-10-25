// Explicit retirement registry. Migration 044 removed path_history_cache after
// its API and worker were removed. On-demand path resolution and path-learning
// are separate, supported workloads; never retire them by association.
export const RETIRED_ANALYSIS_WORKLOADS: ReadonlyMap<string, string> = new Map([
  ['path-history', 'API and worker removed; cache dropped by migration 044. Use on-demand paths and path-learning.'],
]);

type AnalysisState = {
  workload: string;
  lastStatus: string | null;
  lastTerminalReason: string | null;
  lastError: string | null;
  activeRunId?: string | null;
};

export function normalizeRetiredAnalysisState(state: AnalysisState) {
  const reason = RETIRED_ANALYSIS_WORKLOADS.get(state.workload);
  // An unexpected active run needs investigation, even for a retired workload.
  if (!reason || state.activeRunId) return {
    lastStatus: state.lastStatus,
    lastTerminalReason: state.lastTerminalReason,
    lastError: state.lastError,
  };
  return { lastStatus: 'unavailable', lastTerminalReason: 'retired', lastError: null };
}

export function analysisRetirement(state: AnalysisState) {
  const reason = RETIRED_ANALYSIS_WORKLOADS.get(state.workload);
  return reason ? {
    reason,
    unexpectedActiveRun: Boolean(state.activeRunId),
    // Retain the actual last failure for operators; retirement is not success.
    historicalState: {
      lastStatus: state.lastStatus,
      lastTerminalReason: state.lastTerminalReason,
      lastError: state.lastError,
    },
  } : null;
}
