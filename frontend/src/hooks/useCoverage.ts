import type { ApiScope } from '../utils/api.js';

export interface NodeCoverage {
  node_id: string;
  geom: { type: string; coordinates: unknown };
  strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  antenna_height_m?: number;
  radius_m?: number;
  // Predicted RF links to nearby real repeaters — only present for planned (plan_) rows.
  predicted_links?: import('../components/Map/types.js').PredictedLink[];
  calculated_at?: string;
}

type CoverageState = {
  coverage: NodeCoverage[];
  loadedScopeKey: string | null;
};
let state: CoverageState = {
  coverage: [],
  loadedScopeKey: null,
};

const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function setState(next: CoverageState): void {
  state = next;
  emit();
}

function scopeKey(scope: ApiScope = {}): string {
  return `${scope.network ?? 'all'}|${scope.observer ?? 'all'}`;
}

function replaceCoverage(coverage: NodeCoverage[], key: string): void {
  setState({
    coverage,
    loadedScopeKey: key,
  });
}

function upsertCoverageBatch(
  updates: Array<{
    node_id: string;
    geom: NodeCoverage['geom'];
    strength_geoms?: NodeCoverage['strength_geoms'];
  }>,
): void {
  if (updates.length === 0) return;
  const idsToRemove = new Set(updates.map((update) => update.node_id));
  const filtered = state.coverage.filter((entry) => !idsToRemove.has(entry.node_id));
  const added = updates.map((update) => ({
    node_id: update.node_id,
    geom: update.geom,
    strength_geoms: update.strength_geoms,
  }));
  setState({
    ...state,
    coverage: [...filtered, ...added],
  });
}

function handleCoverageUpdate(update: {
  node_id: string;
  geom: NodeCoverage['geom'];
  strength_geoms?: NodeCoverage['strength_geoms'];
}): void {
  upsertCoverageBatch([update]);
}

function handleCoverageUpdateBatch(updates: Array<{
  node_id: string;
  geom: NodeCoverage['geom'];
  strength_geoms?: NodeCoverage['strength_geoms'];
}>): void {
  upsertCoverageBatch(updates);
}

function getState(): CoverageState {
  return state;
}

export const coverageStore = {
  subscribe,
  getState,
  replaceCoverage,
  handleCoverageUpdate,
  handleCoverageUpdateBatch,
  scopeKey,
};
