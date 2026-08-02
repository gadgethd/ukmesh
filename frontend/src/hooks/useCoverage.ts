import type { ApiScope } from '../utils/api.js';
import { canonicalNodeId } from '../utils/nodeIds.js';

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
  scopeKey: string | null;
  epoch: number;
  coverage: NodeCoverage[];
  loadedScopeKey: string | null;
};
let state: CoverageState = {
  scopeKey: null,
  epoch: 0,
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

function acceptsEpoch(epoch: number | undefined): boolean {
  return epoch === undefined || epoch === state.epoch;
}

function reset(key: string): number {
  const epoch = state.epoch + 1;
  setState({
    scopeKey: key,
    epoch,
    coverage: [],
    loadedScopeKey: null,
  });
  return epoch;
}

function normalizeCoverage(entry: NodeCoverage): NodeCoverage {
  return { ...entry, node_id: canonicalNodeId(entry.node_id) };
}

function replaceCoverage(coverage: NodeCoverage[], key: string, epoch?: number): void {
  if (!acceptsEpoch(epoch) || key !== state.scopeKey) return;
  setState({
    ...state,
    coverage: coverage.map(normalizeCoverage),
    loadedScopeKey: key,
  });
}

function upsertCoverageBatch(
  updates: Array<{
    node_id: string;
    geom: NodeCoverage['geom'];
    strength_geoms?: NodeCoverage['strength_geoms'];
  }>,
  epoch?: number,
): void {
  if (!acceptsEpoch(epoch) || updates.length === 0) return;
  const normalizedUpdates = updates.map((update) => ({
    ...update,
    node_id: canonicalNodeId(update.node_id),
  }));
  const idsToRemove = new Set(normalizedUpdates.map((update) => update.node_id));
  const filtered = state.coverage.filter((entry) => !idsToRemove.has(entry.node_id));
  const added = normalizedUpdates.map((update) => ({
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
}, epoch?: number): void {
  upsertCoverageBatch([update], epoch);
}

function handleCoverageUpdateBatch(updates: Array<{
  node_id: string;
  geom: NodeCoverage['geom'];
  strength_geoms?: NodeCoverage['strength_geoms'];
}>, epoch?: number): void {
  upsertCoverageBatch(updates, epoch);
}

function getState(): CoverageState {
  return state;
}

export const coverageStore = {
  subscribe,
  getState,
  reset,
  replaceCoverage,
  handleCoverageUpdate,
  handleCoverageUpdateBatch,
  scopeKey,
};
