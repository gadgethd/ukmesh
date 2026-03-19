import { useEffect, useSyncExternalStore } from 'react';
import { withScopeParams, type ApiScope } from '../utils/api.js';

export interface NodeCoverage {
  node_id: string;
  geom: { type: string; coordinates: unknown };
  strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  antenna_height_m?: number;
  radius_m?: number;
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

export function useCoverageData(): NodeCoverage[] {
  return useSyncExternalStore(subscribe, () => state.coverage);
}

export function useCoverageLoader(scope: ApiScope = {}, enabled = false): void {
  useEffect(() => {
    if (!enabled) return;

    const key = scopeKey(scope);
    if (state.loadedScopeKey === key && state.coverage.length > 0) return;

    const controller = new AbortController();
    const url = withScopeParams('/api/coverage', scope);

    fetch(url, { signal: controller.signal })
      .then((response) => response.json())
      .then((coverage: NodeCoverage[]) => {
        if (!controller.signal.aborted) replaceCoverage(coverage, key);
      })
      .catch(() => {
        // non-fatal
      });

    return () => controller.abort();
  }, [enabled, scope.network, scope.observer]);
}
