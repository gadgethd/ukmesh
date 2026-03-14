import { useState, useCallback, useEffect } from 'react';
import { withScopeParams, type ApiScope } from '../utils/api.js';

export interface NodeCoverage {
  node_id:          string;
  geom:             { type: string; coordinates: unknown };
  strength_geoms?:  Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  antenna_height_m?: number;
  radius_m?:        number;
  calculated_at?:   string;
}

export function useCoverage(scope: ApiScope = {}) {
  const [coverage, setCoverage] = useState<NodeCoverage[]>([]);

  // Fetch all stored polygons on mount
  useEffect(() => {
    const url = withScopeParams('/api/coverage', scope);
    fetch(url)
      .then((r) => r.json())
      .then((data: NodeCoverage[]) => setCoverage(data))
      .catch(() => { /* non-fatal */ });
  }, [scope.network, scope.observer]);

  // Called when a coverage_update WS message arrives
  const handleCoverageUpdate = useCallback((update: { node_id: string; geom: NodeCoverage['geom']; strength_geoms?: NodeCoverage['strength_geoms'] }) => {
    setCoverage((prev) => {
      const filtered = prev.filter((c) => c.node_id !== update.node_id);
      return [...filtered, { node_id: update.node_id, geom: update.geom, strength_geoms: update.strength_geoms }];
    });
  }, []);

  const handleCoverageUpdateBatch = useCallback((updates: { node_id: string; geom: NodeCoverage['geom']; strength_geoms?: NodeCoverage['strength_geoms'] }[]) => {
    if (updates.length === 0) return;
    setCoverage((prev) => {
      const idsToRemove = new Set(updates.map(u => u.node_id));
      const filtered = prev.filter((c) => !idsToRemove.has(c.node_id));
      const added = updates.map(u => ({ node_id: u.node_id, geom: u.geom, strength_geoms: u.strength_geoms }));
      return [...filtered, ...added];
    });
  }, []);

  return { coverage, handleCoverageUpdate, handleCoverageUpdateBatch };
}
