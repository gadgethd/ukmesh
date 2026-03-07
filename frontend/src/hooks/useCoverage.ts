import { useState, useCallback, useEffect } from 'react';

export interface NodeCoverage {
  node_id:          string;
  geom:             { type: string; coordinates: unknown };
  strength_geoms?:  Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  antenna_height_m?: number;
  radius_m?:        number;
  calculated_at?:   string;
}

export function useCoverage(network?: string) {
  const [coverage, setCoverage] = useState<NodeCoverage[]>([]);

  // Fetch all stored polygons on mount
  useEffect(() => {
    const url = network ? `/api/coverage?network=${encodeURIComponent(network)}` : '/api/coverage';
    fetch(url)
      .then((r) => r.json())
      .then((data: NodeCoverage[]) => setCoverage(data))
      .catch(() => { /* non-fatal */ });
  }, [network]);

  // Called when a coverage_update WS message arrives
  const handleCoverageUpdate = useCallback((update: { node_id: string; geom: NodeCoverage['geom']; strength_geoms?: NodeCoverage['strength_geoms'] }) => {
    setCoverage((prev) => {
      const filtered = prev.filter((c) => c.node_id !== update.node_id);
      return [...filtered, { node_id: update.node_id, geom: update.geom, strength_geoms: update.strength_geoms }];
    });
  }, []);

  return { coverage, handleCoverageUpdate };
}
