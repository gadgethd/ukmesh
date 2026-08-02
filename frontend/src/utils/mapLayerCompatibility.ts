import type { Filters } from '../components/FilterPanel/FilterPanel.js';

type TerrainCoverageState = Pick<Filters, 'terrain' | 'coverage'>;

/**
 * MapLibre cannot reliably drape our georeferenced HopReach image sources over
 * its 3D terrain mesh: the images become large black/distorted quadrilaterals.
 * Keep the controls deterministic by preserving whichever incompatible layer
 * the user just enabled. Existing state/URLs that already contain both prefer
 * RF coverage, which is the more specific data view.
 */
export function resolveTerrainCoverageConflict(
  current: TerrainCoverageState,
  next: Filters,
): Filters {
  if (!next.terrain || !next.coverage) return next;

  const enabledTerrain = !current.terrain && next.terrain;
  const enabledCoverage = !current.coverage && next.coverage;
  if (enabledTerrain && !enabledCoverage) return { ...next, coverage: false };
  return { ...next, terrain: false };
}
