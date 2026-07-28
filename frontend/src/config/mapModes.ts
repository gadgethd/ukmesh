import type { Filters } from '../components/FilterPanel/FilterPanel.js';

export type MapMode = 'explore' | 'traffic' | 'diagnose' | 'plan';

export const MAP_MODES: Array<{ id: MapMode; label: string; description: string; requiresViewshed?: boolean }> = [
  { id: 'explore', label: 'Explore', description: 'Browse repeaters and inspect individual nodes.' },
  { id: 'traffic', label: 'Live', description: 'Follow live packets and predicted relay paths.' },
  { id: 'diagnose', label: 'Diagnose', description: 'Inspect historical paths and path-hash clashes.' },
  { id: 'plan', label: 'Plan', description: 'Compare hypothetical repeater placements.', requiresViewshed: true },
];

export function isMapMode(value: string | null): value is MapMode {
  return MAP_MODES.some((mode) => mode.id === value);
}

export function filtersForMapMode(mode: MapMode, current: Filters): Filters {
  const shared = { ...current, betaPathThreshold: 0.45, hexClashMaxHops: 3 };
  switch (mode) {
    case 'traffic':
      return {
        ...shared,
        livePackets: true,
        links: false,
        terrain: false,
        clientNodes: false,
        packetHistory: false,
        heatmap: false,
        betaPaths: true,
        hexClashes: false,
      };
    case 'diagnose':
      return {
        ...shared,
        livePackets: false,
        links: false,
        terrain: false,
        clientNodes: false,
        packetHistory: true,
        heatmap: false,
        betaPaths: false,
        hexClashes: true,
      };
    case 'plan':
      return {
        ...shared,
        livePackets: false,
        links: true,
        terrain: true,
        clientNodes: false,
        packetHistory: false,
        heatmap: false,
        betaPaths: false,
        hexClashes: false,
      };
    default:
      return {
        ...shared,
        livePackets: true,
        links: false,
        terrain: false,
        clientNodes: false,
        packetHistory: false,
        heatmap: false,
        betaPaths: false,
        hexClashes: false,
      };
  }
}
