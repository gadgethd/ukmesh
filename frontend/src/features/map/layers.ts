import type { Filters } from '../../types/filters.js';

type FilterRow = {
  key: keyof Filters; label: string; color: string; hollow?: boolean;
  requiresRfCoverage?: boolean; advanced?: boolean;
};
export const FILTER_ROWS: FilterRow[] = [
  { key: 'livePackets', label: 'Live Feed', color: '#00c4ff' },
  { key: 'terrain', label: '3D Terrain', color: '#60a5fa', advanced: true },
  { key: 'coverage', label: 'RF Coverage', color: '#22c55e', requiresRfCoverage: true },
  { key: 'packetHistory', label: 'Paths', color: '#00c4ff', hollow: true },
  { key: 'heatmap', label: 'Packet Heatmap', color: '#ef4444', advanced: true },
  { key: 'betaPaths', label: 'Live Path', color: '#a855f7', hollow: true, advanced: true },
  { key: 'hexClashes', label: 'Hex Clashes', color: '#f97316', advanced: true },
  { key: 'clientNodes', label: 'Companion / Room', color: '#ff9800' },
];
export const visibleFilterRows = (heatmapEnabled: boolean, rfCoverageEnabled = false): FilterRow[] =>
  FILTER_ROWS.filter(({ key, requiresRfCoverage }) =>
    (key !== 'heatmap' || heatmapEnabled) && (!requiresRfCoverage || rfCoverageEnabled));
