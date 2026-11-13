import React from 'react';
import { useOverlayStore } from '../../store/overlayStore.js';
import type { MapMode } from '../../config/mapModes.js';
import { MapModeSelector } from '../app/MapModeSelector.js';
import { WatchlistPanel } from '../app/WatchlistPanel.js';
import type { Filters } from '../../types/filters.js';

interface FilterPanelProps {
  filters:  Filters;
  onChange: (f: Filters) => void;
  activeMode: MapMode | null;
  viewshedEnabled: boolean;
  rfCoverageEnabled: boolean;
  heatmapEnabled: boolean;
  onModeChange: (mode: MapMode) => void;
  onShare: () => void;
  shareLabel: string;
  collapsed: boolean;
  onToggleCollapse: () => void;
  /** True while a node detail panel is docked on the right. */
  nodeOpen?: boolean;
}

import { visibleFilterRows } from '../../features/map/layers.js';
import { MapLayerOptions } from '../app/MapLayerOptions.js';
export { FILTER_ROWS, visibleFilterRows } from '../../features/map/layers.js';

const activeLayerCount = (filters: Filters, heatmapEnabled: boolean, rfCoverageEnabled: boolean): number =>
  visibleFilterRows(heatmapEnabled, rfCoverageEnabled).reduce(
    (count, { key }) => count + (filters[key] ? 1 : 0),
    0,
  );

export const FilterPanel: React.FC<FilterPanelProps> = ({
  filters, onChange,
  activeMode, viewshedEnabled, rfCoverageEnabled, heatmapEnabled, onModeChange, onShare, shareLabel,
  collapsed, onToggleCollapse, nodeOpen = false,
}) => {
  const liveBetaPathConfidence = useOverlayStore((state) => state.betaPathConfidence);
  const liveBetaPermutationCount = useOverlayStore((state) => state.betaPermutationCount);
  const liveBetaRemainingHops = useOverlayStore((state) => state.betaRemainingHops);
  const pathExplanation = useOverlayStore((state) => state.pathExplanation);
  const resolvedConfidence = liveBetaPathConfidence;
  const resolvedPermutations = liveBetaPermutationCount;
  const resolvedRemainingHops = liveBetaRemainingHops;

  if (collapsed) {
    const count = activeLayerCount(filters, heatmapEnabled, rfCoverageEnabled);
    return (
      <button
        type="button"
        className={`filter-launcher${nodeOpen ? ' filter-launcher--node-open' : ''}`}
        onClick={onToggleCollapse}
        aria-expanded={false}
        aria-label="Show map layers"
      >
        <svg className="filter-launcher__icon" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polygon points="12 2 2 7 12 12 22 7 12 2" /><polyline points="2 17 12 22 22 17" /><polyline points="2 12 12 17 22 12" />
        </svg>
        <span>Layers</span>
        {count > 0 && <span className="filter-launcher__badge">{count}</span>}
      </button>
    );
  }

  return (
    <div className={`filter-panel${nodeOpen ? ' filter-panel--node-open' : ''}`}>
      <div className="filter-panel__head">
        <div className="filter-panel__title filter-panel__title--head">View</div>
        <button
          type="button"
          className="filter-panel__collapse"
          onClick={onToggleCollapse}
          aria-label="Collapse layers panel"
          title="Collapse"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6" />
          </svg>
        </button>
      </div>
      <MapModeSelector
        activeMode={activeMode}
        viewshedEnabled={viewshedEnabled}
        onChange={onModeChange}
        onShare={onShare}
        shareLabel={shareLabel}
      />
      <WatchlistPanel />
      <div className="filter-panel__title filter-panel__title--layers">Layers</div>
      {filters.betaPaths && (
        <div className="filter-beta-note">
          Beta Confidence: <strong>{resolvedConfidence == null ? 'N/A' : `${Math.round(resolvedConfidence * 100)}%`}</strong>
          <br />
          Permutations: <strong>{resolvedPermutations == null ? 'N/A' : resolvedPermutations}</strong>
          <br />
          Remaining Hops: <strong>{resolvedRemainingHops == null ? 'N/A' : resolvedRemainingHops}</strong>
          {pathExplanation && (
            <details className="filter-beta-explanation">
              <summary>Why this path?</summary>
              <p>{pathExplanation.summary}</p>
              <ul>{pathExplanation.reasons.slice(0, 3).map((reason) => <li key={reason}>{reason}</li>)}</ul>
              {pathExplanation.limitations?.[0] && <small>{pathExplanation.limitations[0]}</small>}
            </details>
          )}
        </div>
      )}
      <MapLayerOptions filters={filters} onChange={onChange}
        heatmapEnabled={heatmapEnabled} rfCoverageEnabled={rfCoverageEnabled} />
    </div>
  );
};
