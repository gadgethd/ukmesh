import React from 'react';
import type { Map as LeafletMap } from 'leaflet';
import { NodeSearch } from '../Map/NodeSearch.js';
import { FILTER_ROWS, LinksLegend, type Filters } from '../FilterPanel/FilterPanel.js';
import type { MeshNode } from '../../hooks/useNodes.js';

type MobileControlsProps = {
  map: LeafletMap | null;
  nodes: Map<string, MeshNode>;
  filters: Filters;
  onFiltersChange: (next: Filters) => void;
};

export const MobileControls: React.FC<MobileControlsProps> = ({
  map,
  nodes,
  filters,
  onFiltersChange,
}) => (
  <div className="mobile-controls">
    <div className="mobile-filter-grid">
      {FILTER_ROWS.map(({ key, label, color, hollow }) => (
        <div
          key={key}
          className={`filter-row${filters[key] ? ' filter-row--on' : ''}`}
          onClick={() => onFiltersChange({ ...filters, [key]: !filters[key] })}
          role="button"
          aria-pressed={!!filters[key]}
        >
          <span className="filter-row__label">
            {hollow ? (
              <span className="filter-dot filter-dot--hollow" style={{ borderColor: color, opacity: filters[key] ? 1 : 0.4 }} />
            ) : (
              <span className="filter-dot" style={{ background: color, opacity: filters[key] ? 1 : 0.3 }} />
            )}
            {label}
          </span>
          <span
            className={`filter-toggle${filters[key] ? ' filter-toggle--on' : ''}`}
            style={filters[key] ? { background: `${color}22`, borderColor: color } : {}}
          />
        </div>
      ))}
    </div>
    <div className="mobile-links-legend-wrap">
      <LinksLegend compact muted={!filters.links} />
    </div>
    <div className="mobile-search">
      <NodeSearch map={map} nodes={nodes} />
    </div>
  </div>
);
