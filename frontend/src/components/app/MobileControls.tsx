import React, { useState } from 'react';
import type maplibregl from 'maplibre-gl';
import { NodeSearch } from '../Map/NodeSearch.js';
import { FILTER_ROWS, type Filters } from '../FilterPanel/FilterPanel.js';
import type { MapMode } from '../../config/mapModes.js';
import { MapModeSelector } from './MapModeSelector.js';

type MobileControlsProps = {
  map: maplibregl.Map | null;
  filters: Filters;
  onFiltersChange: (next: Filters) => void;
  activeMode: MapMode | null;
  viewshedEnabled: boolean;
  onModeChange: (mode: MapMode) => void;
  onShare: () => void;
  shareLabel: string;
  onNodeSelect: (nodeId: string) => void;
};

export const MobileControls: React.FC<MobileControlsProps> = ({
  map,
  filters,
  onFiltersChange,
  activeMode,
  viewshedEnabled,
  onModeChange,
  onShare,
  shareLabel,
  onNodeSelect,
}) => {
  const [showFilters, setShowFilters] = useState(false);

  return (
    <div className="mobile-controls">
      <MapModeSelector
        activeMode={activeMode}
        viewshedEnabled={viewshedEnabled}
        onChange={onModeChange}
        onShare={onShare}
        shareLabel={shareLabel}
      />
      <button
        type="button"
        className="mobile-legend-toggle"
        onClick={() => setShowFilters((v) => !v)}
        aria-expanded={showFilters}
      >
        <span>Layers</span>
        <span>{showFilters ? 'Hide' : 'Show'}</span>
      </button>
      <div className={`mobile-filter-wrap${showFilters ? '' : ' mobile-filter-wrap--hidden'}`}>
        <div className="mobile-filter-grid">
          {FILTER_ROWS.map(({ key, label, color, hollow }) => (
            <button
              type="button"
              key={key}
              className={`filter-row${filters[key] ? ' filter-row--on' : ''}`}
              onClick={() => onFiltersChange({ ...filters, [key]: !filters[key] })}
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
            </button>
          ))}
        </div>
        {filters.hexClashes && (
          <div className="filter-slider" style={{ margin: '0 8px 8px' }}>
            <span className="filter-slider__label">
              Hex clash hops: {Math.round(filters.hexClashMaxHops)}
            </span>
            <input
              className="filter-slider__input"
              type="range"
              min={0}
              max={3}
              step={1}
              value={Math.round(filters.hexClashMaxHops)}
              onChange={(e) => onFiltersChange({ ...filters, hexClashMaxHops: Number(e.target.value) })}
            />
          </div>
        )}
      </div>
      <div className="mobile-search">
        <NodeSearch map={map} onNodeSelect={onNodeSelect} />
      </div>
    </div>
  );
};
