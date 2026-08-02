import React, { useState } from 'react';
import type maplibregl from 'maplibre-gl';
import { NodeSearch } from '../Map/NodeSearch.js';
import { visibleFilterRows, type Filters } from '../FilterPanel/FilterPanel.js';
import type { MapMode } from '../../config/mapModes.js';
import { MapModeSelector } from './MapModeSelector.js';
import { WatchlistPanel } from './WatchlistPanel.js';
import { useOverlayStore } from '../../store/overlayStore.js';

type MobileControlsProps = {
  map: maplibregl.Map | null;
  filters: Filters;
  onFiltersChange: (next: Filters) => void;
  activeMode: MapMode | null;
  viewshedEnabled: boolean;
  heatmapEnabled: boolean;
  onModeChange: (mode: MapMode) => void;
  onShare: () => void;
  shareLabel: string;
  onNodeSelect: (nodeId: string) => void;
  fullScreenMap: boolean;
  onToggleFullScreenMap: () => void;
};

export const MobileControls: React.FC<MobileControlsProps> = ({
  map,
  filters,
  onFiltersChange,
  activeMode,
  viewshedEnabled,
  heatmapEnabled,
  onModeChange,
  onShare,
  shareLabel,
  onNodeSelect,
  fullScreenMap,
  onToggleFullScreenMap,
}) => {
  const [showFilters, setShowFilters] = useState(false);
  const customLosMode = useOverlayStore((state) => state.customLosMode);
  const planRepeaterMode = useOverlayStore((state) => state.planRepeaterMode);
  const setCustomLosMode = useOverlayStore((state) => state.setCustomLosMode);
  const clearCustomLos = useOverlayStore((state) => state.clearCustomLos);
  const setPlanRepeaterMode = useOverlayStore((state) => state.setPlanRepeaterMode);

  return (
    <div className="mobile-controls">
      <button
        type="button"
        className="mobile-fullscreen-toggle"
        onClick={onToggleFullScreenMap}
        aria-pressed={fullScreenMap}
      >
        {fullScreenMap ? 'Restore panels' : 'Full-screen map'}
      </button>
      {!fullScreenMap && <MapModeSelector
        activeMode={activeMode}
        viewshedEnabled={viewshedEnabled}
        onChange={onModeChange}
        onShare={onShare}
        shareLabel={shareLabel}
      />}
      {!fullScreenMap && (
        <div className="mobile-map-tools" aria-label="Map planning tools">
          <button
            type="button"
            className={customLosMode ? 'mobile-map-tools__button mobile-map-tools__button--active' : 'mobile-map-tools__button'}
            aria-pressed={customLosMode}
            onClick={() => {
              if (customLosMode) clearCustomLos();
              else {
                setPlanRepeaterMode(false);
                setCustomLosMode(true);
              }
            }}
          >
            Line of sight
          </button>
          {viewshedEnabled && (
            <button
              type="button"
              className={planRepeaterMode ? 'mobile-map-tools__button mobile-map-tools__button--active' : 'mobile-map-tools__button'}
              aria-pressed={planRepeaterMode}
              onClick={() => {
                if (planRepeaterMode) setPlanRepeaterMode(false);
                else {
                  clearCustomLos();
                  setPlanRepeaterMode(true);
                }
              }}
            >
              Plan repeater
            </button>
          )}
        </div>
      )}
      {!fullScreenMap && <button
        type="button"
        className="mobile-legend-toggle"
        onClick={() => setShowFilters((v) => !v)}
        aria-expanded={showFilters}
      >
        <span>Layers</span>
        <span>{showFilters ? 'Hide' : 'Show'}</span>
      </button>}
      {!fullScreenMap && <div className={`mobile-filter-wrap${showFilters ? '' : ' mobile-filter-wrap--hidden'}`}>
        <div className="mobile-filter-grid">
          {visibleFilterRows(viewshedEnabled, heatmapEnabled).map(({ key, label, color, hollow }) => (
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
            <label className="filter-slider__label" htmlFor="mobile-hex-clash-hops">
              Hex clash hops: {Math.round(filters.hexClashMaxHops)}
            </label>
            <input
              id="mobile-hex-clash-hops"
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
      </div>}
      {!fullScreenMap && <div className="mobile-search">
        <NodeSearch map={map} onNodeSelect={onNodeSelect} />
      </div>}
      {!fullScreenMap && <WatchlistPanel />}
    </div>
  );
};
