import React, { useEffect, useRef, useState } from 'react';
import type * as maplibregl from 'maplibre-gl';
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
  rfCoverageEnabled: boolean;
  heatmapEnabled: boolean;
  onModeChange: (mode: MapMode) => void;
  onShare: () => void;
  shareLabel: string;
  onNodeSelect: (nodeId: string) => void;
  fullScreenMap: boolean;
  onToggleFullScreenMap: () => void;
  feedOpen: boolean;
  onToggleFeed: () => void;
};

export const MobileControls: React.FC<MobileControlsProps> = ({
  map,
  filters,
  onFiltersChange,
  activeMode,
  viewshedEnabled,
  rfCoverageEnabled,
  heatmapEnabled,
  onModeChange,
  onShare,
  shareLabel,
  onNodeSelect,
  fullScreenMap,
  onToggleFullScreenMap,
  feedOpen,
  onToggleFeed,
}) => {
  const [menuOpen, setMenuOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const customLosMode = useOverlayStore((state) => state.customLosMode);
  const planRepeaterMode = useOverlayStore((state) => state.planRepeaterMode);
  const setCustomLosMode = useOverlayStore((state) => state.setCustomLosMode);
  const clearCustomLos = useOverlayStore((state) => state.clearCustomLos);
  const setPlanRepeaterMode = useOverlayStore((state) => state.setPlanRepeaterMode);

  // Close the menu on Escape or on any tap outside the control cluster.
  useEffect(() => {
    if (!menuOpen) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (rootRef.current && target && !rootRef.current.contains(target)) {
        setMenuOpen(false);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setMenuOpen(false);
    };
    document.addEventListener('pointerdown', onPointerDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [menuOpen]);

  const filterRows = visibleFilterRows(viewshedEnabled, heatmapEnabled, rfCoverageEnabled);
  const activeFilterCount = filterRows.filter(({ key }) => filters[key]).length;

  if (fullScreenMap) {
    return (
      <div className="mobile-controls mobile-controls--fullscreen" ref={rootRef}>
        <button
          type="button"
          className="mobile-fullscreen-toggle"
          onClick={onToggleFullScreenMap}
        >
          Restore panels
        </button>
      </div>
    );
  }

  return (
    <div className="mobile-controls" ref={rootRef}>
      <div className="mobile-controls__bar">
        <button
          type="button"
          className="mobile-menu-toggle"
          onClick={() => setMenuOpen((value) => !value)}
          aria-expanded={menuOpen}
          aria-haspopup="dialog"
          aria-controls="mobile-map-menu"
        >
          <span className="mobile-menu-toggle__icon" aria-hidden="true">☰</span>
          <span className="mobile-menu-toggle__label">Layers</span>
          {activeFilterCount > 0 && (
            <span className="mobile-menu-toggle__badge">{activeFilterCount}</span>
          )}
        </button>
        <button
          type="button"
          className="mobile-feed-toggle"
          onClick={onToggleFeed}
          aria-pressed={feedOpen && filters.livePackets}
        >
          <span aria-hidden="true">▤</span>
          Live
        </button>
      </div>
      {menuOpen && (
        <div className="mobile-menu" id="mobile-map-menu" role="dialog" aria-label="Map settings">
          <div className="mobile-menu__head">
            <span className="mobile-menu__title">Map settings</span>
            <button
              type="button"
              className="mobile-menu__close"
              onClick={() => setMenuOpen(false)}
              aria-label="Close menu"
            >
              ✕
            </button>
          </div>
          <div className="mobile-menu__body">
            <section className="mobile-menu__section" aria-label="Layers">
              <h3 className="mobile-menu__heading">Layers</h3>
              <div className="mobile-filter-grid">
                {filterRows.map(({ key, label, color, hollow }) => (
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
                <div className="filter-slider" style={{ margin: '10px 0 0' }}>
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
            </section>
            <section className="mobile-menu__section" aria-label="View">
              <h3 className="mobile-menu__heading">View</h3>
              <MapModeSelector
                activeMode={activeMode}
                viewshedEnabled={viewshedEnabled}
                onChange={onModeChange}
                onShare={onShare}
                shareLabel={shareLabel}
              />
              <button
                type="button"
                className="mobile-fullscreen-toggle"
                onClick={onToggleFullScreenMap}
              >
                Full-screen map
              </button>
            </section>
            <section className="mobile-menu__section" aria-label="Map planning tools">
              <h3 className="mobile-menu__heading">Tools</h3>
              <div className="mobile-map-tools">
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
            </section>
            <section className="mobile-menu__section" aria-label="Find a node">
              <h3 className="mobile-menu__heading">Find</h3>
              <div className="mobile-search">
                <NodeSearch map={map} onNodeSelect={onNodeSelect} />
              </div>
            </section>
            <section className="mobile-menu__section" aria-label="Watchlist">
              <WatchlistPanel />
            </section>
          </div>
        </div>
      )}
    </div>
  );
};
