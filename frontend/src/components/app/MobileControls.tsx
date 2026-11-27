import React, { useEffect, useState } from 'react';
import type * as maplibregl from 'maplibre-gl';
import { NodeSearch } from '../Map/NodeSearch.js';
import { visibleFilterRows } from '../../features/map/layers.js';
import { MapLayerOptions } from './MapLayerOptions.js';
import { Dialog, DialogTitle } from '../ui/Dialog.js';
import { useMediaQuery } from '../../hooks/useMediaQuery.js';
import type { Filters } from '../../types/filters.js';
import type { MapMode } from '../../config/mapModes.js';
import { MapModeSelector } from './MapModeSelector.js';
import { WatchlistPanel } from './WatchlistPanel.js';
import { useOverlayStore } from '../../store/overlayStore.js';
import type { PublicScope } from '../../utils/scope.js';

type MobileControlsProps = {
  healthUrl: string;
  map: maplibregl.Map | null;
  scope: PublicScope;
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
  onShowShortcuts: () => void;
  fullScreenMap: boolean;
  onToggleFullScreenMap: () => void;
  feedOpen: boolean;
  onToggleFeed: () => void;
};

export const MobileControls: React.FC<MobileControlsProps> = ({
  healthUrl,
  map,
  scope,
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
  onShowShortcuts,
  fullScreenMap,
  onToggleFullScreenMap,
  feedOpen,
  onToggleFeed,
}) => {
  const [panel, setPanel] = useState<'layers' | 'search' | null>(null);
  const menuOpen = panel === 'layers';
  const searchOpen = panel === 'search';
  const closePanel = () => setPanel(null);
  const compact = useMediaQuery('(max-width: 640px)');
  useEffect(() => { if (!compact) setPanel(null); }, [compact]);
  const customLosMode = useOverlayStore((state) => state.customLosMode);
  const planRepeaterMode = useOverlayStore((state) => state.planRepeaterMode);
  const setCustomLosMode = useOverlayStore((state) => state.setCustomLosMode);
  const clearCustomLos = useOverlayStore((state) => state.clearCustomLos);
  const setPlanRepeaterMode = useOverlayStore((state) => state.setPlanRepeaterMode);

  const filterRows = visibleFilterRows(heatmapEnabled, rfCoverageEnabled);
  const activeFilterCount = filterRows.filter(({ key }) => filters[key]).length;

  if (fullScreenMap) {
    return (
      <div className="mobile-controls mobile-controls--fullscreen">
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
    <div className="mobile-controls">
      <div className="mobile-controls__bar">
        <button type="button" className="mobile-menu-toggle" onClick={() => setPanel('search')}
          aria-expanded={searchOpen} aria-haspopup="dialog" aria-controls="mobile-map-search">Find</button>
        <button
          type="button"
          className="mobile-menu-toggle"
          onClick={() => setPanel(menuOpen ? null : 'layers')}
          aria-expanded={menuOpen}
          aria-haspopup="dialog"
          aria-controls="mobile-map-menu"
        >
          <span className="mobile-menu-toggle__icon" aria-hidden="true">☰</span>
          <span className="mobile-menu-toggle__label">Layers</span>
          {activeFilterCount > 0 && (
            <span className="mobile-menu-toggle__badge" aria-hidden="true">{activeFilterCount}</span>
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
      {panel && compact && (
        <Dialog id={searchOpen ? 'mobile-map-search' : 'mobile-map-menu'} isOpen
          onOpenChange={open => { if (!open) closePanel(); }}
          ariaLabel={searchOpen ? 'Find on the map' : 'Map settings'} className="mobile-menu" overlayClassName="mobile-map-overlay">
          <div className="mobile-menu__head">
            <DialogTitle className="mobile-menu__title">{searchOpen ? 'Find on the map' : 'Map settings'}</DialogTitle>
            <button
              type="button"
              className="mobile-menu__close"
              onClick={closePanel}
              aria-label="Close menu"
            >
              ✕
            </button>
          </div>
          <div className="mobile-menu__body">
            {searchOpen ? (
              <div className="mobile-search">
                <NodeSearch map={map} scope={scope} onNodeSelect={id => { onNodeSelect(id); closePanel(); }} />
              </div>
            ) : <>
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
                onClick={() => { closePanel(); onToggleFullScreenMap(); }}
              >
                Full-screen map
              </button>
            </section>
            <section className="mobile-menu__section" aria-label="Layers">
              <h3 className="mobile-menu__heading">Layers</h3>
              <MapLayerOptions filters={filters} onChange={onFiltersChange}
                heatmapEnabled={heatmapEnabled} rfCoverageEnabled={rfCoverageEnabled} />
            </section>

            <section className="mobile-menu__section" aria-label="Map planning tools">
              <h3 className="mobile-menu__heading">Tools</h3>
              <div className="mobile-map-tools">
                <a className="mobile-map-tools__button topbar__health-link" href={healthUrl}>Health Check</a>
                <button type="button" className="mobile-map-tools__button" onClick={() => {
                  closePanel();
                  // Restore the Layers trigger before opening the next dialog.
                  requestAnimationFrame(() => requestAnimationFrame(onShowShortcuts));
                }}>Keyboard shortcuts</button>
                <button
                  type="button"
                  className={customLosMode ? 'mobile-map-tools__button mobile-map-tools__button--active' : 'mobile-map-tools__button'}
                  aria-pressed={customLosMode}
                  onClick={() => {
                    closePanel();
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
                      closePanel();
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
            <section className="mobile-menu__section" aria-label="Watchlist">
              <WatchlistPanel />
            </section>
            </>}
          </div>
        </Dialog>
      )}
    </div>
  );
};
