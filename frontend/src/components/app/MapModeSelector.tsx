import React from 'react';
import { MAP_MODES, type MapMode } from '../../config/mapModes.js';

type MapModeSelectorProps = {
  activeMode: MapMode | null;
  viewshedEnabled: boolean;
  onChange: (mode: MapMode) => void;
  onShare: () => void;
  shareLabel: string;
};

export const MapModeSelector: React.FC<MapModeSelectorProps> = ({
  activeMode,
  viewshedEnabled,
  onChange,
  onShare,
  shareLabel,
}) => (
  <div className="map-modes" aria-label="Map mode">
    <div className="map-modes__buttons">
      {MAP_MODES.filter((mode) => viewshedEnabled || !mode.requiresViewshed).map((mode) => (
        <button
          key={mode.id}
          type="button"
          className={`map-modes__button${activeMode === mode.id ? ' map-modes__button--active' : ''}`}
          aria-pressed={activeMode === mode.id}
          title={mode.description}
          onClick={() => onChange(mode.id)}
        >
          {mode.label}
        </button>
      ))}
    </div>
    <button type="button" className="map-modes__share" onClick={onShare}>
      {shareLabel}
    </button>
  </div>
);
