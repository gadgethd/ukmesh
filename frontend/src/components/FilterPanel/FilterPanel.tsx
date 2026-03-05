import React from 'react';

export interface Filters {
  livePackets:       boolean;
  coverage:          boolean;
  clientNodes:       boolean;
  packetPaths:       boolean;
  betaPaths:         boolean;
  betaPathThreshold: number;  // 0–1
  links:             boolean;
}

interface FilterPanelProps {
  filters:  Filters;
  onChange: (f: Filters) => void;
  betaPathConfidence?: number | null;
}

export const FILTER_ROWS: Array<{ key: keyof Filters; label: string; color: string; hollow?: boolean }> = [
  { key: 'livePackets',  label: 'Live Feed',        color: '#00c4ff' },
  { key: 'packetPaths',  label: 'Packet Paths',     color: '#00c4ff', hollow: true },
  { key: 'betaPaths',    label: 'Paths (Beta)',      color: '#a855f7', hollow: true },
  { key: 'links',        label: 'Links (Beta)',     color: '#fbbf24' },
  { key: 'coverage',     label: 'Coverage',         color: '#00e676' },
  { key: 'clientNodes',  label: 'Companion / Room', color: '#ff9800' },
];

export const FilterPanel: React.FC<FilterPanelProps> = ({ filters, onChange, betaPathConfidence }) => {
  const toggle = (key: keyof Filters) => {
    onChange({ ...filters, [key]: !filters[key] });
  };

  const applyPreset = (preset: 'minimal' | 'links' | 'beta') => {
    const next: Filters = { ...filters };
    if (preset === 'minimal') {
      next.livePackets = true;
      next.packetPaths = false;
      next.betaPaths = false;
      next.links = false;
      next.coverage = false;
      next.clientNodes = false;
    } else if (preset === 'links') {
      next.livePackets = true;
      next.packetPaths = false;
      next.betaPaths = true;
      next.links = true;
      next.coverage = false;
      next.clientNodes = false;
    } else {
      next.livePackets = true;
      next.packetPaths = false;
      next.betaPaths = true;
      next.links = true;
      next.coverage = true;
      next.clientNodes = false;
    }
    onChange(next);
  };

  const confidenceLabel = betaPathConfidence == null
    ? 'N/A'
    : betaPathConfidence >= 0.75
      ? 'High'
      : betaPathConfidence >= 0.5
        ? 'Medium'
        : 'Low';

  return (
    <div className="filter-panel">
      <div className="filter-panel__title">Layers</div>
      <div className="filter-presets">
        <button className="filter-preset-btn" onClick={() => applyPreset('minimal')}>Minimal</button>
        <button className="filter-preset-btn" onClick={() => applyPreset('links')}>Links + Beta</button>
        <button className="filter-preset-btn" onClick={() => applyPreset('beta')}>Full Beta</button>
      </div>
      {filters.betaPaths && (
        <div className="filter-beta-note">
          Beta Confidence: <strong>{confidenceLabel}</strong>
          {betaPathConfidence != null && ` (${Math.round(betaPathConfidence * 100)}%)`}
        </div>
      )}
      {FILTER_ROWS.map(({ key, label, color, hollow }) => (
        <React.Fragment key={key}>
          <div
            className="filter-row"
            onClick={() => toggle(key)}
            role="button"
            aria-pressed={filters[key] as boolean}
          >
            <span className="filter-row__label">
              {hollow ? (
                <span
                  className="filter-dot filter-dot--hollow"
                  style={{
                    borderColor: color,
                    opacity:     filters[key] ? 1 : 0.4,
                  }}
                />
              ) : (
                <span className="filter-dot" style={{ background: color, opacity: filters[key] ? 1 : 0.3 }} />
              )}
              {label}
            </span>
            <span className={`filter-toggle ${filters[key] ? 'filter-toggle--on' : ''}`}
                  style={filters[key] ? { background: `${color}22`, borderColor: color } : {}}
            />
          </div>
          {key === 'betaPaths' && filters.betaPaths && (
            <div className="filter-slider" onClick={(e) => e.stopPropagation()}>
              <span className="filter-slider__label">
                Confidence: {Math.round(filters.betaPathThreshold * 100)}%
              </span>
              <input
                className="filter-slider__input"
                type="range"
                min={0}
                max={100}
                step={5}
                value={Math.round(filters.betaPathThreshold * 100)}
                onChange={(e) => onChange({ ...filters, betaPathThreshold: Number(e.target.value) / 100 })}
              />
            </div>
          )}
        </React.Fragment>
      ))}
    </div>
  );
};
