import React from 'react';

export interface Filters {
  livePackets:       boolean;
  coverage:          boolean;
  clientNodes:       boolean;
  packetPaths:       boolean;
  betaPaths:         boolean;
  betaPathThreshold: number;  // 0–1
  links:             boolean;
  hexClashes:        boolean;
  hexClashMaxHops:   number;  // 0–3 (0 = direct only)
}

interface FilterPanelProps {
  filters:  Filters;
  onChange: (f: Filters) => void;
  betaPathConfidence?: number | null;
  betaPermutationCount?: number | null;
}

export const LinksLegend: React.FC<{ compact?: boolean; muted?: boolean }> = ({ compact = false, muted = false }) => (
  <div className={`links-legend-inline${compact ? ' links-legend-inline--compact' : ''}${muted ? ' links-legend-inline--muted' : ''}`}>
    <div className="links-legend-inline__title">Links Legend</div>
    <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#22c55e' }} /> Good (≤120 dB)</div>
    <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#fbbf24' }} /> Marginal (121-135 dB)</div>
    <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#ef4444' }} /> Weak (&gt;135 dB)</div>
    <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#d1d5db' }} /> Unknown (no dB yet)</div>
  </div>
);

export const FILTER_ROWS: Array<{ key: keyof Filters; label: string; color: string; hollow?: boolean }> = [
  { key: 'livePackets',  label: 'Live Feed',        color: '#00c4ff' },
  { key: 'packetPaths',  label: 'Packet Paths',     color: '#00c4ff', hollow: true },
  { key: 'betaPaths',    label: 'Paths (Beta)',      color: '#a855f7', hollow: true },
  { key: 'links',        label: 'Links (Beta)',     color: '#fbbf24' },
  { key: 'hexClashes',   label: 'Hex Clashes',      color: '#f97316' },
  { key: 'coverage',     label: 'Coverage',         color: '#00e676' },
  { key: 'clientNodes',  label: 'Companion / Room', color: '#ff9800' },
];

export const FilterPanel: React.FC<FilterPanelProps> = ({ filters, onChange, betaPathConfidence, betaPermutationCount }) => {
  const toggle = (key: keyof Filters) => {
    onChange({ ...filters, [key]: !filters[key] });
  };

  return (
    <div className="filter-panel">
      <div className="filter-panel__title">Layers</div>
      {filters.betaPaths && (
        <div className="filter-beta-note">
          Beta Confidence: <strong>{betaPathConfidence == null ? 'N/A' : `${Math.round(betaPathConfidence * 100)}%`}</strong>
          <br />
          Permutations: <strong>{betaPermutationCount == null ? 'N/A' : betaPermutationCount}</strong>
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
          {key === 'hexClashes' && filters.hexClashes && (
            <div className="filter-slider" onClick={(e) => e.stopPropagation()}>
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
                onChange={(e) => onChange({ ...filters, hexClashMaxHops: Number(e.target.value) })}
              />
            </div>
          )}
        </React.Fragment>
      ))}
      <LinksLegend muted={!filters.links} />
    </div>
  );
};
