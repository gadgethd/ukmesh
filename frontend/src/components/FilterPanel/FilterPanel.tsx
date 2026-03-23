import React from 'react';
import { useOverlayStore } from '../../store/overlayStore.js';

export interface Filters {
  livePackets:       boolean;
  links:             boolean;
  terrain:           boolean;
  clientNodes:       boolean;
  packetHistory:     boolean;
  betaPaths:         boolean;
  betaPathThreshold: number;  // 0–1
  hexClashes:        boolean;
  hexClashMaxHops:   number;  // 0–3 (0 = direct only)
}

interface FilterPanelProps {
  filters:  Filters;
  onChange: (f: Filters) => void;
  betaPathConfidence?: number | null;
  betaPermutationCount?: number | null;
  betaRemainingHops?: number | null;
}

export const LinksLegend: React.FC<{ compact?: boolean; muted?: boolean }> = ({ compact = false, muted = false }) => (
  <div className={`links-legend-inline${compact ? ' links-legend-inline--compact' : ''}${muted ? ' links-legend-inline--muted' : ''}`}>
    <div className="links-legend-inline__title">Links Legend</div>
    <div className="links-legend-inline__grid">
      <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#22c55e' }} /> Good (≤121.5 dB)</div>
      <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#fbbf24' }} /> Marginal (121.5-129.5 dB)</div>
      <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#ef4444' }} /> Weak (&gt;129.5 dB)</div>
      <div className="links-legend-inline__row"><span className="links-legend__swatch" style={{ background: '#d1d5db' }} /> Unknown (no dB yet)</div>
    </div>
  </div>
);

export const FILTER_ROWS: Array<{ key: keyof Filters; label: string; color: string; hollow?: boolean }> = [
  { key: 'livePackets',  label: 'Live Feed',        color: '#00c4ff' },
  { key: 'terrain',      label: '3D Terrain',       color: '#60a5fa' },
  { key: 'packetHistory', label: 'Paths',            color: '#00c4ff', hollow: true },
  { key: 'betaPaths',    label: 'Live Path',         color: '#a855f7', hollow: true },
  { key: 'hexClashes',   label: 'Hex Clashes',      color: '#f97316' },
  { key: 'clientNodes',  label: 'Companion / Room', color: '#ff9800' },
];

export const FilterPanel: React.FC<FilterPanelProps> = ({ filters, onChange, betaPathConfidence, betaPermutationCount, betaRemainingHops }) => {
  const liveBetaPathConfidence = useOverlayStore((state) => state.betaPathConfidence);
  const liveBetaPermutationCount = useOverlayStore((state) => state.betaPermutationCount);
  const liveBetaRemainingHops = useOverlayStore((state) => state.betaRemainingHops);
  const resolvedConfidence = betaPathConfidence ?? liveBetaPathConfidence;
  const resolvedPermutations = betaPermutationCount ?? liveBetaPermutationCount;
  const resolvedRemainingHops = betaRemainingHops ?? liveBetaRemainingHops;
  const toggle = (key: keyof Filters) => {
    onChange({ ...filters, [key]: !filters[key] });
  };

  return (
    <div className="filter-panel">
      <div className="filter-panel__title">Layers</div>
      {filters.betaPaths && (
        <div className="filter-beta-note">
          Beta Confidence: <strong>{resolvedConfidence == null ? 'N/A' : `${Math.round(resolvedConfidence * 100)}%`}</strong>
          <br />
          Permutations: <strong>{resolvedPermutations == null ? 'N/A' : resolvedPermutations}</strong>
          <br />
          Remaining Hops: <strong>{resolvedRemainingHops == null ? 'N/A' : resolvedRemainingHops}</strong>
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
    </div>
  );
};
