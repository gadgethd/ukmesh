import React, { useId } from 'react';
import { visibleFilterRows } from '../../features/map/layers.js';
import type { Filters } from '../../types/filters.js';

export function MapLayerOptions({ filters, onChange, heatmapEnabled, rfCoverageEnabled }: {
  filters: Filters; onChange: (filters: Filters) => void;
  heatmapEnabled: boolean; rfCoverageEnabled: boolean;
}) {
  const id = useId();
  const rows = visibleFilterRows(heatmapEnabled, rfCoverageEnabled);
  const renderRows = (advanced: boolean) => rows.filter(row => !!row.advanced === advanced).map(({ key, label, color, hollow }) => <React.Fragment key={key}>
    <button type="button" className="filter-row" aria-pressed={!!filters[key]}
      onClick={() => onChange({ ...filters, [key]: !filters[key] })}>
      <span className="filter-row__label">
        <span className={`filter-dot${hollow ? ' filter-dot--hollow' : ''}`}
          style={{ [hollow ? 'borderColor' : 'background']: color, opacity: filters[key] ? 1 : 0.4 }} />
        {label}
      </span>
      <span className={`filter-toggle${filters[key] ? ' filter-toggle--on' : ''}`}
        style={filters[key] ? { background: `${color}22`, borderColor: color } : {}} />
    </button>
    {key === 'hexClashes' && filters.hexClashes && <div className="filter-slider">
      <label className="filter-slider__label" htmlFor={id}>Hex clash hops: {Math.round(filters.hexClashMaxHops)}</label>
      <input id={id} className="filter-slider__input" type="range" min={0} max={3} step={1}
        value={Math.round(filters.hexClashMaxHops)}
        onChange={event => onChange({ ...filters, hexClashMaxHops: Number(event.target.value) })} />
    </div>}
  </React.Fragment>);
  return <>
    {renderRows(false)}
    <details className="map-advanced-layers" open={rows.some(row => row.advanced && filters[row.key])}>
      <summary>Advanced layers</summary>
      <p>3D terrain changes map height. Hex clashes mark nodes with matching short IDs.</p>
      {renderRows(true)}
    </details>
  </>;
}
