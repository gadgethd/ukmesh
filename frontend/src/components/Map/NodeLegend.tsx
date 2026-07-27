import React, { useState } from 'react';

// Colours mirror the `node-dots` circle-color expression in MapLibreMap so the
// legend always matches what's actually drawn on the map.
const LEGEND_ROWS: Array<{ label: string; color: string; ring?: boolean }> = [
  { label: 'Repeater',      color: '#00c4ff' },
  { label: 'Companion',     color: '#ff9f43' },
  { label: 'Room server',   color: '#a78bfa' },
  { label: 'Sensor',        color: '#34d399' },
  { label: 'Offline / stale', color: '#6b7280' },
  { label: 'Inferred',      color: '#7dd3fc' },
  { label: 'Selected',      color: '#8af4ff', ring: true },
];

const LEGEND_KEY = 'meshcore-legend-open';

export const NodeLegend: React.FC = () => {
  const [open, setOpen] = useState<boolean>(() => {
    try { return localStorage.getItem(LEGEND_KEY) === '1'; } catch { return false; }
  });

  const toggle = () => {
    setOpen((prev) => {
      const next = !prev;
      try { localStorage.setItem(LEGEND_KEY, next ? '1' : '0'); } catch { /* ignore */ }
      return next;
    });
  };

  return (
    <div className={`node-legend${open ? ' node-legend--open' : ''}`}>
      <button
        type="button"
        className="node-legend__toggle"
        onClick={toggle}
        aria-expanded={open}
        aria-controls="node-legend-body"
      >
        <span className="node-legend__toggle-icon" aria-hidden="true" />
        Legend
        <span className="node-legend__chevron" aria-hidden="true">{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <ul id="node-legend-body" className="node-legend__body">
          {LEGEND_ROWS.map(({ label, color, ring }) => (
            <li key={label} className="node-legend__row">
              <span
                className={`node-legend__swatch${ring ? ' node-legend__swatch--ring' : ''}`}
                style={ring ? { background: color, boxShadow: `0 0 0 2px #fff, 0 0 6px ${color}` } : { background: color }}
              />
              {label}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};
