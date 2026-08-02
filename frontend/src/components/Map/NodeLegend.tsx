import React, { useState } from 'react';
import { MAP_OVERLAY_COLORS } from './mapConfig.js';

const LEGEND_KEY = 'meshcore-legend-open';

export const NodeLegend: React.FC<{ mapLight: boolean }> = ({ mapLight }) => {
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
  const colors = MAP_OVERLAY_COLORS[mapLight ? 'light' : 'dark'];
  const nodeRows: Array<{ label: string; color: string; ring?: boolean }> = [
    { label: 'Repeater', color: colors.repeater },
    { label: 'Companion', color: colors.companion },
    { label: 'Room server', color: colors.roomServer },
    { label: 'Sensor', color: colors.sensor },
    { label: 'Offline / stale', color: colors.stale },
    { label: 'Inferred', color: colors.inferred },
    { label: 'Selected', color: colors.selected, ring: true },
  ];
  const linkRows = [
    { label: 'Good link', color: colors.linkGood, width: 3 },
    { label: 'Marginal link', color: colors.linkMarginal, width: 2.1 },
    { label: 'Poor link', color: colors.linkPoor, width: 1.4 },
  ];

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
          {nodeRows.map(({ label, color, ring }) => (
            <li key={label} className="node-legend__row">
              <span
                className={`node-legend__swatch${ring ? ' node-legend__swatch--ring' : ''}`}
                style={ring ? { background: color, boxShadow: `0 0 0 2px ${colors.selectedStroke}, 0 0 6px ${color}` } : { background: color }}
              />
              {label}
            </li>
          ))}
          {linkRows.map(({ label, color, width }) => (
            <li key={label} className="node-legend__row">
              <span
                className="node-legend__link-swatch"
                style={{ borderTopColor: color, borderTopWidth: width }}
              />
              {label}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};
