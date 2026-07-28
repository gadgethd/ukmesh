import React from 'react';
import { StatsPanel } from '../StatsPanel/StatsPanel.js';
import type { WSReadyState } from '../../hooks/useWebSocket.js';
import type { DashboardStats } from '../../hooks/useDashboardStats.js';

type AppTopBarProps = {
  homeUrl: string;
  wsState: WSReadyState;
  onShowDisclaimer: () => void;
  stats: DashboardStats;
  mapLight: boolean;
  onToggleMapTheme: () => void;
  network: string;
  onNetworkChange: (network: string) => void;
  annotation: string;
  onEditAnnotation: () => void;
  onShowShortcuts: () => void;
  highContrast: boolean;
  onToggleContrast: () => void;
};

const ConnIndicator: React.FC<{ state: WSReadyState }> = ({ state }) => (
  <div className="conn-indicator" role="status" aria-live="polite" aria-label={`Live connection: ${state}`}>
    <span className={`conn-dot ${state === 'connected' ? 'conn-dot--connected' : ''}`} />
    <span style={{ color: state === 'connected' ? 'var(--online)' : 'var(--text-muted)' }}>
      {state === 'connected' ? 'LIVE' : state === 'connecting' ? 'CONNECTING' : 'OFFLINE'}
    </span>
  </div>
);

const MeshIcon: React.FC = () => (
  <svg viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
    <circle cx="10" cy="4"  r="2" fill="currentColor" />
    <circle cx="3"  cy="16" r="2" fill="currentColor" />
    <circle cx="17" cy="16" r="2" fill="currentColor" />
    <line x1="10" y1="6" x2="3"  y2="14" stroke="currentColor" strokeWidth="1.2" />
    <line x1="10" y1="6" x2="17" y2="14" stroke="currentColor" strokeWidth="1.2" />
    <line x1="3"  y1="16" x2="17" y2="16" stroke="currentColor" strokeWidth="1.2" />
    <circle cx="10" cy="10" r="1.5" fill="currentColor" opacity="0.6" />
  </svg>
);

export const AppTopBar: React.FC<AppTopBarProps> = ({
  homeUrl,
  wsState,
  onShowDisclaimer,
  stats,
  mapLight,
  onToggleMapTheme,
  network,
  onNetworkChange,
  annotation,
  onEditAnnotation,
  onShowShortcuts,
  highContrast,
  onToggleContrast,
}) => (
  <header className="topbar">
    <a href={homeUrl} className="topbar__brand" title="Home">
      <span className="topbar__brand-icon">◈</span>
      UK Mesh
    </a>
    <div className="topbar__divider" />
    <div className="topbar__logo">
      <MeshIcon />
      Live Map
    </div>
    <div className="topbar__divider" />
    <ConnIndicator state={wsState} />
    <select
      className="topbar__network"
      value={network}
      onChange={(event) => onNetworkChange(event.target.value)}
      aria-label="Network region"
      title="Switch network"
    >
      <option value="ukmesh">UK Mesh</option>
      <option value="teesside">Teesside</option>
    </select>
    <button type="button" className="topbar__tool-btn" onClick={onToggleMapTheme} title="Toggle map theme">
      {mapLight ? '☀ Light' : '☾ Dark'}
    </button>
    <button type="button" className={`topbar__tool-btn${highContrast ? ' topbar__tool-btn--active' : ''}`} onClick={onToggleContrast} aria-pressed={highContrast} title="Toggle high contrast">
      ◐ Contrast
    </button>
    <button type="button" className={`topbar__tool-btn${annotation ? ' topbar__tool-btn--active' : ''}`} onClick={onEditAnnotation} title="Add a shareable note">
      Note
    </button>
    <button type="button" className="topbar__tool-btn topbar__shortcut-btn" onClick={onShowShortcuts} title="Keyboard shortcuts">
      ?
    </button>
    <button
      className="topbar__info-btn"
      onClick={onShowDisclaimer}
      title="Data disclaimer"
      aria-label="Data disclaimer"
    >
      i
    </button>
    <StatsPanel
      mqttNodes={stats.mqttNodes}
      mapNodes={stats.mapNodes}
      totalDevices={stats.totalNodes}
      staleNodes={stats.staleNodes}
      packetsDay={stats.packetsDay}
    />
  </header>
);
