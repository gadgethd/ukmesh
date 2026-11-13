import React from 'react';
import { StatsPanel } from '../StatsPanel/StatsPanel.js';
import type { WSConnectionState } from '../../features/realtime/protocol.js';
import { ConnectionStatus } from './ConnectionStatus.js';
import type { DashboardStats } from '../../hooks/useDashboardStats.js';

type AppTopBarProps = {
  homeUrl: string;
  connection: WSConnectionState;
  hasData: boolean;
  refreshedAt: number | null;
  refreshing: boolean;
  refreshFailed: boolean;
  onShowDisclaimer: () => void;
  stats: DashboardStats;
  mapLight: boolean;
  onToggleMapTheme: () => void;
  annotation: string;
  onEditAnnotation: () => void;
  onShowShortcuts: () => void;
  highContrast: boolean;
  onToggleContrast: () => void;
  regionSelector?: React.ReactNode;
};

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
  connection, hasData, refreshedAt, refreshing, refreshFailed,
  onShowDisclaimer,
  stats,
  mapLight,
  onToggleMapTheme,
  annotation,
  onEditAnnotation,
  onShowShortcuts,
  highContrast,
  onToggleContrast,
  regionSelector,
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
    <ConnectionStatus connection={connection} hasData={hasData} refreshedAt={refreshedAt} refreshing={refreshing} failed={refreshFailed} />
    {regionSelector}
    <button
      type="button"
      className={'topbar__tool-btn' + (mapLight ? ' topbar__tool-btn--active' : '')}
      onClick={onToggleMapTheme}
      aria-pressed={mapLight}
      title="Toggle map theme"
    >
      {mapLight ? '☀ Light' : '☾ Dark'}
    </button>
    <button type="button" className={`topbar__tool-btn${highContrast ? ' topbar__tool-btn--active' : ''}`} onClick={onToggleContrast} aria-pressed={highContrast} title="Toggle high contrast">
      ◐ Contrast
    </button>
    <button type="button" className={`topbar__tool-btn${annotation ? ' topbar__tool-btn--active' : ''}`} onClick={onEditAnnotation} aria-pressed={Boolean(annotation)} title="Add a shareable note">
      Note
    </button>
    <button type="button" className="topbar__tool-btn topbar__shortcut-btn" onClick={onShowShortcuts} title="Keyboard shortcuts">
      ?
    </button>
    <button
      type="button"
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
