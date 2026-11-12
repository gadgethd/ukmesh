import { StatsSignal } from '../components/stats/StatsSignal.js';
import { StatsPaths } from '../components/stats/StatsPaths.js';
import { StatsObservers } from '../components/stats/StatsObservers.js';
import { StatsTraffic } from '../components/stats/StatsTraffic.js';
import { StatsOverview } from '../components/stats/StatsOverview.js';
import type { StatsTabId } from '../features/stats/model.js';
import React, { lazy, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { STATS_TABS, isStatsTabId } from '../components/stats/StatsPrimitives.js';
import type { DecodedPathSelection } from '../components/stats/StatsDecodedPathDialog.js';
import { LazyMapBoundary } from '../components/Map/LazyMapBoundary.js';
import { useStatsPageData } from '../hooks/useStatsPageData.js';
import { isStatsPayloadEmpty } from '../features/stats/state.js';
import './path-modal.css';
import './stats-page.css';

const StatsDecodedPathDialog = lazy(() => import('../components/stats/StatsDecodedPathDialog.js').then(module => ({ default: module.StatsDecodedPathDialog })));
export const StatsPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedDecodedPath, setSelectedDecodedPath] = useState<DecodedPathSelection | null>(null);
  const pathTrigger = useRef<HTMLElement | null>(null);
  const openPath = (selection: DecodedPathSelection) => {
    pathTrigger.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setSelectedDecodedPath(selection);
  };
  const closePath = () => { setSelectedDecodedPath(null); pathTrigger.current?.focus(); };
  const tabGroup = React.useId();
  const requestedTab = searchParams.get('tab');
  const requestedRegion = searchParams.get('region')?.trim().toUpperCase() ?? '';
  const activeTab: StatsTabId = isStatsTabId(requestedTab) ? requestedTab : 'overview';
  const { data, loading, refreshing, loadError, lastUpdatedAt, reload, liveObservers, observerError } = useStatsPageData(activeTab);
  useEffect(() => {
    if (!data) setSelectedDecodedPath(null);
  }, [data]);

  const setActiveTab = (tab: StatsTabId) => {
    const next = new URLSearchParams(searchParams);
    if (tab === 'overview') {
      next.delete('tab');
    } else {
      next.set('tab', tab);
    }
    setSearchParams(next, { replace: true });
  };
  const dataIsEmpty = data ? isStatsPayloadEmpty(data) : false;

  useEffect(() => {
    if (activeTab !== 'observers' || !requestedRegion || !data) return;
    const frame = window.requestAnimationFrame(() => {
      const card = document.getElementById(`observer-region-${requestedRegion}`);
      card?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      card?.focus({ preventScroll: true });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [activeTab, data, requestedRegion]);

  return (
    <div className="site-layout__inner">
      <h1 className="ui-visually-hidden">Network stats</h1>
      {/* ── Page hero ─────────────────────────────────────────────────────── */}

      <div className="site-content">

        {!loading && !data && (
          <div className="stats-page__request-state" role={loadError ? 'alert' : 'status'}>
            <strong>{loadError ? 'Stats are unavailable' : 'No stats response was returned'}</strong>
            <p>{loadError ?? 'Try loading the current network scope again.'}</p>
            <button type="button" onClick={() => { void reload(); }}>Retry</button>
          </div>
        )}

        {data && (
          <div className="stats-page__refresh-state" role={loadError ? 'alert' : 'status'}>
            <span>
              {loadError
                ? `${loadError}. Showing the last successful result.`
                : refreshing
                  ? 'Refreshing stats…'
                  : `Chart data as of ${lastUpdatedAt ? new Date(lastUpdatedAt).toLocaleString() : 'unknown'}`}
            </span>
            <button type="button" disabled={refreshing} onClick={() => { void reload(); }}>
              {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
        )}

        {data && dataIsEmpty && (
          <div className="stats-page__request-state" role="status">
            <strong>No stats are available for this scope</strong>
            <p>No packet, radio, or observer records were found in the current reporting window.</p>
            <button type="button" onClick={() => { void reload(); }}>Retry</button>
          </div>
        )}

          <div className="stats-page__tab-system">
            <div className="stats-page__tabs" role="tablist" aria-label="Stats sections"
              onKeyDown={(event) => {
                if (event.altKey || event.ctrlKey || event.metaKey) return;
                const tabs = [...event.currentTarget.querySelectorAll<HTMLButtonElement>('[role="tab"]')];
                const index = tabs.indexOf(document.activeElement as HTMLButtonElement);
                if (index < 0) return;
                const last = tabs.length - 1;
                const next = event.key === 'Home' ? 0 : event.key === 'End' ? last
                  : event.key === 'ArrowRight' ? (index + 1) % tabs.length
                    : event.key === 'ArrowLeft' ? (index + last) % tabs.length : -1;
                if (next < 0) return;
                event.preventDefault();
                tabs[next].focus();
              }}>
              {STATS_TABS.map((tab) => (
                <button type="button" role="tab" key={tab.id}
                  id={`${tabGroup}-tab-${tab.id}`} aria-controls={`${tabGroup}-panel-${tab.id}`}
                  aria-selected={activeTab === tab.id} tabIndex={activeTab === tab.id ? 0 : -1}
                  className={`stats-page__tab${activeTab === tab.id ? ' stats-page__tab--active' : ''}`}
                  onFocus={() => { if (activeTab !== tab.id) setActiveTab(tab.id); }}
                  onClick={() => { if (activeTab !== tab.id) setActiveTab(tab.id); }}>
                  {tab.label}
                </button>
              ))}
            </div>
            {STATS_TABS.filter(tab => tab.id !== activeTab).map(tab => (
              <div key={tab.id} role="tabpanel" id={`${tabGroup}-panel-${tab.id}`}
                aria-labelledby={`${tabGroup}-tab-${tab.id}`} hidden />
            ))}
            <div role="tabpanel" id={`${tabGroup}-panel-${activeTab}`}
              aria-labelledby={`${tabGroup}-tab-${activeTab}`} tabIndex={0} className="stats-page__tabpanel">
        {loading && (
          <div className="stats-page__loading-shell" aria-label="Loading stats">
            <LoadingIndicator label="Loading stats..." variant="block" />
            <div className="stats-page__summary">
              {Array.from({ length: 6 }, (_, index) => <div key={index} className="stats-page__stat skeleton-shimmer" />)}
            </div>
            <div className="stats-page__row">
              <div className="stats-page__chart skeleton-shimmer" />
              <div className="stats-page__chart skeleton-shimmer" />
            </div>
          </div>
        )}

            {data && !dataIsEmpty && (<>

            {activeTab === 'overview' && <StatsOverview data={data} />}

            {activeTab === 'traffic' && <StatsTraffic data={data} />}

            {activeTab === 'observers' && <StatsObservers data={data} liveObservers={liveObservers} observerError={observerError} />}

            {activeTab === 'paths' && <StatsPaths data={data} openPath={openPath} />}

            {activeTab === 'signal' && <StatsSignal data={data} />}
            </>)}
            </div>
          </div>

        {selectedDecodedPath && (
          <LazyMapBoundary key={selectedDecodedPath.hash ?? selectedDecodedPath.title}
            className="stats-page__path-status" label="Decoded path map" onClose={closePath}>
            <StatsDecodedPathDialog selection={selectedDecodedPath} onClose={closePath} />
          </LazyMapBoundary>
        )}
      </div>
    </div>
  );
};
