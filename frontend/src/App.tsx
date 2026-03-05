import React, { useCallback, useEffect, useState } from 'react';
import type { Map as LeafletMap } from 'leaflet';
import { MapView } from './components/Map/MapView.js';
import { FilterPanel, type Filters } from './components/FilterPanel/FilterPanel.js';
import { PacketFeed } from './components/PacketFeed.js';
import { DisclaimerModal } from './components/app/DisclaimerModal.js';
import { AppTopBar } from './components/app/AppTopBar.js';
import { MobileControls } from './components/app/MobileControls.js';
import { useWebSocket } from './hooks/useWebSocket.js';
import { useNodes } from './hooks/useNodes.js';
import { useCoverage } from './hooks/useCoverage.js';
import { useDashboardStats } from './hooks/useDashboardStats.js';
import { useLinkState } from './hooks/useLinkState.js';
import { usePacketPathOverlay } from './hooks/usePacketPathOverlay.js';
import { useAppMessageHandler } from './hooks/useAppMessageHandler.js';
import { usePathLearningModel } from './hooks/usePathLearningModel.js';
import { getCurrentSite } from './config/site.js';

const DEFAULT_FILTERS: Filters = {
  livePackets: true,
  coverage: false,
  clientNodes: false,
  packetPaths: false,
  betaPaths: false,
  betaPathThreshold: 0.5,
  links: false,
};

const DISCLAIMER_KEY = 'meshcore-disclaimer-dismissed';
const FILTERS_KEY = 'meshcore-app-filters-v2';

export const App: React.FC = () => {
  const site = getCurrentSite();
  const [filters, setFilters] = useState<Filters>(() => {
    try {
      const raw = localStorage.getItem(FILTERS_KEY);
      if (!raw) return DEFAULT_FILTERS;
      const parsed = JSON.parse(raw) as Partial<Filters>;
      return { ...DEFAULT_FILTERS, ...parsed };
    } catch {
      return DEFAULT_FILTERS;
    }
  });
  const [map, setMap] = useState<LeafletMap | null>(null);
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));

  const {
    nodes,
    packets,
    arcs,
    activeNodes,
    handleInitialState,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
  } = useNodes();

  // 'ukmesh' build sees all data; teesside/default build filters to its own network
  const networkFilter = import.meta.env['VITE_NETWORK'] === 'ukmesh' ? undefined : site.network;

  const { coverage, handleCoverageUpdate } = useCoverage(networkFilter);
  const stats = useDashboardStats(networkFilter);
  const learningModelNetwork = import.meta.env['VITE_NETWORK'] === 'ukmesh' ? 'all' : site.network;
  const learningModel = usePathLearningModel(learningModelNetwork);

  useEffect(() => {
    if (!learningModel) return;
    setFilters((current) => {
      const tuned = Math.min(0.9, Math.max(0.25, learningModel.recommendedThreshold));
      if (Math.abs(current.betaPathThreshold - tuned) < 0.01) return current;
      return { ...current, betaPathThreshold: tuned };
    });
  }, [learningModel]);

  const {
    linkPairs,
    linkMetrics,
    viablePairsArr,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
  } = useLinkState();

  const {
    packetPath,
    betaPacketPath,
    betaPathConfidence,
    pathOpacity,
    pinnedPacketId,
    handlePacketPin,
  } = usePacketPathOverlay({
    packets,
    nodes,
    coverage,
    linkPairs,
    linkMetrics,
    learningModel,
    filters,
  });

  useEffect(() => {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }
  }, []);

  useEffect(() => {
    localStorage.setItem(FILTERS_KEY, JSON.stringify(filters));
  }, [filters]);

  useEffect(() => {
    const postError = (kind: string, message: string, stack?: string) => {
      void fetch('/api/telemetry/frontend-error', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          kind,
          message,
          stack,
          page: window.location.href,
          userAgent: navigator.userAgent,
        }),
      }).catch(() => {});
    };

    const onError = (event: ErrorEvent) => {
      postError('error', event.message ?? 'unknown error', event.error?.stack);
    };
    const onRejection = (event: PromiseRejectionEvent) => {
      const reason = event.reason instanceof Error ? event.reason : new Error(String(event.reason));
      postError('unhandledrejection', reason.message, reason.stack);
    };

    window.addEventListener('error', onError);
    window.addEventListener('unhandledrejection', onRejection);
    return () => {
      window.removeEventListener('error', onError);
      window.removeEventListener('unhandledrejection', onRejection);
    };
  }, []);

  const dismissDisclaimer = useCallback(() => {
    localStorage.setItem(DISCLAIMER_KEY, '1');
    setShowDisclaimer(false);
  }, []);

  const handleMessage = useAppMessageHandler({
    handleInitialState,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
    handleCoverageUpdate,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
  });

  const wsState = useWebSocket(handleMessage, networkFilter);

  return (
    <div className="app-shell">
      <AppTopBar
        homeUrl={site.appHomeUrl}
        wsState={wsState}
        onShowDisclaimer={() => setShowDisclaimer(true)}
        stats={stats}
      />

      <MobileControls
        map={map}
        nodes={nodes}
        filters={filters}
        onFiltersChange={setFilters}
      />

      <MapView
        nodes={nodes}
        arcs={arcs}
        activeNodes={activeNodes}
        coverage={coverage}
        showPackets={filters.livePackets}
        showCoverage={filters.coverage}
        showClientNodes={filters.clientNodes}
        showLinks={filters.links}
        viablePairsArr={viablePairsArr}
        linkMetrics={linkMetrics}
        packetPath={packetPath}
        betaPath={betaPacketPath}
        showBetaPaths={filters.betaPaths || pinnedPacketId !== null}
        pathOpacity={pathOpacity}
        onMapReady={setMap}
      />

      <FilterPanel filters={filters} onChange={setFilters} betaPathConfidence={betaPathConfidence} />

      {filters.livePackets && (
        <PacketFeed
          packets={packets}
          nodes={nodes}
          onPacketClick={handlePacketPin}
          pinnedPacketId={pinnedPacketId}
        />
      )}

      {showDisclaimer && <DisclaimerModal onClose={dismissDisclaimer} />}
    </div>
  );
};
