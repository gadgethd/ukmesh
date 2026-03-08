import React, { useCallback, useEffect, useRef, useState } from 'react';
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
import { getCurrentSite } from './config/site.js';
import { uncachedEndpoint, withScopeParams } from './utils/api.js';

const DEFAULT_FILTERS: Filters = {
  livePackets: true,
  coverage: false,
  clientNodes: false,
  packetPaths: false,
  betaPaths: false,
  betaPathThreshold: 0.45,
  links: false,
  hexClashes: false,
  hexClashMaxHops: 3,
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
      return { ...DEFAULT_FILTERS, ...parsed, betaPathThreshold: 0.45 };
    } catch {
      return DEFAULT_FILTERS;
    }
  });
  const [map, setMap] = useState<LeafletMap | null>(null);
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));
  const clashRestoreRef = useRef<{ links: boolean; coverage: boolean; clientNodes: boolean } | null>(null);
  const prevHexClashesRef = useRef<boolean>(DEFAULT_FILTERS.hexClashes);

  const {
    nodes,
    packets,
    arcs,
    activeNodes,
    handleInitialState,
    replaceRecentPackets,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
  } = useNodes();

  const networkFilter = site.networkFilter;
  const observerFilter = site.observerId;

  const { coverage, handleCoverageUpdate } = useCoverage({ network: networkFilter, observer: observerFilter });
  const stats = useDashboardStats({ network: networkFilter, observer: observerFilter });
  const {
    linkMetrics,
    viablePairsArr,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
  } = useLinkState();

  const {
    packetPaths,
    betaPacketPaths,
    betaLowConfidencePaths,
    betaLowConfidenceSegments,
    betaCompletionPaths,
    betaPathConfidence,
    betaPermutationCount,
    betaRemainingHops,
    pathOpacity,
    pinnedPacketId,
    handlePacketPin,
  } = usePacketPathOverlay({
    packets,
    nodes,
    filters,
    network: networkFilter,
    observer: observerFilter,
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
    let cancelled = false;
    const syncRecentPackets = async () => {
      try {
        const response = await fetch(
          uncachedEndpoint(withScopeParams('/api/packets/recent?limit=12', { network: networkFilter, observer: observerFilter })),
          { cache: 'no-store' },
        );
        if (!response.ok) return;
        const rows = await response.json() as Array<{
          time: string;
          packet_hash: string;
          rx_node_id?: string;
          observer_node_ids?: string[] | null;
          src_node_id?: string;
          packet_type?: number;
          hop_count?: number;
          summary?: string | null;
          payload?: Record<string, unknown>;
          advert_count?: number | null;
          path_hashes?: string[] | null;
        }>;
        if (cancelled) return;
        replaceRecentPackets(rows);
      } catch {
        // keep websocket as primary path; polling is only a feed repair/fallback
      }
    };

    void syncRecentPackets();
    const timer = window.setInterval(() => { void syncRecentPackets(); }, 4000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [networkFilter, observerFilter, replaceRecentPackets]);

  useEffect(() => {
    const wasHexClashes = prevHexClashesRef.current;
    const isHexClashes = filters.hexClashes;

    if (!wasHexClashes && isHexClashes) {
      clashRestoreRef.current = {
        links: filters.links,
        coverage: filters.coverage,
        clientNodes: filters.clientNodes,
      };
      setFilters((current) => ({
        ...current,
        links: false,
        coverage: false,
        clientNodes: false,
      }));
    } else if (wasHexClashes && !isHexClashes && clashRestoreRef.current) {
      const restore = clashRestoreRef.current;
      clashRestoreRef.current = null;
      setFilters((current) => ({
        ...current,
        links: restore.links,
        coverage: restore.coverage,
        clientNodes: restore.clientNodes,
      }));
    }

    prevHexClashesRef.current = isHexClashes;
  }, [filters.hexClashes, filters.links, filters.coverage, filters.clientNodes]);

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
    onPacketObserved: () => {
      window.dispatchEvent(new Event('meshcore:packet-observed'));
    },
  });

  const wsState = useWebSocket(handleMessage, { network: networkFilter, observer: observerFilter });

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
        showHexClashes={filters.hexClashes}
        maxHexClashHops={filters.hexClashMaxHops}
        viablePairsArr={viablePairsArr}
        linkMetrics={linkMetrics}
        packetPaths={packetPaths}
        betaPaths={betaPacketPaths}
        betaLowPaths={betaLowConfidencePaths}
        betaLowSegments={betaLowConfidenceSegments}
        betaCompletionPaths={betaCompletionPaths}
        showBetaPaths={filters.betaPaths || pinnedPacketId !== null}
        pathOpacity={pathOpacity}
        onMapReady={setMap}
      />

      <FilterPanel
        filters={filters}
        onChange={setFilters}
        betaPathConfidence={betaPathConfidence}
        betaPermutationCount={betaPermutationCount}
        betaRemainingHops={betaRemainingHops}
      />

      {filters.livePackets && (
      <PacketFeed
        packets={packets}
        nodes={nodes}
        mqttObserverCount={stats.mqttNodes}
        onPacketClick={handlePacketPin}
        pinnedPacketId={pinnedPacketId}
      />
      )}

      {showDisclaimer && <DisclaimerModal onClose={dismissDisclaimer} />}
    </div>
  );
};
