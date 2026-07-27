import React, { useCallback, useEffect, useRef, useState } from 'react';
import './styles/map-app.css';
import type maplibregl from 'maplibre-gl';
import { MapLibreMap } from './components/Map/MapLibreMap.js';
import { LiveOverlayController } from './components/Map/LiveOverlayController.js';
import { FilterPanel, type Filters } from './components/FilterPanel/FilterPanel.js';
import { PacketFeed } from './components/PacketFeed.js';
import { DisclaimerModal } from './components/app/DisclaimerModal.js';
import { AppTopBar } from './components/app/AppTopBar.js';
import { MobileControls } from './components/app/MobileControls.js';
import { LoadingIndicator } from './components/LoadingIndicator.js';
import { useWebSocket } from './hooks/useWebSocket.js';
import { nodeStore, type MeshNode } from './hooks/useNodes.js';
import { coverageStore } from './hooks/useCoverage.js';
import { useDashboardStats, type DashboardStats } from './hooks/useDashboardStats.js';
import { linkStateStore } from './hooks/useLinkState.js';
import { useAppMessageHandler } from './hooks/useAppMessageHandler.js';
import { VIEWSHED_ENABLED } from './config/features.js';
import { getCurrentSite } from './config/site.js';
import { filtersForMapMode, isMapMode, type MapMode } from './config/mapModes.js';
import { useOverlayStore } from './store/overlayStore.js';
import { uncachedEndpoint, withScopeParams } from './utils/api.js';
import {
  filtersFromUrl,
  initialMapViewFromUrl,
  writeFiltersToUrl,
  writeMapViewToUrl,
} from './utils/mapUrlState.js';

type PacketHistorySegment = {
  positions: [[number, number], [number, number]];
  count: number;
};

const DEFAULT_FILTERS: Filters = {
  livePackets: true,
  links: false,
  terrain: false,
  clientNodes: false,
  packetHistory: false,
  betaPaths: false,
  betaPathThreshold: 0.45,
  hexClashes: false,
  hexClashMaxHops: 3,
};

const DISCLAIMER_KEY = 'meshcore-disclaimer-dismissed';
const FILTERS_KEY = 'meshcore-app-filters-v3';
const ignoreCoverageUpdate = () => {};
const TimelineControl = React.lazy(() => import('./components/app/TimelineControl.js').then((module) => ({ default: module.TimelineControl })));
const PlannerComparison = React.lazy(() => import('./components/app/PlannerComparison.js').then((module) => ({ default: module.PlannerComparison })));

export const App: React.FC = () => {
  const site = getCurrentSite();
  const [filters, setFilters] = useState<Filters>(() => {
    let stored = DEFAULT_FILTERS;
    try {
      const raw = localStorage.getItem(FILTERS_KEY);
      if (raw) stored = { ...DEFAULT_FILTERS, ...JSON.parse(raw) as Partial<Filters>, betaPathThreshold: 0.45 };
    } catch {
      stored = DEFAULT_FILTERS;
    }
    const requestedMode = new URLSearchParams(window.location.search).get('mode');
    const modeFilters = isMapMode(requestedMode) && (requestedMode !== 'plan' || VIEWSHED_ENABLED)
      ? filtersForMapMode(requestedMode, stored)
      : stored;
    return filtersFromUrl(modeFilters);
  });
  const [activeMode, setActiveMode] = useState<MapMode | null>(() => {
    const requested = new URLSearchParams(window.location.search).get('mode');
    return isMapMode(requested) && (requested !== 'plan' || VIEWSHED_ENABLED) ? requested : null;
  });
  const [initialMapView] = useState(() => initialMapViewFromUrl());
  const [shareLabel, setShareLabel] = useState('Copy view link');
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(() => new URLSearchParams(window.location.search).get('node'));
  const [filtersCollapsed, setFiltersCollapsed] = useState<boolean>(() => !!new URLSearchParams(window.location.search).get('node'));
  // MapLibre map instance — used by MobileControls/NodeSearch for flyTo
  const [mlMap, setMlMap] = useState<maplibregl.Map | null>(null);
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));
  const [inferredNodes, setInferredNodes] = useState<MeshNode[]>([]);
  const [inferredActiveNodeIds, setInferredActiveNodeIds] = useState<Set<string>>(new Set());
  const [packetHistorySegments, setPacketHistorySegments] = useState<PacketHistorySegment[]>([]);
  const [fetchedStats, setFetchedStats] = useState<DashboardStats | null>(null);
  const [initialStateLoaded, setInitialStateLoaded] = useState(false);
  const [initialPollLoaded, setInitialPollLoaded] = useState(false);
  const [pollRefreshing, setPollRefreshing] = useState(false);
  const [isPageVisible, setIsPageVisible] = useState(
    () => (typeof document === 'undefined' ? true : document.visibilityState === 'visible'),
  );
  const clashRestoreRef = useRef<{ clientNodes: boolean } | null>(null);
  const prevHexClashesRef = useRef<boolean>(DEFAULT_FILTERS.hexClashes);

  const networkFilter = site.networkFilter;
  const observerFilter = site.observerId;

  const stats = useDashboardStats(fetchedStats);

  useEffect(() => {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }
  }, []);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const updateVisibility = () => setIsPageVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', updateVisibility);
    return () => document.removeEventListener('visibilitychange', updateVisibility);
  }, []);

  useEffect(() => {
    localStorage.setItem(FILTERS_KEY, JSON.stringify(filters));
    writeFiltersToUrl(filters, activeMode);
  }, [activeMode, filters]);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (selectedNodeId) url.searchParams.set('node', selectedNodeId);
    else url.searchParams.delete('node');
    window.history.replaceState(null, '', url);
  }, [selectedNodeId]);

  // Selecting a node collapses the layers panel so the docked detail panel has
  // room on the right; clearing the selection restores it.
  useEffect(() => {
    setFiltersCollapsed(selectedNodeId != null);
  }, [selectedNodeId]);

  useEffect(() => {
    if (!mlMap) return undefined;
    const syncMapUrl = () => {
      const center = mlMap.getCenter();
      writeMapViewToUrl({ lat: center.lat, lon: center.lng, zoom: mlMap.getZoom() });
    };
    mlMap.on('moveend', syncMapUrl);
    return () => { mlMap.off('moveend', syncMapUrl); };
  }, [mlMap]);

  const handleFiltersChange = useCallback((next: Filters) => {
    setActiveMode(null);
    useOverlayStore.getState().setPlanRepeaterMode(false);
    setFilters(next);
  }, []);

  const handleModeChange = useCallback((mode: MapMode) => {
    setActiveMode(mode);
    setFilters((current) => filtersForMapMode(mode, current));
    useOverlayStore.getState().setPlanRepeaterMode(mode === 'plan');
  }, []);

  const handleShare = useCallback(async () => {
    if (mlMap) {
      const center = mlMap.getCenter();
      writeMapViewToUrl({ lat: center.lat, lon: center.lng, zoom: mlMap.getZoom() });
    }
    try {
      await navigator.clipboard.writeText(window.location.href);
      setShareLabel('Link copied');
    } catch {
      setShareLabel('Copy failed');
    }
    window.setTimeout(() => setShareLabel('Copy view link'), 1800);
  }, [mlMap]);

  // Keep the fast poll to live data that changes independently of the socket.
  // Expensive inferred/path overlays use their own, slower conditional polls.
  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;
    let controller: AbortController | null = null;

    const scheduleNext = () => {
      if (cancelled || !isPageVisible) return;
      timer = window.setTimeout(() => { void syncLiveData(); }, 10_000);
    };

    const syncLiveData = async () => {
      if (cancelled || !isPageVisible) return;
      controller = new AbortController();
      setPollRefreshing(true);

      try {
        const [packetsRes, statsRes] = await Promise.allSettled([
          fetch(uncachedEndpoint(withScopeParams('/api/packets/recent?limit=12', { network: networkFilter, observer: observerFilter })), {
            cache: 'no-store', signal: controller.signal,
          }),
          fetch(uncachedEndpoint(withScopeParams('/api/stats', { network: networkFilter, observer: observerFilter })), {
            cache: 'no-store', signal: controller.signal,
          }),
        ]);

        if (cancelled) return;

        if (packetsRes.status === 'fulfilled' && packetsRes.value.ok) {
          const rows = await packetsRes.value.json() as Array<{
            time: string; packet_hash: string; rx_node_id?: string;
            observer_node_ids?: string[] | null; src_node_id?: string;
            packet_type?: number; hop_count?: number; summary?: string | null;
            payload?: Record<string, unknown>; advert_count?: number | null;
            path_hashes?: string[] | null;
          }>;
          if (!cancelled) nodeStore.replaceRecentPackets(rows);
        }

        if (statsRes.status === 'fulfilled' && statsRes.value.ok) {
          const payload = await statsRes.value.json() as DashboardStats;
          if (!cancelled) setFetchedStats(payload);
        }
      } finally {
        if (!cancelled) {
          setInitialPollLoaded(true);
          setPollRefreshing(false);
        }
        controller = null;
        scheduleNext();
      }
    };

    void syncLiveData();

    return () => {
      cancelled = true;
      controller?.abort();
      if (timer) window.clearTimeout(timer);
    };
  }, [isPageVisible, networkFilter, observerFilter]);

  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;
    let controller: AbortController | null = null;

    const scheduleNext = () => {
      if (cancelled || !isPageVisible) return;
      timer = window.setTimeout(() => { void syncInferredNodes(); }, 60_000);
    };

    const syncInferredNodes = async () => {
      if (cancelled || !isPageVisible) return;
      controller = new AbortController();
      try {
        const response = await fetch(
          uncachedEndpoint(withScopeParams('/api/inferred-nodes', { network: networkFilter, observer: observerFilter })),
          { cache: 'no-store', signal: controller.signal },
        );
        if (!response.ok || cancelled) return;
        const payload = await response.json() as {
          inferredNodes: MeshNode[]; inferredActiveNodeIds: string[];
        };
        if (!cancelled) {
          setInferredNodes(payload.inferredNodes ?? []);
          setInferredActiveNodeIds(new Set((payload.inferredActiveNodeIds ?? []).map((value) => value.toLowerCase())));
        }
      } catch (err) {
        if (!cancelled && (err as DOMException).name !== 'AbortError') {
          console.warn('[app] inferred nodes refresh failed');
        }
      } finally {
        controller = null;
        scheduleNext();
      }
    };

    void syncInferredNodes();
    return () => {
      cancelled = true;
      controller?.abort();
      if (timer) window.clearTimeout(timer);
    };
  }, [isPageVisible, networkFilter, observerFilter]);

  useEffect(() => {
    if (!filters.packetHistory) {
      setPacketHistorySegments([]);
      return undefined;
    }

    let cancelled = false;
    let timer: number | null = null;
    let controller: AbortController | null = null;

    const scheduleNext = () => {
      if (cancelled || !isPageVisible) return;
      timer = window.setTimeout(() => { void syncPacketHistory(); }, 60_000);
    };

    const syncPacketHistory = async () => {
      if (cancelled || !isPageVisible) return;
      controller = new AbortController();
      try {
        const response = await fetch(
          uncachedEndpoint(withScopeParams('/api/path-beta/multibyte-paths', { network: networkFilter, observer: observerFilter })),
          { cache: 'no-store', signal: controller.signal },
        );
        if (!response.ok || cancelled) return;
        const payload = await response.json() as { segments?: PacketHistorySegment[] };
        if (!cancelled) setPacketHistorySegments(Array.isArray(payload.segments) ? payload.segments : []);
      } catch (err) {
        if (!cancelled && (err as DOMException).name !== 'AbortError') {
          console.warn('[app] path history refresh failed');
        }
      } finally {
        controller = null;
        scheduleNext();
      }
    };

    void syncPacketHistory();
    return () => {
      cancelled = true;
      controller?.abort();
      if (timer) window.clearTimeout(timer);
    };
  }, [filters.packetHistory, isPageVisible, networkFilter, observerFilter]);

  useEffect(() => {
    const wasHexClashes = prevHexClashesRef.current;
    const isHexClashes = filters.hexClashes;

    if (!wasHexClashes && isHexClashes) {
      clashRestoreRef.current = { clientNodes: filters.clientNodes };
      setFilters((current) => ({ ...current, clientNodes: false }));
    } else if (wasHexClashes && !isHexClashes && clashRestoreRef.current) {
      const restore = clashRestoreRef.current;
      clashRestoreRef.current = null;
      setFilters((current) => ({ ...current, clientNodes: restore.clientNodes }));
    }

    prevHexClashesRef.current = isHexClashes;
  }, [filters.hexClashes, filters.clientNodes]);

  useEffect(() => {
    const postError = (kind: string, message: string, stack?: string) => {
      void fetch('/api/telemetry/frontend-error', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ kind, message, stack, page: window.location.href, userAgent: navigator.userAgent }),
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

  const handleInitialState = useCallback((data: Parameters<typeof nodeStore.handleInitialState>[0]) => {
    nodeStore.handleInitialState(data);
    setInitialStateLoaded(true);
  }, []);

  const handleMessage = useAppMessageHandler({
    handleInitialState,
    handlePacket: nodeStore.handlePacket,
    handleNodeUpdate: nodeStore.handleNodeUpdate,
    handleNodeUpdateBatch: nodeStore.handleNodeUpdateBatch,
    handleNodeUpsert: nodeStore.handleNodeUpsert,
    handleNodeUpsertBatch: nodeStore.handleNodeUpsertBatch,
    handleCoverageUpdate: VIEWSHED_ENABLED ? coverageStore.handleCoverageUpdate : ignoreCoverageUpdate,
    handleCoverageUpdateBatch: VIEWSHED_ENABLED ? coverageStore.handleCoverageUpdateBatch : ignoreCoverageUpdate,
    applyInitialViablePairs: linkStateStore.applyInitialViablePairs,
    applyInitialViableLinks: linkStateStore.applyInitialViableLinks,
    applyLinkUpdate: linkStateStore.applyLinkUpdate,
    applyLinkUpdateBatch: linkStateStore.applyLinkUpdateBatch,
    onPacketObserved: () => {
      window.dispatchEvent(new Event('meshcore:packet-observed'));
    },
  });

  const wsState = useWebSocket(handleMessage, { network: networkFilter, observer: observerFilter });

  return (
    <div className="app-shell" data-node-open={selectedNodeId ? 'true' : undefined} data-live-feed={filters.livePackets ? 'true' : undefined}>
      <AppTopBar
        homeUrl={site.appHomeUrl}
        wsState={wsState}
        onShowDisclaimer={() => setShowDisclaimer(true)}
        stats={stats}
      />

      <MobileControls
        map={mlMap}
        filters={filters}
        onFiltersChange={handleFiltersChange}
        activeMode={activeMode}
        viewshedEnabled={VIEWSHED_ENABLED}
        onModeChange={handleModeChange}
        onShare={handleShare}
        shareLabel={shareLabel}
        onNodeSelect={setSelectedNodeId}
      />

      <div className="map-layer">
        <MapLibreMap
          inferredNodes={inferredNodes}
          inferredActiveNodeIds={inferredActiveNodeIds}
          showLinks={filters.links}
          showTerrain={filters.terrain}
          showClientNodes={filters.clientNodes}
          showHexClashes={filters.hexClashes}
          maxHexClashHops={filters.hexClashMaxHops}
          viewshedEnabled={VIEWSHED_ENABLED}
          initialView={initialMapView}
          selectedNodeId={selectedNodeId}
          onNodeSelect={setSelectedNodeId}
          onMapReady={setMlMap}
        />
        <LiveOverlayController
          map={mlMap}
          filters={filters}
          network={networkFilter}
          observer={observerFilter}
          packetHistorySegments={packetHistorySegments}
        />
        {(!initialStateLoaded || !initialPollLoaded) && (
          <LoadingIndicator
            label={!initialStateLoaded ? 'Loading network nodes...' : 'Loading dashboard data...'}
            variant="overlay"
          />
        )}
        {initialPollLoaded && pollRefreshing && (
          <div className="app-refresh-status">
            <LoadingIndicator label="Refreshing map data..." variant="inline" />
          </div>
        )}
      </div>

      <FilterPanel
        filters={filters}
        onChange={handleFiltersChange}
        activeMode={activeMode}
        viewshedEnabled={VIEWSHED_ENABLED}
        onModeChange={handleModeChange}
        onShare={handleShare}
        shareLabel={shareLabel}
        collapsed={filtersCollapsed}
        onToggleCollapse={() => setFiltersCollapsed((value) => !value)}
        nodeOpen={selectedNodeId != null}
      />

      <React.Suspense fallback={null}>
        <TimelineControl network={networkFilter} observer={observerFilter} />
        {VIEWSHED_ENABLED && <PlannerComparison enabled />}
      </React.Suspense>

      {filters.livePackets && <PacketFeed />}

      {showDisclaimer && <DisclaimerModal viewshedEnabled={VIEWSHED_ENABLED} onClose={dismissDisclaimer} />}
    </div>
  );
};
