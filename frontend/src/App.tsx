import React, { useCallback, useEffect, useRef, useState } from 'react';
import type maplibregl from 'maplibre-gl';
import { MapLibreMap } from './components/Map/MapLibreMap.js';
import { LiveOverlayController } from './components/Map/LiveOverlayController.js';
import { FilterPanel, type Filters } from './components/FilterPanel/FilterPanel.js';
import { PacketFeed } from './components/PacketFeed.js';
import { DisclaimerModal } from './components/app/DisclaimerModal.js';
import { AppTopBar } from './components/app/AppTopBar.js';
import { MobileControls } from './components/app/MobileControls.js';
import { useWebSocket } from './hooks/useWebSocket.js';
import { nodeStore, type MeshNode } from './hooks/useNodes.js';
import { coverageStore } from './hooks/useCoverage.js';
import { useDashboardStats, type DashboardStats } from './hooks/useDashboardStats.js';
import { linkStateStore } from './hooks/useLinkState.js';
import { useAppMessageHandler } from './hooks/useAppMessageHandler.js';
import { getCurrentSite } from './config/site.js';
import { uncachedEndpoint, withScopeParams } from './utils/api.js';

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
  // MapLibre map instance — used by MobileControls/NodeSearch for flyTo
  const [mlMap, setMlMap] = useState<maplibregl.Map | null>(null);
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));
  const [inferredNodes, setInferredNodes] = useState<MeshNode[]>([]);
  const [inferredActiveNodeIds, setInferredActiveNodeIds] = useState<Set<string>>(new Set());
  const [packetHistorySegments, setPacketHistorySegments] = useState<PacketHistorySegment[]>([]);
  const [fetchedStats, setFetchedStats] = useState<DashboardStats | null>(null);
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
  }, [filters]);

  // Consolidated polling
  useEffect(() => {
    let cancelled = false;

    const syncAllData = async () => {
      if (!isPageVisible) return;

      const [packetsRes, historyRes, inferredRes, statsRes] = await Promise.allSettled([
        fetch(uncachedEndpoint(withScopeParams('/api/packets/recent?limit=12', { network: networkFilter, observer: observerFilter })), { cache: 'no-store' }),
        fetch(uncachedEndpoint(withScopeParams('/api/path-beta/history', { network: networkFilter })), { cache: 'no-store' }),
        fetch(uncachedEndpoint(withScopeParams('/api/inferred-nodes', { network: networkFilter, observer: observerFilter })), { cache: 'no-store' }),
        fetch(uncachedEndpoint(withScopeParams('/api/stats', { network: networkFilter, observer: observerFilter })), { cache: 'no-store' }),
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

      if (historyRes.status === 'fulfilled' && historyRes.value.ok) {
          const payload = await historyRes.value.json() as { segments?: PacketHistorySegment[] };
        if (!cancelled) setPacketHistorySegments(Array.isArray(payload.segments) ? payload.segments : []);
      }

      if (inferredRes.status === 'fulfilled' && inferredRes.value.ok) {
        const payload = await inferredRes.value.json() as {
          inferredNodes: MeshNode[]; inferredActiveNodeIds: string[];
        };
        if (!cancelled) {
          setInferredNodes(payload.inferredNodes ?? []);
          setInferredActiveNodeIds(new Set((payload.inferredActiveNodeIds ?? []).map((v) => v.toLowerCase())));
        }
      }

      if (statsRes.status === 'fulfilled' && statsRes.value.ok) {
        const payload = await statsRes.value.json() as DashboardStats;
        if (!cancelled) setFetchedStats(payload);
      }
    };

      void syncAllData();

      const pollMs = isPageVisible ? 10000 : 60000;
      const timer = window.setInterval(() => { void syncAllData(); }, pollMs);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [isPageVisible, networkFilter, observerFilter]);

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

  const handleMessage = useAppMessageHandler({
    handleInitialState: nodeStore.handleInitialState,
    handlePacket: nodeStore.handlePacket,
    handleNodeUpdate: nodeStore.handleNodeUpdate,
    handleNodeUpdateBatch: nodeStore.handleNodeUpdateBatch,
    handleNodeUpsert: nodeStore.handleNodeUpsert,
    handleNodeUpsertBatch: nodeStore.handleNodeUpsertBatch,
    handleCoverageUpdate: coverageStore.handleCoverageUpdate,
    handleCoverageUpdateBatch: coverageStore.handleCoverageUpdateBatch,
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
    <div className="app-shell">
      <AppTopBar
        homeUrl={site.appHomeUrl}
        wsState={wsState}
        onShowDisclaimer={() => setShowDisclaimer(true)}
        stats={stats}
      />

      <MobileControls
        map={mlMap}
        filters={filters}
        onFiltersChange={setFilters}
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
          onMapReady={setMlMap}
        />
        <LiveOverlayController
          map={mlMap}
          filters={filters}
          network={networkFilter}
          observer={observerFilter}
          packetHistorySegments={packetHistorySegments}
        />
      </div>

      <FilterPanel
        filters={filters}
        onChange={setFilters}
      />

      {filters.livePackets && <PacketFeed />}

      {showDisclaimer && <DisclaimerModal onClose={dismissDisclaimer} />}
    </div>
  );
};
