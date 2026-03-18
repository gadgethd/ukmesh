import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Map as LeafletMap } from 'leaflet';
import { MapView } from './components/Map/MapView.js';
import type { DeckViewState, GPUNodeData } from './components/Map/DeckGLOverlay.js';
import { DeckGLOverlay } from './components/Map/DeckGLOverlay.js';
import { FilterPanel, type Filters } from './components/FilterPanel/FilterPanel.js';
import { PacketFeed } from './components/PacketFeed.js';
import { DisclaimerModal } from './components/app/DisclaimerModal.js';
import { AppTopBar } from './components/app/AppTopBar.js';
import { MobileControls } from './components/app/MobileControls.js';
import { useWebSocket } from './hooks/useWebSocket.js';
import { useNodes, type MeshNode } from './hooks/useNodes.js';
import { useCoverage } from './hooks/useCoverage.js';
import { useDashboardStats, type DashboardStats } from './hooks/useDashboardStats.js';
import { useLinkState } from './hooks/useLinkState.js';
import { usePacketPathOverlay } from './hooks/usePacketPathOverlay.js';
import { useAppMessageHandler } from './hooks/useAppMessageHandler.js';
import { getCurrentSite } from './config/site.js';
import { uncachedEndpoint, withScopeParams } from './utils/api.js';
import { buildHiddenCoordMask, resolvePathNodeIds, hasCoords, maskNodePoint } from './utils/pathing.js';

type PacketHistorySegment = {
  positions: [[number, number], [number, number]];
  count: number;
};

const DEFAULT_FILTERS: Filters = {
  livePackets: true,
  coverage: false,
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
  const [map, setMap] = useState<LeafletMap | null>(null);
  const [deckViewState, setDeckViewState] = useState<DeckViewState>({ longitude: -1.23, latitude: 54.57, zoom: 10, pitch: 0, bearing: 0 });
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));
  const [inferredNodes, setInferredNodes] = useState<MeshNode[]>([]);
  const [inferredActiveNodeIds, setInferredActiveNodeIds] = useState<Set<string>>(new Set());
  const [packetHistorySegments, setPacketHistorySegments] = useState<PacketHistorySegment[]>([]);
  const [fetchedStats, setFetchedStats] = useState<DashboardStats | null>(null);
  const [isPageVisible, setIsPageVisible] = useState(
    () => (typeof document === 'undefined' ? true : document.visibilityState === 'visible'),
  );
  const [prefixFocusActive, setPrefixFocusActive] = useState(false);
  const clashRestoreRef = useRef<{ coverage: boolean; clientNodes: boolean } | null>(null);
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
    handleNodeUpdateBatch,
    handleNodeUpsert,
    handleNodeUpsertBatch,
  } = useNodes();

  const networkFilter = site.networkFilter;
  const observerFilter = site.observerId;

  // Coordinate privacy mask — computed once here and shared with MapView (for node markers /
  // clash lines) and DeckGLOverlay (for GPU-rendered path/history layers).
  const hiddenCoordMask = useMemo(() => buildHiddenCoordMask(nodes.values()), [nodes]);

  const { coverage, handleCoverageUpdate, handleCoverageUpdateBatch } = useCoverage({ network: networkFilter, observer: observerFilter }, filters.coverage);
  const stats = useDashboardStats(fetchedStats);
  const {
    linkMetrics,
    viablePairsArr,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
    applyLinkUpdateBatch,
  } = useLinkState();

  const {
    betaPacketPaths,
    betaLowConfidenceSegments,
    betaCompletionPaths,
    betaPathConfidence,
    betaPermutationCount,
    betaRemainingHops,
    pathFadingOut,
    pinnedPacketId,
    pinnedPacketSnapshot,
    handlePacketPin,
  } = usePacketPathOverlay({
    packets,
    nodes,
    filters,
    network: networkFilter,
    observer: observerFilter,
  });

  // Compute the set of node IDs involved in the currently displayed path.
  // Active when: a packet is pinned, OR the live-path toggle is on (auto-tracks packets[0]).
  // Passed to MapView so it can hide unrelated repeaters.
  //
  // Uses a ref-based stability guard: if the computed Set has identical contents to the
  // previous result, the same reference is returned. This prevents MapView from re-rendering
  // on every packet arrival when the active path packet hasn't actually changed.
  const pathNodeIdsPrevRef = useRef<Set<string> | null>(null);
  const pathNodeIds = useMemo<Set<string> | null>(() => {
    const activePacket = pinnedPacketSnapshot ?? (filters.betaPaths ? (packets.find((p) => p.packetType === 4 || p.packetType === 5) ?? null) : null);
    if (!activePacket) {
      if (pathNodeIdsPrevRef.current !== null) pathNodeIdsPrevRef.current = null;
      return null;
    }
    const srcNode = activePacket.srcNodeId ? (nodes.get(activePacket.srcNodeId) ?? null) : null;
    const rxNode = activePacket.rxNodeId ? (nodes.get(activePacket.rxNodeId) ?? null) : null;
    const srcWithCoords = srcNode && hasCoords(srcNode) ? srcNode as MeshNode & { lat: number; lon: number } : null;
    const rxWithCoords = rxNode && hasCoords(rxNode) ? rxNode as MeshNode & { lat: number; lon: number }
      : (() => {
          for (const id of activePacket.observerIds) {
            const n = nodes.get(id);
            if (n && hasCoords(n)) return n as MeshNode & { lat: number; lon: number };
          }
          return null;
        })();
    const ids = resolvePathNodeIds(activePacket.path ?? [], srcWithCoords, rxWithCoords, nodes);
    for (const id of activePacket.observerIds) ids.add(id.toLowerCase());
    const result = ids.size > 0 ? ids : null;
    // Stabilise reference: return previous set when contents are identical, so MapView's
    // propsAreEqual check passes and it doesn't re-render just because packets changed.
    const prev = pathNodeIdsPrevRef.current;
    if (prev && result && prev.size === result.size && [...result].every((id) => prev.has(id))) {
      return prev;
    }
    pathNodeIdsPrevRef.current = result;
    return result;
  }, [pinnedPacketSnapshot, filters.betaPaths, packets, nodes]);

  // GPU node data — computed here so DeckGLOverlay can render dots without Leaflet SVG elements.
  // Colors mirror the markerColor() logic in NodeMarker.tsx (hex-clash colours are never needed
  // here because showGpuNodes=false during clash mode).
  const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
  const SEVEN_DAYS_MS    =  7 * 24 * 60 * 60 * 1000;
  const gpuNodes = useMemo<GPUNodeData[]>(() => {
    const now    = Date.now();
    const result: GPUNodeData[] = [];

    const addNode = (node: MeshNode, isInferredVariant: boolean) => {
      if (!hasCoords(node)) return;
      if (now - new Date(node.last_seen).getTime() > FOURTEEN_DAYS_MS) return;
      if (pathNodeIds && !pathNodeIds.has(node.node_id.toLowerCase())) return;

      const isStale = now - new Date(node.last_seen).getTime() > SEVEN_DAYS_MS;
      const variant = (isInferredVariant || node.is_inferred)
        ? 'inferred'
        : (node.role === 1 ? 'companion' : node.role === 3 ? 'room' : 'repeater');

      const masked = maskNodePoint(node, hiddenCoordMask);
      const maskedLat = masked?.[0] ?? node.lat!;
      const maskedLon = masked?.[1] ?? node.lon!;

      const alpha = 178; // 0.7 * 255
      let color: [number, number, number, number];
      if (isStale)                      color = [255,  68,  68, alpha];
      else if (!node.is_online)         color = [102, 102, 102, alpha];
      else if (variant === 'companion') color = [255, 152,   0, alpha];
      else if (variant === 'room')      color = [206, 147, 216, alpha];
      else if (variant === 'inferred')  color = [109, 220, 122,   230];
      else                              color = [  0, 196, 255, alpha]; // repeater

      result.push({ id: node.node_id, position: [maskedLon, maskedLat], color, radius: 3.5 });
    };

    for (const node of nodes.values()) {
      const role = node.role;
      if (role !== undefined && role !== 2 && role !== 1 && role !== 3) continue;
      if ((role === 1 || role === 3) && !filters.clientNodes) continue;
      addNode(node, false);
    }
    for (const node of inferredNodes) {
      addNode(node, true);
    }

    return result;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes, inferredNodes, hiddenCoordMask, pathNodeIds, filters.clientNodes]);

  const handlePrefixFocusActiveChange = useCallback((active: boolean) => {
    setPrefixFocusActive(active);
  }, []);

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

  // Consolidated polling - fetches all data in parallel with a single timer
  useEffect(() => {
    let cancelled = false;

    const syncAllData = async () => {
      if (!isPageVisible) return;

      // Fetch all data in parallel
      const [packetsRes, historyRes, inferredRes, statsRes] = await Promise.allSettled([
        fetch(uncachedEndpoint(withScopeParams('/api/packets/recent?limit=12', { network: networkFilter, observer: observerFilter })), { cache: 'no-store' }),
        fetch(uncachedEndpoint(withScopeParams('/api/path-beta/history', { network: networkFilter })), { cache: 'no-store' }),
        fetch(uncachedEndpoint(withScopeParams('/api/inferred-nodes', { network: networkFilter, observer: observerFilter })), { cache: 'no-store' }),
        fetch(uncachedEndpoint(withScopeParams('/api/stats', { network: networkFilter, observer: observerFilter })), { cache: 'no-store' }),
      ]);

      if (cancelled) return;

      // Process packets
      if (packetsRes.status === 'fulfilled' && packetsRes.value.ok) {
        const rows = await packetsRes.value.json() as Array<{
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
        if (!cancelled) replaceRecentPackets(rows);
      }

      // Process history
      if (historyRes.status === 'fulfilled' && historyRes.value.ok) {
        const payload = await historyRes.value.json() as { segments?: PacketHistorySegment[] };
        if (!cancelled) setPacketHistorySegments(Array.isArray(payload.segments) ? payload.segments : []);
      }

      // Process inferred nodes
      if (inferredRes.status === 'fulfilled' && inferredRes.value.ok) {
        const payload = await inferredRes.value.json() as {
          inferredNodes: MeshNode[];
          inferredActiveNodeIds: string[];
        };
        if (!cancelled) {
          setInferredNodes(payload.inferredNodes ?? []);
          setInferredActiveNodeIds(new Set((payload.inferredActiveNodeIds ?? []).map((value) => value.toLowerCase())));
        }
      }

      // Process stats (consolidates the previously separate 30s useDashboardStats poll)
      if (statsRes.status === 'fulfilled' && statsRes.value.ok) {
        const payload = await statsRes.value.json() as DashboardStats;
        if (!cancelled) setFetchedStats(payload);
      }
    };

    void syncAllData();

    // Single timer: 10s when visible, 60s when hidden (reduced from 4s/30s to lower load)
    const pollMs = isPageVisible ? 10000 : 60000;
    const timer = window.setInterval(() => { void syncAllData(); }, pollMs);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [isPageVisible, networkFilter, observerFilter, replaceRecentPackets]);

  // Removed: redundant secondary inferred-nodes poll (5-min interval).
  // The consolidated polling loop above already fetches inferred-nodes every 10s
  // and the server caches the result for 60s, so a second timer is pure overhead.

  useEffect(() => {
    const wasHexClashes = prevHexClashesRef.current;
    const isHexClashes = filters.hexClashes;

    if (!wasHexClashes && isHexClashes) {
      clashRestoreRef.current = {
        coverage: filters.coverage,
        clientNodes: filters.clientNodes,
      };
      setFilters((current) => ({
        ...current,
        coverage: false,
        clientNodes: false,
      }));
    } else if (wasHexClashes && !isHexClashes && clashRestoreRef.current) {
      const restore = clashRestoreRef.current;
      clashRestoreRef.current = null;
      setFilters((current) => ({
        ...current,
        coverage: restore.coverage,
        clientNodes: restore.clientNodes,
      }));
    }

    prevHexClashesRef.current = isHexClashes;
  }, [filters.hexClashes, filters.coverage, filters.clientNodes]);

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
    handleNodeUpdateBatch,
    handleNodeUpsert,
    handleNodeUpsertBatch,
    handleCoverageUpdate,
    handleCoverageUpdateBatch,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
    applyLinkUpdateBatch,
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

      <div className="map-layer">
        <MapView
          nodes={nodes}
          inferredNodes={inferredNodes}
          inferredActiveNodeIds={inferredActiveNodeIds}
          activeNodes={activeNodes}
          coverage={coverage}
          onDeckViewStateChange={setDeckViewState}
          showCoverage={filters.coverage}
          showClientNodes={filters.clientNodes}
          showHexClashes={filters.hexClashes}
          maxHexClashHops={filters.hexClashMaxHops}
          viablePairsArr={viablePairsArr}
          linkMetrics={linkMetrics}
          hiddenCoordMask={hiddenCoordMask}
          pathNodeIds={pathNodeIds}
          onMapReady={setMap}
          onPrefixFocusActiveChange={handlePrefixFocusActiveChange}
        />
        <DeckGLOverlay
          arcs={arcs}
          showArcs={filters.livePackets}
          packetHistorySegments={packetHistorySegments}
          showPacketHistory={filters.packetHistory}
          betaPaths={betaPacketPaths}
          betaLowSegments={betaLowConfidenceSegments}
          betaCompletionPaths={betaCompletionPaths}
          showBetaPaths={filters.betaPaths || pinnedPacketId !== null}
          pathFadingOut={pathFadingOut}
          gpuNodes={gpuNodes}
          showGpuNodes={!filters.hexClashes && !prefixFocusActive}
          viewState={deckViewState}
          hiddenCoordMask={hiddenCoordMask}
        />
      </div>

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
