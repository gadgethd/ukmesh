/**
 * MapLibreMap — replaces MapView (Leaflet).
 *
 * Node dots are rendered as a MapLibre GeoJSON circle layer (GPU, no React fibers).
 * Pan/zoom is pure GPU — zero JS work on move events.
 * RF coverage is a separate HopReach image overlay; this component owns
 * nodes, links, hex-clash lines, and privacy rings.
 * Click hit-testing uses MapLibre's built-in R-tree spatial index.
 * deck.gl overlays are integrated via @deck.gl/mapbox (MapboxOverlay).
 */
import { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import maplibregl from 'maplibre-gl';
import type { MeshNode } from '../../hooks/useNodes.js';
import { nodeStore } from '../../hooks/useNodes.js';
import { linkStateStore } from '../../hooks/useLinkState.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import {
  hasCoords,
  isProhibitedMapNode,
  maskNodePoint,
} from '../../utils/pathing.js';
import { NodeSearch } from './NodeSearch.js';
import { useOverlayStore } from '../../store/overlayStore.js';
import {
  DEFAULT_CENTER,
  DEFAULT_ZOOM,
  EMPTY_FC,
  MAP_ARC_REFRESH_INTERVAL_MS,
  MAP_LABEL_COLORS,
  MAP_RASTER_PAINT,
  MAP_REFRESH_INTERVAL_MS,
  MAP_STYLE,
  MAP_STYLE_LIGHT,
  NODE_STALE_AFTER_MS,
  TERRAIN_CONFIG,
  TERRAIN_DEM_SOURCE,
} from './mapConfig.js';
import {
  buildHiddenMask,
  buildLinksGeoJSON,
  buildNodeGeoJSON,
  buildPlannedCoverageGeoJSON,
  buildPlannedLinksGeoJSON,
  buildPlannedPinGeoJSON,
  buildPrivacyRingsGeoJSON,
  computeClashData,
  ALL_MAP_SOURCE_DIRTY_FLAGS,
  mergeMapSourceDirtyFlags,
} from './geojsonBuilders.js';
import type { MapSourceDirtyFlags } from './geojsonBuilders.js';
import { NodePopupContent } from './NodePopupContent.js';
import { NodeLegend } from './NodeLegend.js';
import { ActivitySparkline } from './ActivitySparkline.js';
import { PlannedRepeaterPopup } from './PlannedRepeaterPopup.js';
import { useWatchlist } from '../../hooks/useWatchlist.js';
import type {
  ClashComputation,
  CustomLosPoint,
  LosProfile,
  MapLibreMapProps,
  NodeFeatureProps,
  NodeLink,
  PlannedRepeater,
  PopupNodeView,
} from './types.js';
import { sampleElevationAt } from '../../utils/terrainSampler.js';
import { computeCustomLos } from '../../utils/customLos.js';
import { fetchJson, withScopeParams, type ApiScope } from '../../utils/api.js';
import { ScopedCache } from '../../utils/scopedCache.js';

// The node detail dock is an overlay, so node selection must never reserve
// camera padding or shift the visible map.
function mapPaddingForNode(_selected: string | null): { top: number; right: number; bottom: number; left: number } {
  return {
    top: 0,
    right: 0,
    bottom: 0,
    left: 0,
  };
}

const NODE_LINK_CACHE_TTL_MS = 5 * 60_000;
const nodeLinksCache = new ScopedCache<NodeLink[]>({
  name: 'map-node-links',
  ttlMs: NODE_LINK_CACHE_TTL_MS,
  maxEntries: 512,
  maxBytes: 8 * 1024 * 1024,
  maxInflight: 8,
});

function fetchNodeLinks(nodeId: string, scopeKey: string, scope: ApiScope): Promise<NodeLink[]> {
  return nodeLinksCache.getOrLoad(scopeKey, nodeId.toUpperCase(), async () => {
    const payload = await fetchJson<unknown>(
      withScopeParams(`/api/nodes/${encodeURIComponent(nodeId)}/links`, scope),
      {},
      { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024 },
    );
    if (!Array.isArray(payload)) throw new Error('node links response was not an array');
    return payload as NodeLink[];
  });
}

import {
  applyMapOverlayTheme,
  installMapSourcesAndLayers,
} from './mapSourceLayers.js';
// ── Main Component ────────────────────────────────────────────────────────────

export function MapLibreMap({
  showLinks,
  showTerrain,
  showClientNodes,
  showHexClashes,
  maxHexClashHops,
  viewshedEnabled,
  rfCoverageEnabled,
  selectedRfCoverageNodeKey = null,
  getRfCoverageNodeState,
  onShowRfCoverage,
  onClearRfCoverage,
  initialView,
  selectedNodeId = null,
  onNodeSelect,
  onMapReady,
  mapLight,
  network,
  observer,
  privacyGeneration,
  showMapChrome = true,
}: MapLibreMapProps) {
  const requestScope = useMemo(() => ({ network, observer }), [network, observer]);
  const requestScopeKey = `${network ?? 'all'}|${observer ?? 'all'}|privacy-${privacyGeneration}`;
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const mapLoadedRef = useRef(false);
  const nodesRef = useRef(nodeStore.getState().nodes);
  const viablePairsRef = useRef(linkStateStore.getState().viablePairsArr);
  const linkMetricsRef = useRef(linkStateStore.getState().linkMetrics);
  const showLinksRef = useRef(showLinks);
  const showTerrainRef = useRef(showTerrain);
  const showClientNodesRef = useRef(showClientNodes);
  const showHexClashesRef = useRef(showHexClashes);
  const maxHexClashHopsRef = useRef(maxHexClashHops);
  const viewshedEnabledRef = useRef(viewshedEnabled);
  const mapLightRef = useRef(mapLight);
  const pathNodeIdsRef = useRef(useOverlayStore.getState().pathNodeIds);
  const replayNodeIdsRef = useRef(useOverlayStore.getState().replayNodeIds);
  const setClashPathLines = useOverlayStore((state) => state.setClashPathLines);
  const hiddenCoordMaskRef = useRef<Map<string, HiddenMaskGeometry>>(new Map());
  const refreshTimerRef = useRef<number | null>(null);
  const lastArcActivityRef = useRef(0);
  const dirtyFlagsRef = useRef<MapSourceDirtyFlags>({ ...ALL_MAP_SOURCE_DIRTY_FLAGS });
  const clashRef = useRef<ClashComputation>({
    clashOffenderNodeIds: new Set(),
    clashRelayIds: new Set(),
    clashPathLines: [],
    clashModeActive: false,
  });
  const selectedNodeIdRef = useRef<string | null>(selectedNodeId);
  const onNodeSelectRef = useRef(onNodeSelect);
  onNodeSelectRef.current = onNodeSelect;

  const customLosNodeClickedRef = useRef(false);
  // handleCustomLosPointRef is assigned after handleCustomLosPoint is defined below
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleCustomLosPointRef = useRef<(point: CustomLosPoint) => Promise<void>>(null as any);
  // Planned repeater placement
  const plannedRepeatersRef = useRef<PlannedRepeater[]>([]);
  const plannedPollRefs = useRef<Map<string, { stop: () => void }>>(new Map());
  // Plans whose LOS overlay has already been auto-applied (so we don't re-apply
  // it or fight a user who manually hid it from the popup).
  const plannedLosAppliedRef = useRef<Set<string>>(new Set());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placePlannedRepeaterRef = useRef<(lat: number, lon: number) => Promise<void>>(null as any);
  // Planned-repeater popup (parallel to the normal node popup)
  const plannedPopupRef = useRef<maplibregl.Popup | null>(null);
  const plannedPopupContainerRef = useRef<HTMLDivElement>(document.createElement('div'));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const openPlannedPopupRef = useRef<(planId: string, lngLat: maplibregl.LngLatLike) => void>(null as any);

  const [plannedPopupState, setPlannedPopupState] = useState<{ planId: string; lngLat: maplibregl.LngLatLike } | null>(null);
  const [popupLinks, setPopupLinks] = useState<NodeLink[] | null>(null);
  const [focusedPrefix, setFocusedPrefix] = useState<string | null>(null);
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  const [focusedPrefixNodeIds, setFocusedPrefixNodeIds] = useState<Set<string> | null>(null);
  const [popupVersion, setPopupVersion] = useState(0);
  const focusTimerRef = useRef<number | null>(null);
  const dockRef = useRef<HTMLElement>(null);
  const watchlist = useWatchlist();
  const [copyLinkLabel, setCopyLinkLabel] = useState('Copy link');

  // MapLibre's stylesheet is loaded only when the map component mounts, keeping
  // map-specific CSS out of non-map entry points.
  useEffect(() => {
    void import('maplibre-gl/dist/maplibre-gl.css');
  }, []);

  // -- Map theme (light/dark) -------------------------------------------------
  useEffect(() => {
      const theme = mapLight ? 'light' : 'dark';
      mapLightRef.current = mapLight;
      const map = mapRef.current;
      if (map && mapLoadedRef.current) {
        const oldId = mapLight ? 'carto-dark' : 'carto-light';
        const newId = mapLight ? 'carto-light' : 'carto-dark';
        const variant = mapLight ? 'light_nolabels' : 'dark_nolabels';
        if (map.getSource(newId)) return;
        if (map.getLayer('background')) map.removeLayer('background');
        if (map.getLayer('bg-fill')) map.removeLayer('bg-fill');
        if (map.getSource(oldId)) map.removeSource(oldId);
        map.addSource(newId, {
          type: 'raster',
          tiles: ['a', 'b', 'c', 'd'].map(
            (s) => `https://${s}.basemaps.cartocdn.com/${variant}/{z}/{x}/{y}{r}.png`,
          ),
          tileSize: 256,
          attribution:
            '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
          maxzoom: 19,
        });
        // Insert bg-fill + basemap at the very bottom
        const firstLayer = map.getStyle().layers[0]?.id;
        map.addLayer(
          { id: 'bg-fill', type: 'background', paint: { 'background-color': mapLight ? '#edf2f7' : '#080d14' } },
          firstLayer,
        );
        map.addLayer(
          {
            id: 'background',
            type: 'raster',
            source: newId,
            paint: MAP_RASTER_PAINT[mapLight ? 'light' : 'dark'],
          },
          map.getStyle().layers[1]?.id,  // after bg-fill, before vector labels
        );

        const labelColors = mapLight ? MAP_LABEL_COLORS.light : MAP_LABEL_COLORS.dark;
        for (const [layerId, colorKey] of [
          ['map-labels-place', 'place'],
          ['map-labels-water', 'water'],
          ['map-labels-road', 'road'],
        ] as const) {
          if (!map.getLayer(layerId)) continue;
          map.setPaintProperty(layerId, 'text-color', labelColors[colorKey]);
          map.setPaintProperty(layerId, 'text-halo-color', labelColors.halo);
          map.setPaintProperty(layerId, 'text-halo-width', colorKey === 'place' ? 1.7 : 1.5);
          map.setPaintProperty(layerId, 'text-halo-blur', 0.1);
        }

        applyMapOverlayTheme(map, theme);
        const nodes = nodesRef.current;
        const viablePairs = viablePairsRef.current;
        const linkMetrics = linkMetricsRef.current;
        const hiddenMask = hiddenCoordMaskRef.current;
        const links = showLinksRef.current
          ? buildLinksGeoJSON(nodes, viablePairs, linkMetrics, hiddenMask, theme)
          : EMPTY_FC;
        (map.getSource('viable-links') as maplibregl.GeoJSONSource | undefined)?.setData(links);
        const plannedLinks = viewshedEnabledRef.current && showLinksRef.current
          ? buildPlannedLinksGeoJSON(plannedRepeatersRef.current, nodes, hiddenMask, theme)
          : EMPTY_FC;
        (map.getSource('planned-links') as maplibregl.GeoJSONSource | undefined)?.setData(plannedLinks);
      }
  }, [mapLight]);

  // -- LOS profiles (client-side, multi-node, auto-expire) -------------------

  const addLosLoading = useOverlayStore((state) => state.addLosLoading);
  const setLosProfilesForNode = useOverlayStore((state) => state.setLosProfilesForNode);
  const removeLosNode = useOverlayStore((state) => state.removeLosNode);

  // Targeted selectors — only re-render MapLibreMap when the SELECTED node's LOS
  // status changes (boolean equality), not every time any node's Set changes.
  const popupNodeId = selectedNodeId;
  const popupLosActive = useOverlayStore((state) => popupNodeId != null && state.losNodeIds.has(popupNodeId));
  const popupLosLoading = useOverlayStore((state) => popupNodeId != null && state.losLoadingIds.has(popupNodeId));

  // Same targeted LOS selectors for the open planned-repeater popup.
  const plannedPopupId = plannedPopupState?.planId ?? null;
  const plannedPopupLosActive = useOverlayStore((state) => plannedPopupId != null && state.losNodeIds.has(plannedPopupId));
  const plannedPopupLosLoading = useOverlayStore((state) => plannedPopupId != null && state.losLoadingIds.has(plannedPopupId));

  // Timers for auto-expiry: nodeId → setTimeout handle
  const losTimersRef = useRef<Map<string, number>>(new Map());

  const clearLosTimer = useCallback((nodeId: string) => {
    const handle = losTimersRef.current.get(nodeId);
    if (handle !== undefined) {
      window.clearTimeout(handle);
      losTimersRef.current.delete(nodeId);
    }
  }, []);

  const handleToggleLos = useCallback(async (nodeId: string) => {
    // Read current state imperatively — avoids stale closure from useCallback deps.
    if (useOverlayStore.getState().losNodeIds.has(nodeId)) {
      clearLosTimer(nodeId);
      removeLosNode(nodeId);
      return;
    }
    addLosLoading(nodeId);
    // deck.gl renders at actual altitude; MapLibre terrain is visually exaggerated.
    // Multiply altitude by the terrain exaggeration factor so lines appear
    // above the terrain mesh rather than inside it.
    const ANTENNA_H = 10;
    const EXAG = TERRAIN_CONFIG.exaggeration;
    try {
      const links = await fetchNodeLinks(nodeId, requestScopeKey, requestScope);
      const sourceNode = nodesRef.current.get(nodeId);
      if (!sourceNode || !hasCoords(sourceNode)) {
        setLosProfilesForNode(nodeId, []);
      } else {
        const srcElev = ((sourceNode.elevation_m ?? 0) + ANTENNA_H) * EXAG;
        const profiles = links
          .map((link): LosProfile | null => {
            const peer = nodesRef.current.get(link.peer_id);
            if (!peer || !hasCoords(peer)) return null;
            const peerElev = ((peer.elevation_m ?? 0) + ANTENNA_H) * EXAG;
            return {
              peer_id: link.peer_id,
              peer_name: link.peer_name,
              itm_path_loss_db: link.itm_path_loss_db,
              itm_viable: link.itm_path_loss_db != null && link.itm_path_loss_db <= 129.5,
              profile: [
                [sourceNode.lon, sourceNode.lat, srcElev],
                [peer.lon, peer.lat, peerElev],
              ],
            };
          })
          .filter((p): p is LosProfile => p !== null);
        setLosProfilesForNode(nodeId, profiles);
      }
    } catch {
      setLosProfilesForNode(nodeId, []);
    }
    // Auto-expire after 15 seconds
    clearLosTimer(nodeId);
    const handle = window.setTimeout(() => {
      losTimersRef.current.delete(nodeId);
      removeLosNode(nodeId);
    }, 15_000);
    losTimersRef.current.set(nodeId, handle);
  }, [
    addLosLoading,
    clearLosTimer,
    removeLosNode,
    requestScope,
    requestScopeKey,
    setLosProfilesForNode,
  ]);

  // -- Custom LOS (two-point terrain-sampled LOS) ----------------------------

  const customLosMode = useOverlayStore((state) => state.customLosMode);
  const customLosStart = useOverlayStore((state) => state.customLosStart);
  const setCustomLosMode = useOverlayStore((state) => state.setCustomLosMode);
  const setCustomLosStart = useOverlayStore((state) => state.setCustomLosStart);
  const setCustomLosResult = useOverlayStore((state) => state.setCustomLosResult);
  const clearCustomLos = useOverlayStore((state) => state.clearCustomLos);
  const planRepeaterMode = useOverlayStore((state) => state.planRepeaterMode);
  const plannedRepeaters = useOverlayStore((state) => state.plannedRepeaters);
  const requestedPlanCoordinates = useOverlayStore((state) => state.requestedPlanCoordinates);
  const setPlanRepeaterMode = useOverlayStore((state) => state.setPlanRepeaterMode);

  // Stable async handler called by map click handlers (reads state via getState())
  const handleCustomLosPoint = useCallback(async (point: CustomLosPoint) => {
    const { customLosStart: currentStart } = useOverlayStore.getState();
    if (!currentStart) {
      setCustomLosStart(point);
    } else {
      setCustomLosStart(null);
      const segments = await computeCustomLos(currentStart, point);
      setCustomLosResult(segments);
    }
  }, [setCustomLosStart, setCustomLosResult]);

  // Keep ref in sync so map event handlers always call the latest version
  useEffect(() => {
    handleCustomLosPointRef.current = handleCustomLosPoint;
  }, [handleCustomLosPoint]);

  // -- Planned repeater placement --------------------------------------------

  const handleRemovePlannedRepeater = useCallback((planId: string) => {
    const poll = plannedPollRefs.current.get(planId);
    if (poll) {
      poll.stop();
      plannedPollRefs.current.delete(planId);
    }
    setPlannedPopupState((prev) => (prev?.planId === planId ? null : prev));
    plannedLosAppliedRef.current.delete(planId);
    useOverlayStore.getState().removeLosNode(planId);
    useOverlayStore.getState().removePlannedRepeater(planId);
  }, []);

  // Rebuild the planned-link lines from the current plans + live node positions,
  // and toggle their visibility with the global Links toggle.
  const updatePlannedLinks = useCallback(() => {
    if (!mapLoadedRef.current || !mapRef.current) return;
    const data = viewshedEnabledRef.current && showLinksRef.current
      ? buildPlannedLinksGeoJSON(
          plannedRepeatersRef.current,
          nodesRef.current,
          hiddenCoordMaskRef.current,
          mapLightRef.current ? 'light' : 'dark',
        )
      : EMPTY_FC;
    (mapRef.current.getSource('planned-links') as maplibregl.GeoJSONSource | undefined)?.setData(data);
    mapRef.current.setLayoutProperty('planned-links-layer', 'visibility', viewshedEnabledRef.current && showLinksRef.current ? 'visible' : 'none');
  }, []);

  const openPlannedPopup = useCallback((planId: string, lngLat: maplibregl.LngLatLike) => {
    if (!viewshedEnabledRef.current) return;
    onNodeSelectRef.current?.(null); // close any normal node panel
    setPlannedPopupState({ planId, lngLat });
  }, []);

  useEffect(() => {
    openPlannedPopupRef.current = openPlannedPopup;
  }, [openPlannedPopup]);

  // Build 3D LOS sight-lines for a planned repeater from its server-predicted
  // links: plan location → each viable peer, at antenna-tip altitude (same
  // representation the normal "Show LOS" overlay uses).
  const buildPlannedLosProfiles = useCallback(async (repeater: PlannedRepeater): Promise<LosProfile[]> => {
    if (!viewshedEnabledRef.current) return [];
    const links = repeater.coverage?.predicted_links;
    if (!links || links.length === 0) return [];
    const ANTENNA_H = 10;
    const EXAG = TERRAIN_CONFIG.exaggeration;
    const srcElevRaw = await sampleElevationAt(repeater.lon, repeater.lat).catch(() => 0);
    const srcElev = ((srcElevRaw ?? 0) + ANTENNA_H) * EXAG;
    const profiles: LosProfile[] = [];
    for (const link of links) {
      const peer = nodesRef.current.get(link.peer_id);
      if (!peer || !hasCoords(peer)) continue;
      const [peerLat, peerLon] = maskNodePoint(peer, hiddenCoordMaskRef.current);
      const peerElev = ((peer.elevation_m ?? 0) + ANTENNA_H) * EXAG;
      profiles.push({
        peer_id: link.peer_id,
        peer_name: link.peer_name,
        itm_path_loss_db: link.itm_path_loss_db,
        itm_viable: link.itm_viable,
        profile: [
          [repeater.lon, repeater.lat, srcElev],
          [peerLon, peerLat, peerElev],
        ],
      });
    }
    return profiles;
  }, []);

  const handleTogglePlannedLos = useCallback(async (planId: string) => {
    if (!viewshedEnabledRef.current) return;
    if (useOverlayStore.getState().losNodeIds.has(planId)) {
      useOverlayStore.getState().removeLosNode(planId);
      return;
    }
    const repeater = plannedRepeatersRef.current.find((r) => r.id === planId);
    if (!repeater) return;
    useOverlayStore.getState().addLosLoading(planId);
    const profiles = await buildPlannedLosProfiles(repeater);
    useOverlayStore.getState().setLosProfilesForNode(planId, profiles);
  }, [buildPlannedLosProfiles]);

  // Auto-show LOS for ready plans (no toggle needed) and clean up removed ones.
  useEffect(() => {
    if (!viewshedEnabled) return;
    const liveIds = new Set(plannedRepeaters.map((r) => r.id));
    for (const id of Array.from(plannedLosAppliedRef.current)) {
      if (!liveIds.has(id)) {
        plannedLosAppliedRef.current.delete(id);
        useOverlayStore.getState().removeLosNode(id);
      }
    }
    for (const repeater of plannedRepeaters) {
      if (repeater.status !== 'ready') continue;
      if (plannedLosAppliedRef.current.has(repeater.id)) continue;
      plannedLosAppliedRef.current.add(repeater.id);
      useOverlayStore.getState().addLosLoading(repeater.id);
      void buildPlannedLosProfiles(repeater).then((profiles) => {
        useOverlayStore.getState().setLosProfilesForNode(repeater.id, profiles);
      });
    }
  }, [viewshedEnabled, plannedRepeaters, buildPlannedLosProfiles]);

  const placePlannedRepeater = useCallback(async (lat: number, lon: number) => {
    void lat;
    void lon;
    // Retired with the legacy viewshed planner. Kept as a no-op until the
    // dormant planner UI is removed after the rollback window.
  }, []);

  // Keep handler refs in sync for map event handlers
  useEffect(() => {
    placePlannedRepeaterRef.current = placePlannedRepeater;
  }, [placePlannedRepeater]);

  useEffect(() => {
    if (!viewshedEnabled || requestedPlanCoordinates.length === 0) return;
    const available = Math.max(0, 5 - useOverlayStore.getState().plannedRepeaters.length);
    for (const coordinate of requestedPlanCoordinates.slice(0, available)) {
      void placePlannedRepeater(coordinate.lat, coordinate.lon);
    }
    useOverlayStore.getState().clearPlanRestoreRequest();
  }, [placePlannedRepeater, requestedPlanCoordinates, viewshedEnabled]);

  // Keep planned repeaters ref in sync
  useEffect(() => {
    plannedRepeatersRef.current = viewshedEnabled ? plannedRepeaters : [];
  }, [viewshedEnabled, plannedRepeaters]);

  // Update planned coverage, pin, and predicted-link layers when plans change
  useEffect(() => {
    if (!mapLoadedRef.current || !mapRef.current) return;
    const visiblePlans = viewshedEnabled ? plannedRepeaters : [];
    (mapRef.current.getSource('planned-coverage') as maplibregl.GeoJSONSource | undefined)
      ?.setData(buildPlannedCoverageGeoJSON(visiblePlans));
    (mapRef.current.getSource('planned-pins') as maplibregl.GeoJSONSource | undefined)
      ?.setData(buildPlannedPinGeoJSON(visiblePlans));
    updatePlannedLinks();
  }, [viewshedEnabled, plannedRepeaters, updatePlannedLinks]);

  // Stop old-scope jobs before they can publish into a new network/observer.
  useEffect(() => () => {
    for (const [planId, poll] of plannedPollRefs.current) {
      poll.stop();
      useOverlayStore.getState().updatePlannedRepeater(planId, { status: 'error' });
    }
    plannedPollRefs.current.clear();
  }, [requestScopeKey]);

  // Clean up all planned repeaters, pollers, and LOS overlays on unmount
  useEffect(() => () => {
    for (const [, poll] of plannedPollRefs.current) {
      poll.stop();
    }
    plannedPollRefs.current.clear();
    for (const planId of plannedLosAppliedRef.current) {
      useOverlayStore.getState().removeLosNode(planId);
    }
    plannedLosAppliedRef.current.clear();
  }, []);

  // Cursor crosshair while in custom LOS mode or plan repeater mode
  useEffect(() => {
    const canvas = mapRef.current?.getCanvas();
    if (!canvas) return;
    canvas.style.cursor = (customLosMode || (viewshedEnabled && planRepeaterMode)) ? 'crosshair' : '';
  }, [viewshedEnabled, customLosMode, planRepeaterMode]);

  // Escape key clears custom LOS mode or plan repeater mode
  useEffect(() => {
    if (!customLosMode && !(viewshedEnabled && planRepeaterMode)) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (customLosMode) clearCustomLos();
        if (viewshedEnabled && planRepeaterMode) setPlanRepeaterMode(false);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [viewshedEnabled, customLosMode, planRepeaterMode, clearCustomLos, setPlanRepeaterMode]);

  // Escape closes the node detail dock (when not in a picking mode, which the
  // handler above owns). Also move focus into the dock when it opens so keyboard
  // users land on the panel rather than being stranded on the map canvas.
  useEffect(() => {
    if (!selectedNodeId || customLosMode || (viewshedEnabled && planRepeaterMode)) return;
    dockRef.current?.focus();
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onNodeSelectRef.current?.(null);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [selectedNodeId, customLosMode, viewshedEnabled, planRepeaterMode]);

  // -- Focus mode (same-prefix highlight) ------------------------------------

  const clearFocusTimer = useCallback(() => {
    if (focusTimerRef.current !== null) {
      window.clearTimeout(focusTimerRef.current);
      focusTimerRef.current = null;
    }
  }, []);

  const refreshMapSources = useCallback(() => {
    if (!mapLoadedRef.current || !mapRef.current) return;

    const dirty = dirtyFlagsRef.current;
    dirtyFlagsRef.current = {
      nodes: false,
      privacy: false,
      links: false,
      clash: false,
      plannedLinks: false,
    };
    const nodes = nodesRef.current;
    const viablePairsArr = viablePairsRef.current;
    const linkMetrics = linkMetricsRef.current;
    const currentPathNodeIds = pathNodeIdsRef.current;
    const maskDirty = dirty.nodes || dirty.privacy || dirty.links || dirty.clash || dirty.plannedLinks;
    const currentHiddenCoordMask = maskDirty ? buildHiddenMask(nodes) : hiddenCoordMaskRef.current;
    if (maskDirty) hiddenCoordMaskRef.current = currentHiddenCoordMask;

    const clashDirty = dirty.clash || dirty.nodes || dirty.links;
    const clash = clashDirty
      ? computeClashData(
          nodes,
          viablePairsArr,
          linkMetrics,
          showHexClashesRef.current,
          maxHexClashHopsRef.current,
          focusedNodeId,
          focusedPrefixNodeIds,
        )
      : clashRef.current;
    if (clashDirty) clashRef.current = clash;

    if (dirty.nodes || clashDirty) {
      const nodeGeoJSON = buildNodeGeoJSON(
        nodes,
        currentHiddenCoordMask,
        showClientNodesRef.current,
        clash.clashOffenderNodeIds,
        clash.clashRelayIds,
        clash.clashModeActive,
        clash.clashModeActive ? null : currentPathNodeIds,
        replayNodeIdsRef.current,
        Date.now(),
      );
      (mapRef.current.getSource('nodes') as maplibregl.GeoJSONSource | undefined)?.setData(nodeGeoJSON);
    }

    if (dirty.privacy || dirty.nodes) {
      const privacyGeoJSON = buildPrivacyRingsGeoJSON(nodes, currentHiddenCoordMask);
      (mapRef.current.getSource('privacy-rings') as maplibregl.GeoJSONSource | undefined)?.setData(privacyGeoJSON);
    }

    if (dirty.links || dirty.nodes) {
      const linksGeoJSON = showLinksRef.current
        ? buildLinksGeoJSON(
            nodes,
            viablePairsArr,
            linkMetrics,
            currentHiddenCoordMask,
            mapLightRef.current ? 'light' : 'dark',
          )
        : EMPTY_FC;
      (mapRef.current.getSource('viable-links') as maplibregl.GeoJSONSource | undefined)?.setData(linksGeoJSON);
      mapRef.current.setLayoutProperty('viable-links-layer', 'visibility', showLinksRef.current ? 'visible' : 'none');
    }

    if (clashDirty) {
      setClashPathLines(clash.clashModeActive ? clash.clashPathLines : []);
      (mapRef.current.getSource('clash-lines') as maplibregl.GeoJSONSource | undefined)?.setData(EMPTY_FC);
      mapRef.current.setLayoutProperty('clash-lines-layer', 'visibility', 'none');
    }

    if (dirty.plannedLinks || dirty.nodes || dirty.links) updatePlannedLinks();
  }, [viewshedEnabled, focusedNodeId, focusedPrefixNodeIds, setClashPathLines, updatePlannedLinks]);

  const scheduleRefresh = useCallback((flags: Partial<MapSourceDirtyFlags> = ALL_MAP_SOURCE_DIRTY_FLAGS) => {
    dirtyFlagsRef.current = mergeMapSourceDirtyFlags(dirtyFlagsRef.current, flags);
    if (refreshTimerRef.current !== null) return;
    const interval = Date.now() - lastArcActivityRef.current < 5_000
      ? MAP_ARC_REFRESH_INTERVAL_MS
      : MAP_REFRESH_INTERVAL_MS;
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null;
      refreshMapSources();
    }, interval);
  }, [refreshMapSources]);

  useEffect(() => {
    const noteArcActivity = () => { lastArcActivityRef.current = Date.now(); };
    window.addEventListener('meshcore:packet-observed', noteArcActivity);
    return () => window.removeEventListener('meshcore:packet-observed', noteArcActivity);
  }, []);

  useEffect(() => {
    viewshedEnabledRef.current = viewshedEnabled;
    if (!viewshedEnabled) {
      setPlanRepeaterMode(false);
      setPlannedPopupState(null);
      plannedPopupRef.current?.remove();
      for (const [, poll] of plannedPollRefs.current) {
        poll.stop();
      }
      plannedPollRefs.current.clear();
      for (const repeater of useOverlayStore.getState().plannedRepeaters) {
        useOverlayStore.getState().removeLosNode(repeater.id);
        useOverlayStore.getState().removePlannedRepeater(repeater.id);
      }
      plannedLosAppliedRef.current.clear();
    }
    scheduleRefresh();
  }, [viewshedEnabled, scheduleRefresh, setPlanRepeaterMode]);

  const handleFocusSamePrefix = useCallback((nodeId: string) => {
    const prefix = nodeId.slice(0, 2).toUpperCase();
    const ids = Array.from(nodesRef.current.values())
      .filter((node) => hasCoords(node) && (node.role === undefined || node.role === 2))
      .filter((node) => node.node_id.slice(0, 2).toUpperCase() === prefix)
      .map((node) => node.node_id);
    clearFocusTimer();
    setFocusedPrefix(prefix);
    setFocusedNodeId(nodeId);
    setFocusedPrefixNodeIds(new Set(ids.length > 0 ? ids : [nodeId]));
    // Auto-clear after 10s
    focusTimerRef.current = window.setTimeout(() => {
      setFocusedPrefix(null);
      setFocusedNodeId(null);
      setFocusedPrefixNodeIds(null);
      focusTimerRef.current = null;
    }, 10_000);
  }, [clearFocusTimer]);

  useEffect(() => () => {
    clearFocusTimer();
    if (refreshTimerRef.current !== null) {
      window.clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
  }, [clearFocusTimer]);

  // -- Map initialisation (runs once on mount) --------------------------------

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: mapLight ? MAP_STYLE_LIGHT : MAP_STYLE,
      center: initialView ? [initialView.lon, initialView.lat] : [DEFAULT_CENTER[1], DEFAULT_CENTER[0]],
      zoom: initialView?.zoom ?? DEFAULT_ZOOM,
      maxPitch: 0,
      minZoom: 6,
      attributionControl: false,
    });

    map.on('load', () => {
      mapLoadedRef.current = true;
      mapRef.current = map;
      map.setPadding(mapPaddingForNode(null));
      onMapReady?.(map);

      installMapSourcesAndLayers(map, {
        showLinks: showLinksRef.current,
        mapLight: mapLightRef.current,
      });
      // ── Click handler ──────────────────────────────────────────────────────
      map.on('click', 'planned-pins-dot', (e) => {
        if (!viewshedEnabledRef.current) return;
        // Click a planned pin (in any mode) to open its popup with coords,
        // predicted links, and a remove action.
        const feature = e.features?.[0];
        if (!feature) return;
        const planId = (feature.properties as { plan_id: string }).plan_id;
        const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
        customLosNodeClickedRef.current = true; // prevent general map-click from placing a new pin
        openPlannedPopupRef.current(planId, { lng: coords[0], lat: coords[1] });
      });

      map.on('click', 'node-dots-hit', (e) => {
        const feature = e.features?.[0];
        if (!feature) return;
        const props = feature.properties as NodeFeatureProps;

        // In plan repeater mode, node clicks place a repeater on the node's location
        if (viewshedEnabledRef.current && useOverlayStore.getState().planRepeaterMode) {
          customLosNodeClickedRef.current = true;
          if (plannedRepeatersRef.current.length < 5) {
            const node = nodesRef.current.get(props.node_id);
            if (node && hasCoords(node)) {
              void placePlannedRepeaterRef.current(node.lat, node.lon);
            }
          }
          return;
        }

        // In custom LOS mode, intercept node clicks as point picks
        if (useOverlayStore.getState().customLosMode) {
          customLosNodeClickedRef.current = true; // always consume to prevent map-click firing
          if (!props.is_prohibited) {
            const node = nodesRef.current.get(props.node_id);
            if (node && hasCoords(node)) {
              void handleCustomLosPointRef.current({ lat: node.lat, lon: node.lon, elevation_m: node.elevation_m ?? 0 });
            }
          }
          return;
        }

        // MapLibre serialises properties to JSON strings for non-primitive types,
        // but all our props are primitives so this is safe.
        setPopupLinks(null);
        onNodeSelectRef.current?.(props.node_id);
      });

      // General map click — used for custom LOS mode and plan repeater placement on empty areas
      map.on('click', (e) => {
        const { lng, lat } = e.lngLat;
        // A node/pin layer click already handled this event — read and clear the
        // flag up-front so it can never persist into a later background click.
        const consumed = customLosNodeClickedRef.current;
        customLosNodeClickedRef.current = false;

        if (viewshedEnabledRef.current && useOverlayStore.getState().planRepeaterMode) {
          if (consumed) return;
          if (plannedRepeatersRef.current.length < 5) {
            void placePlannedRepeaterRef.current(lat, lng);
          }
          return;
        }

        if (!useOverlayStore.getState().customLosMode) {
          // Not in a special picking mode: a click on empty map deselects the
          // current node (closes the docked detail panel), matching the intuition
          // that clicking away dismisses the selection.
          if (!consumed && selectedNodeIdRef.current) onNodeSelectRef.current?.(null);
          return;
        }
        if (consumed) return; // Node dot / planned pin already handled above
        void sampleElevationAt(lng, lat).then((elevation_m) => {
          void handleCustomLosPointRef.current({ lat, lon: lng, elevation_m });
        });
      });

      // Make cursor a pointer over node dots and planned pins
      map.on('mouseenter', 'node-dots-hit', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'node-dots-hit', () => {
        map.getCanvas().style.cursor = (viewshedEnabledRef.current && useOverlayStore.getState().planRepeaterMode) || useOverlayStore.getState().customLosMode ? 'crosshair' : '';
      });
      map.on('mouseenter', 'planned-pins-dot', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'planned-pins-dot', () => {
        map.getCanvas().style.cursor = viewshedEnabledRef.current && useOverlayStore.getState().planRepeaterMode ? 'crosshair' : '';
      });

      refreshMapSources();

      // Apply any pre-existing selection (e.g. ?node= deep link) to the highlight.
      if (selectedNodeIdRef.current) {
        map.setFilter('node-dots-selected-halo', ['==', ['get', 'node_id'], selectedNodeIdRef.current]);
        map.setFilter('node-dots-selected', ['==', ['get', 'node_id'], selectedNodeIdRef.current]);
      }

      // Restore terrain if it was saved in preferences
      if (showTerrainRef.current) {
        console.log('[terrain] restoring on load, config:', JSON.stringify(TERRAIN_DEM_SOURCE));
        try {
          map.addSource('terrain-dem', TERRAIN_DEM_SOURCE);
          const src = map.getSource('terrain-dem') as maplibregl.RasterDEMTileSource | undefined;
          src?.on('error', (e: ErrorEvent) => console.error('[terrain] source error (load):', e));
          map.addLayer({
            id: 'hillshade', type: 'hillshade', source: 'terrain-dem', minzoom: 7,
            paint: { 'hillshade-exaggeration': 0.7, 'hillshade-shadow-color': '#000000', 'hillshade-highlight-color': '#ffffff', 'hillshade-illumination-anchor': 'viewport' },
          }, 'node-dots');
          map.setSky({ 'atmosphere-blend': 0.5 });
          map.setMaxPitch(85);
          map.setTerrain(TERRAIN_CONFIG);
          console.log('[terrain] restored on load, getTerrain()=', JSON.stringify(map.getTerrain()));
          map.easeTo({ pitch: 45, duration: 600 });
        } catch (err) {
          console.error('[terrain] restore on load failed:', err);
        }
      }
    });

    return () => {
      mapLoadedRef.current = false;
      setClashPathLines([]);
      map.remove();
      mapRef.current = null;
    };
  // Initial theme is captured on mount; subsequent theme changes are applied by
  // the dedicated theme effect without rebuilding the MapLibre instance.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialView, onMapReady, onNodeSelect, refreshMapSources, setClashPathLines]);

  // -- Imperative source updates ---------------------------------------------

  useEffect(() => {
    showLinksRef.current = showLinks;
    updatePlannedLinks();
    scheduleRefresh({ nodes: true, links: true, plannedLinks: true, clash: true });
  }, [showLinks, scheduleRefresh, updatePlannedLinks]);

  useEffect(() => {
    showTerrainRef.current = showTerrain;
    const map = mapRef.current;
    console.log('[terrain] effect: showTerrain=', showTerrain, 'map=', !!map, 'loaded=', mapLoadedRef.current);
    if (!map || !mapLoadedRef.current) return;
    if (showTerrain) {
      try {
        if (!map.getSource('terrain-dem')) {
          map.addSource('terrain-dem', TERRAIN_DEM_SOURCE);
          console.log('[terrain] source added, config:', JSON.stringify(TERRAIN_DEM_SOURCE));
          const src = map.getSource('terrain-dem') as maplibregl.RasterDEMTileSource | undefined;
          src?.on('error', (e: ErrorEvent) => console.error('[terrain] source error:', e));
        }
        if (!map.getLayer('hillshade')) {
          map.addLayer({
            id: 'hillshade', type: 'hillshade', source: 'terrain-dem', minzoom: 7,
            paint: { 'hillshade-exaggeration': 0.7, 'hillshade-shadow-color': '#000000', 'hillshade-highlight-color': '#ffffff', 'hillshade-illumination-anchor': 'viewport' },
          }, 'node-dots');
          console.log('[terrain] hillshade layer added');
        }
        map.setSky({ 'atmosphere-blend': 0.5 });
        map.setMaxPitch(85);
        console.log('[terrain] calling setTerrain with:', JSON.stringify(TERRAIN_CONFIG));
        map.setTerrain(TERRAIN_CONFIG);
        console.log('[terrain] setTerrain complete, getTerrain()=', JSON.stringify(map.getTerrain()));
        map.easeTo({ pitch: 45, duration: 600 });
      } catch (err) {
        console.error('[terrain] setup failed:', err);
      }
    } else {
      map.setTerrain(null);
      if (map.getLayer('hillshade')) map.removeLayer('hillshade');
      map.setSky({});
      if (map.getSource('terrain-dem')) map.removeSource('terrain-dem');
      map.easeTo({ pitch: 0, duration: 400 });
      setTimeout(() => map.setMaxPitch(0), 400);
    }
  }, [showTerrain]);

  useEffect(() => {
    showClientNodesRef.current = showClientNodes;
    scheduleRefresh({ nodes: true });
  }, [showClientNodes, scheduleRefresh]);

  useEffect(() => {
    showHexClashesRef.current = showHexClashes;
    scheduleRefresh({ nodes: true, clash: true });
  }, [showHexClashes, scheduleRefresh]);

  useEffect(() => {
    maxHexClashHopsRef.current = maxHexClashHops;
    scheduleRefresh({ nodes: true, clash: true });
  }, [maxHexClashHops, scheduleRefresh]);

  useEffect(() => {
    selectedNodeIdRef.current = selectedNodeId;
  }, [selectedNodeId]);

  useEffect(() => {
    const unsubscribeNodes = nodeStore.subscribe(() => {
      const nextNodes = nodeStore.getState().nodes;
      // Packet/message updates share the same node Map reference. Ignore those
      // emissions so live feed traffic does not trigger a 3.6k-node rebuild.
      if (nextNodes === nodesRef.current) return;
      nodesRef.current = nextNodes;
      scheduleRefresh({ nodes: true, privacy: true, links: true, clash: true, plannedLinks: true });
      if (selectedNodeIdRef.current) setPopupVersion((value) => value + 1);
    });
    const unsubscribeLinks = linkStateStore.subscribe(() => {
      const linkState = linkStateStore.getState();
      viablePairsRef.current = linkState.viablePairsArr;
      linkMetricsRef.current = linkState.linkMetrics;
      scheduleRefresh({ links: true, clash: true, nodes: true, plannedLinks: true });
    });
    const unsubscribeOverlay = useOverlayStore.subscribe((overlayState) => {
      if (
        overlayState.pathNodeIds === pathNodeIdsRef.current
        && overlayState.replayNodeIds === replayNodeIdsRef.current
      ) return;
      pathNodeIdsRef.current = overlayState.pathNodeIds;
      replayNodeIdsRef.current = overlayState.replayNodeIds;
      scheduleRefresh({ nodes: true });
    });

    return () => {
      unsubscribeNodes();
      unsubscribeLinks();
      unsubscribeOverlay();
    };
  }, [scheduleRefresh]);

  useEffect(() => {
    scheduleRefresh({ nodes: true, clash: true });
  }, [focusedNodeId, focusedPrefixNodeIds, scheduleRefresh]);

  // -- Popup management ------------------------------------------------------

  // Find the full observed MeshNode from nodeId.
  const getNode = useCallback((nodeId: string): MeshNode | undefined => {
    return nodesRef.current.get(nodeId);
  }, []);

  // Fetch neighbour links for the selected node's detail panel
  useEffect(() => {
    if (!selectedNodeId) { setPopupLinks(null); return; }
    const node = getNode(selectedNodeId);
    if (!node) return;
    let cancelled = false;
    setPopupLinks(null);
    void fetchNodeLinks(selectedNodeId, requestScopeKey, requestScope)
      .then((rows) => { if (!cancelled) setPopupLinks(rows); })
      .catch(() => { if (!cancelled) setPopupLinks([]); });
    return () => { cancelled = true; };
  }, [getNode, requestScope, requestScopeKey, selectedNodeId]);

  // Highlight the selected node on the map (bright recolour + ring). Cleared
  // when no selection. Uses setFilter so we never rebuild the whole node source.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current || !map.getLayer('node-dots-selected')) return;
    const filter: maplibregl.FilterSpecification = ['==', ['get', 'node_id'], selectedNodeId ?? '__none__'];
    map.setFilter('node-dots-selected-halo', filter);
    map.setFilter('node-dots-selected', filter);
  }, [selectedNodeId, popupVersion]);

  // Show/update/close the planned-repeater popup when its state changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current) return;

    if (!plannedPopupState) {
      plannedPopupRef.current?.remove();
      return;
    }

    if (!plannedPopupRef.current) {
      plannedPopupRef.current = new maplibregl.Popup({ maxWidth: '280px', closeOnClick: false })
        .setDOMContent(plannedPopupContainerRef.current)
        .on('close', () => setPlannedPopupState(null));
    }

    plannedPopupRef.current.setLngLat(plannedPopupState.lngLat).addTo(map);
  }, [plannedPopupState]);

  // The currently open planned repeater (re-derived as plans/coverage update)
  const plannedPopupRepeater = useMemo(
    () => viewshedEnabled ? plannedRepeaters.find((r) => r.id === plannedPopupState?.planId) ?? null : null,
    [viewshedEnabled, plannedRepeaters, plannedPopupState],
  );

  // Auto-close the planned popup if its repeater is gone
  useEffect(() => {
    if (plannedPopupState && !plannedPopupRepeater) setPlannedPopupState(null);
  }, [plannedPopupState, plannedPopupRepeater]);

  // Resolve popup props from current nodes map
  const popupNodeProps = useMemo((): PopupNodeView | null => {
    if (!selectedNodeId) return null;
    const node = getNode(selectedNodeId);
    if (!node || !hasCoords(node)) return null;
    const now = Date.now();
    const ageMs = now - new Date(node.last_seen).getTime();
    const masked = maskNodePoint(node as MeshNode & { lat: number; lon: number }, hiddenCoordMaskRef.current);
    return {
      props: {
        node_id: node.node_id,
        name: node.name ?? null,
        role: node.role ?? 2,
        is_online: node.is_online,
        is_stale: ageMs > NODE_STALE_AFTER_MS,
        is_prohibited: isProhibitedMapNode(node),
        replay_active: replayNodeIdsRef.current?.has(node.node_id.toLowerCase()) ?? false,
        replay_mode: replayNodeIdsRef.current !== null,
        hex_clash_state: null,
        visible: true,
        last_seen: node.last_seen,
        public_key: node.public_key ?? null,
        advert_count: node.advert_count ?? null,
        elevation_m: node.elevation_m ?? null,
        hardware_model: node.hardware_model ?? null,
      },
      maskedLat: masked[0],
      maskedLon: masked[1],
    };
  }, [selectedNodeId, popupVersion, getNode]);

  const popupSamePrefixCount = useMemo(() => {
    if (!selectedNodeId) return 1;
    const prefix = selectedNodeId.slice(0, 2).toUpperCase();
    return Array.from(nodesRef.current.values()).filter(
      (node) => hasCoords(node)
        && (node.role === undefined || node.role === 2)
        && node.node_id.slice(0, 2).toUpperCase() === prefix,
    ).length || 1;
  }, [selectedNodeId, popupVersion]);

  // -- Render ----------------------------------------------------------------

  return (
    <div className="map-area" style={{ position: 'relative', width: '100%', height: '100%' }}>
      {showMapChrome && <NodeSearch map={mapRef.current} onNodeSelect={onNodeSelect} />}
      {showMapChrome && <NodeLegend mapLight={mapLight} />}
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />

      {/* Map tool buttons */}
      {showMapChrome && <div className="map-tools">
        <button
          type="button"
          className={`map-tools__btn${customLosMode ? ' map-tools__btn--active' : ''}`}
          onClick={(e) => { e.stopPropagation(); if (customLosMode) clearCustomLos(); else { setPlanRepeaterMode(false); setCustomLosMode(true); } }}
          onMouseDown={(e) => e.stopPropagation()}
          onMouseUp={(e) => e.stopPropagation()}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="2" x2="12" y2="6"/><line x1="12" y1="18" x2="12" y2="22"/><line x1="2" y1="12" x2="6" y2="12"/><line x1="18" y1="12" x2="22" y2="12"/></svg>
          LOS
        </button>
        {viewshedEnabled && (
          <button
            type="button"
            className={`map-tools__btn${planRepeaterMode ? ' map-tools__btn--active' : ''}`}
            onClick={(e) => {
              e.stopPropagation();
              if (planRepeaterMode) { setPlanRepeaterMode(false); } else { clearCustomLos(); setPlanRepeaterMode(true); }
            }}
            onMouseDown={(e) => e.stopPropagation()}
            onMouseUp={(e) => e.stopPropagation()}
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            Repeater
          </button>
        )}
      </div>}

      {/* Custom LOS status hint */}
      {showMapChrome && customLosMode && (
        <div
          style={{
            position: 'absolute', bottom: 40, left: '50%', transform: 'translateX(-50%)',
            background: 'rgba(0,0,0,0.75)', color: '#fff', padding: '6px 14px',
            borderRadius: 4, fontSize: 12, pointerEvents: 'none', zIndex: 10,
            whiteSpace: 'nowrap',
          }}
        >
          {customLosStart
            ? 'Click map or repeater to set end point — Esc to cancel'
            : 'Click map or repeater to set start point — Esc to cancel'}
        </div>
      )}

      {/* Plan repeater mode hint */}
      {showMapChrome && viewshedEnabled && planRepeaterMode && (
        <div
          style={{
            position: 'absolute', bottom: 40, left: '50%', transform: 'translateX(-50%)',
            background: 'rgba(0,0,0,0.75)', color: '#22d3ee', padding: '6px 14px',
            borderRadius: 4, fontSize: 12, pointerEvents: 'none', zIndex: 10,
            whiteSpace: 'nowrap',
          }}
        >
          {plannedRepeaters.length >= 5
            ? 'Max 5 repeaters placed — click a pin for details — Esc to cancel'
            : 'Click map to place a planned repeater — click a pin for details — Esc to cancel'}
        </div>
      )}

      {/* Computing coverage indicator */}
      {showMapChrome && viewshedEnabled && plannedRepeaters.some((r) => r.status === 'queued') && (
        <div
          style={{
            position: 'absolute', top: 10, right: 10, zIndex: 10,
            background: 'rgba(0,0,0,0.75)', color: '#22d3ee', padding: '4px 10px',
            borderRadius: 4, fontSize: 11, pointerEvents: 'none',
          }}
        >
          Computing planned coverage…
        </div>
      )}

      {/* Node detail — docked panel on the right (replaces the old floating popup) */}
      {selectedNodeId && popupNodeProps && (() => {
        const nodeName = popupNodeProps.props.is_prohibited
          ? 'Redacted node'
          : (popupNodeProps.props.name ?? `Node ${selectedNodeId.slice(0, 8)}`);
        const statusLabel = popupNodeProps.props.is_stale
          ? 'STALE' : popupNodeProps.props.is_online ? 'ONLINE' : 'OFFLINE';
        const statusMod = popupNodeProps.props.is_stale
          ? 'stale' : popupNodeProps.props.is_online ? 'online' : 'offline';
        const observations = (popupLinks ?? []).reduce(
          (sum, lk) => sum + Number(lk.count_this_to_peer || 0) + Number(lk.count_peer_to_this || 0), 0);
        const watched = watchlist.isWatched('node', selectedNodeId);
        const copyLink = () => {
          const done = (label: string) => {
            setCopyLinkLabel(label);
            window.setTimeout(() => setCopyLinkLabel('Copy link'), 1600);
          };
          try {
            void navigator.clipboard.writeText(window.location.href).then(
              () => done('Copied!'), () => done('Copy failed'));
          } catch { done('Copy failed'); }
        };
        return (
          <aside
            className="node-dock"
            aria-label="Node details"
            role="dialog"
            tabIndex={-1}
            ref={dockRef}
          >
            <header className="node-dock__header">
              <div className="node-dock__heading">
                <span className={`node-dock__status node-dock__status--${statusMod}`}>{statusLabel}</span>
                <h2 className="node-dock__name" title={nodeName}>{nodeName}</h2>
              </div>
              <button
                type="button"
                className="node-dock__close"
                aria-label="Close node details"
                onClick={() => onNodeSelectRef.current?.(null)}
              >×</button>
            </header>
            <div className="node-dock__actions">
              <button
                type="button"
                className={`node-dock__watch${watched ? ' node-dock__watch--on' : ''}`}
                onClick={() => watchlist.toggle('node', selectedNodeId, nodeName)}
              >
                {watched ? '★ Watching' : '☆ Watch node'}
              </button>
              <button
                type="button"
                className="node-dock__copy"
                onClick={copyLink}
                aria-label="Copy a link to this node"
              >
                {copyLinkLabel}
              </button>
            </div>
            {popupLinks !== null && popupLinks.length > 0 && (
              <div className="node-dock__metrics">
                <div><strong>{popupLinks.length}</strong><span>neighbours</span></div>
                <div><strong>{observations.toLocaleString()}</strong><span>observations</span></div>
              </div>
            )}
            <ActivitySparkline nodeId={selectedNodeId} />
            <div className="node-dock__body">
              <NodePopupContent
                props={popupNodeProps.props}
                lat={popupNodeProps.maskedLat}
                lon={popupNodeProps.maskedLon}
                links={popupLinks}
                hideName
                onFocusSamePrefix={handleFocusSamePrefix}
                samePrefixCount={popupSamePrefixCount}
                losActive={popupLosActive}
                losLoading={popupLosLoading}
                onToggleLos={handleToggleLos}
                rfCoverageEnabled={rfCoverageEnabled}
                rfCoverageActive={!!popupNodeProps.props.public_key
                  && selectedRfCoverageNodeKey?.toLowerCase() === popupNodeProps.props.public_key.toLowerCase()}
                rfCoverageState={getRfCoverageNodeState?.(popupNodeProps.props.public_key ?? '') ?? 'pending'}
                onShowRfCoverage={(publicKey) => onShowRfCoverage?.(publicKey)}
                onClearRfCoverage={() => onClearRfCoverage?.()}
                network={network}
                observer={observer}
                privacyGeneration={privacyGeneration}
              />
            </div>
          </aside>
        );
      })()}

      {/* Planned repeater popup rendered into its MapLibre popup DOM node via portal */}
      {viewshedEnabled && plannedPopupState && plannedPopupRepeater && createPortal(
        <PlannedRepeaterPopup
          planId={plannedPopupRepeater.id}
          lat={plannedPopupRepeater.lat}
          lon={plannedPopupRepeater.lon}
          status={plannedPopupRepeater.status}
          links={plannedPopupRepeater.coverage?.predicted_links}
          getPeerName={(peerId) => nodesRef.current.get(peerId)?.name ?? peerId.slice(0, 8)}
          onRemove={handleRemovePlannedRepeater}
          losActive={plannedPopupLosActive}
          losLoading={plannedPopupLosLoading}
          onToggleLos={handleTogglePlannedLos}
        />,
        plannedPopupContainerRef.current,
      )}

      {/* Focus mode indicator */}
      {focusedPrefix && (
        <div
          style={{
            position: 'absolute', top: 10, left: '50%', transform: 'translateX(-50%)',
            background: 'rgba(0,0,0,0.75)', color: '#fff', padding: '4px 10px',
            borderRadius: 4, fontSize: 12, pointerEvents: 'none', zIndex: 10,
          }}
        >
          Showing {focusedPrefix}xx prefix nodes
        </div>
      )}
    </div>
  );
}
