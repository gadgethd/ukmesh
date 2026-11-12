import './basemap-labels.css';
import { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import * as maplibregl from './maplibre.js';
import type { MeshNode } from '../../types/domain.js';
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
  MAP_INITIAL_LOAD_TIMEOUT_MS,
  NODE_STALE_AFTER_MS,
  OPENFREEMAP_STYLE_DARK,
  OPENFREEMAP_STYLE_LIGHT,
  TERRAIN_DEM_SOURCE,
} from './mapConfig.js';
import { TERRAIN_CONFIG } from '../../config/terrain.js';
import type { CustomLosPoint } from '../../types/customLos.js';
import { NodeDock } from './NodeDock.js';
import { fetchNodeLinks, activateNodeLinksScope } from '../../features/map/nodeLinks.js';
import { useNodeLos } from '../../hooks/useNodeLos.js';
import { NodeLegend } from './NodeLegend.js';
import { useLegacyPlanner } from './useLegacyPlanner.js';
import type { MapLibreMapProps } from './types.js';
import type { NodeFeatureProps, NodeLink, PopupNodeView } from '../../features/map/model.js';
import { sampleElevationAt } from '../../utils/terrainSampler.js';
import { computeCustomLos } from '../../utils/customLos.js';
import { scopeKey } from '../../utils/scope.js';
import { createLiveMapSources } from './liveMapSources.js';
import { useCommittedRef } from '../../hooks/useCommittedRef.js';

// Keep a live DEM source across basemap changes. Removing it while a worker
// initializes can strand MapLibre 6.11's in-flight DEM actor. A fresh vector
// metadata URL still forces the failed basemap source to be replaced.
function setBasemapStyle(map: maplibregl.Map, style: string | maplibregl.StyleSpecification, revision: number): void {
  // An initial style that never loaded cannot be a diff/transform base.
  // MapLibre queues transforms until style.load, which would strand fallback.
  if (!map.getStyle()) {
    map.setStyle(style, { diff: false });
    return;
  }
  map.setStyle(style, {
    diff: true,
    transformStyle: (previous, next) => {
      const sources = { ...next.sources };
      const basemap = sources[BASEMAP_VECTOR_SOURCE_ID];
      if (basemap?.type === 'vector' && basemap.url) {
        sources[BASEMAP_VECTOR_SOURCE_ID] = { ...basemap, url: refreshBasemapMetadataUrl(basemap.url, revision) };
      }
      const terrain = previous?.sources['terrain-dem'];
      const layers = [...next.layers];
      if (terrain?.type === 'raster-dem') {
        sources['terrain-dem'] = terrain;
        const hillshade = previous?.layers.find(layer => layer.id === 'hillshade');
        if (hillshade && !layers.some(layer => layer.id === hillshade.id)) {
          const firstSymbol = layers.findIndex(layer => layer.type === 'symbol');
          layers.splice(firstSymbol < 0 ? layers.length : firstSymbol, 0, hillshade);
        }
      }
      return { ...next, sources, layers, ...(terrain ? { terrain: previous?.terrain } : {}) };
    },
  });
}

import {
  applyMapOverlayTheme,
  installMapSourcesAndLayers,
} from './mapSourceLayers.js';
import {
  BASEMAP_RETRY_TIMEOUT_MS,
  BASEMAP_SOURCE_LOAD_TIMEOUT_MS,
  BASEMAP_VECTOR_SOURCE_ID,
  createBasemapHealth,
  decideBasemapAction,
  isSuccessfulBasemapTile,
  noteFallback,
  noteRetryStarted,
  noteSourceData,
  noteStyleLoaded,
  basemapWatchdogDelay,
  noteTileError,
  refreshBasemapMetadataUrl,
  type BasemapPhase,
} from './basemapHealth.js';
import { buildFallbackStyle, FALLBACK_RASTER_SOURCE_ID, installFallbackMarkers, loadFallbackCities } from './basemapFallback.js';
import { applyBasemapLabelTheme, installUkPlaceLabels } from './basemapLabels.js';

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
  region,
  privacyGeneration,
  showMapChrome = true,
}: MapLibreMapProps) {
  const requestScope = useMemo(() => ({ network, observer, region }), [network, observer, region]);
  const publicScope = { network, observer, region, privacyGeneration };
  const requestScopeKey = scopeKey(publicScope);
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const mapLoadedRef = useRef(false);
  const [mapError, setMapError] = useState<string | null>(null);
  const [mapAttempt, setMapAttempt] = useState(0);
  const showLinksRef = useCommittedRef(showLinks);
  const showTerrainRef = useCommittedRef(showTerrain);
  const viewshedEnabledRef = useCommittedRef(viewshedEnabled);
  const mapLightRef = useCommittedRef(mapLight);
  const styleUrlRef = useRef(mapLight ? OPENFREEMAP_STYLE_LIGHT : OPENFREEMAP_STYLE_DARK);
  const hasLoadedStyleRef = useRef(false);
  const setClashPathLines = useOverlayStore((state) => state.setClashPathLines);
  const selectedNodeIdRef = useCommittedRef<string | null>(selectedNodeId);
  const onNodeSelectRef = useCommittedRef(onNodeSelect);

  const customLosNodeClickedRef = useRef(false);
  const [popupLinks, setPopupLinks] = useState<NodeLink[] | null>(null);
  const [focusedPrefix, setFocusedPrefix] = useState<string | null>(null);
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  const [focusedPrefixNodeIds, setFocusedPrefixNodeIds] = useState<Set<string> | null>(null);
  const [popupVersion, setPopupVersion] = useState(0);
  const focusTimerRef = useRef<number | null>(null);

  const sourceView = useCommittedRef({ showLinks, showClients: showClientNodes, showClashes: showHexClashes,
    maxClashHops: maxHexClashHops, light: mapLight, focusedNodeId, focusedPrefixNodeIds });
  const [liveSources] = useState(() => createLiveMapSources({
    getMap: () => mapLoadedRef.current ? mapRef.current : null,
    getView: () => sourceView.current,
    onSelectedNodeChange: () => { if (selectedNodeIdRef.current) setPopupVersion(value => value + 1); },
    refreshPlannedLinks: () => updatePlannedLinks(),
  }));
  const refreshMapSources = liveSources.refresh;
  const scheduleRefresh = liveSources.schedule;
  useEffect(() => liveSources.start(), [liveSources]);

  const basemapHealthRef = useRef(createBasemapHealth());
  const basemapModeRef = useRef<'remote' | 'fallback'>('remote');
  const basemapTimerRef = useRef<number | null>(null);
  const basemapNonceRef = useRef(Date.now());
  const basemapPhaseRef = useRef<BasemapPhase>('loading');
  const basemapRetryRef = useRef<() => void>(() => {});
  const basemapStartRef = useRef<() => void>(() => {});
  const fallbackMarkersCleanupRef = useRef<(() => void) | null>(null);
  const mapReadyNotifiedRef = useRef(false);
  const [basemapPhase, setBasemapPhase] = useState<BasemapPhase>('loading');

  // MapLibre's stylesheet is loaded only when the map component mounts, keeping
  // map-specific CSS out of non-map entry points.
  useEffect(() => {
    void import('maplibre-gl/dist/maplibre-gl.css');
  }, []);



  const popupLosActive = useOverlayStore(state => selectedNodeId != null && state.losNodeIds.has(selectedNodeId));
  const popupLosLoading = useOverlayStore(state => selectedNodeId != null && state.losLoadingIds.has(selectedNodeId));
  const handleToggleLos = useNodeLos(publicScope);
  useEffect(() => activateNodeLinksScope(publicScope), [requestScopeKey]);


  const customLosMode = useOverlayStore((state) => state.customLosMode);
  const customLosStart = useOverlayStore((state) => state.customLosStart);
  const setCustomLosMode = useOverlayStore((state) => state.setCustomLosMode);
  const setCustomLosStart = useOverlayStore((state) => state.setCustomLosStart);
  const setCustomLosResult = useOverlayStore((state) => state.setCustomLosResult);
  const clearCustomLos = useOverlayStore((state) => state.clearCustomLos);
  const planRepeaterMode = useOverlayStore((state) => state.planRepeaterMode);
  const plannedRepeaters = useOverlayStore((state) => state.plannedRepeaters);
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

  const handleCustomLosPointRef = useCommittedRef(handleCustomLosPoint);

  const legacy = useLegacyPlanner({ enabled: viewshedEnabled,
    getMap: () => mapLoadedRef.current ? mapRef.current : null,
    getNodes: () => liveSources.getNodes(), getMask: () => liveSources.getMask(),
    isLight: () => mapLightRef.current, linksVisible: () => showLinksRef.current,
    onSelectNode: nodeId => onNodeSelectRef.current?.(nodeId),
  });
  const updatePlannedLinks = legacy.updateLinks;

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


  const clearFocusTimer = useCallback(() => {
    if (focusTimerRef.current !== null) {
      window.clearTimeout(focusTimerRef.current);
      focusTimerRef.current = null;
    }
  }, []);

  const restoreTerrain = useCallback((map: maplibregl.Map, animatePitch = false) => {
    if (!showTerrainRef.current) return;
    try {
      if (!map.getSource('terrain-dem')) {
        map.addSource('terrain-dem', TERRAIN_DEM_SOURCE);
        const src = map.getSource('terrain-dem') as maplibregl.RasterDEMTileSource | undefined;
        src?.on('error', (e: maplibregl.ErrorEvent) => console.error('[terrain] source error:', e));
      }
      if (!map.getLayer('hillshade')) {
        const hillshadeLayer = {
          id: 'hillshade', type: 'hillshade' as const, source: 'terrain-dem', minzoom: 7,
          paint: {
            'hillshade-exaggeration': 0.7,
            'hillshade-shadow-color': '#000000',
            'hillshade-highlight-color': '#ffffff',
            'hillshade-illumination-anchor': 'viewport' as const,
          },
        };
        if (map.getLayer('node-dots')) map.addLayer(hillshadeLayer, 'node-dots');
        else map.addLayer(hillshadeLayer);
      }
      map.setSky({ 'atmosphere-blend': 0.5 });
      map.setMaxPitch(85);
      map.setTerrain(TERRAIN_CONFIG);
      if (animatePitch) map.easeTo({ pitch: 45, duration: 600 });
    } catch (err) {
      console.error('[terrain] restore failed:', err);
    }
  }, []);

  const rehydrateMapStyle = useCallback((map: maplibregl.Map, animateTerrain = false) => {
    mapLoadedRef.current = true;
    installMapSourcesAndLayers(map, {
      showLinks: showLinksRef.current,
      mapLight: mapLightRef.current,
    });
    applyMapOverlayTheme(map, mapLightRef.current ? 'light' : 'dark');
    if (basemapModeRef.current !== 'fallback') {
      applyBasemapLabelTheme(map, mapLightRef.current ? 'light' : 'dark');
      void installUkPlaceLabels(map, mapLightRef.current ? 'light' : 'dark');
    }

    // setStyle removes all application sources and layers, so force every
    // source to refresh from the current refs after installing the new style.
    liveSources.reset();

    legacy.rehydrate(map);

    const selectedFilter: maplibregl.FilterSpecification = [
      '==', ['get', 'node_id'], selectedNodeIdRef.current ?? '__none__',
    ];
    if (map.getLayer('node-dots-selected-halo')) map.setFilter('node-dots-selected-halo', selectedFilter);
    if (map.getLayer('node-dots-selected')) map.setFilter('node-dots-selected', selectedFilter);

    restoreTerrain(map, animateTerrain);
  }, [refreshMapSources, restoreTerrain]);

  useEffect(() => {
    scheduleRefresh();
  }, [viewshedEnabled, scheduleRefresh]);

  const handleFocusSamePrefix = useCallback((nodeId: string) => {
    const prefix = nodeId.slice(0, 2).toUpperCase();
    const ids = Array.from(liveSources.getNodes().values())
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
    liveSources.cancel();
  }, [clearFocusTimer]);


  useEffect(() => {
    if (!containerRef.current) return;
    setMapError(null);
    hasLoadedStyleRef.current = false;
    styleUrlRef.current = mapLight ? OPENFREEMAP_STYLE_LIGHT : OPENFREEMAP_STYLE_DARK;
    basemapHealthRef.current = noteStyleLoaded(createBasemapHealth(), Date.now());

    let map: maplibregl.Map;
    try {
      map = new maplibregl.Map({
        container: containerRef.current,
        style: mapLight ? OPENFREEMAP_STYLE_LIGHT : OPENFREEMAP_STYLE_DARK,
        center: initialView ? [initialView.lon, initialView.lat] : [DEFAULT_CENTER[1], DEFAULT_CENTER[0]],
        zoom: initialView?.zoom ?? DEFAULT_ZOOM,
        maxPitch: 0,
        minZoom: 6,
        transformRequest: (url) => ({ url: refreshBasemapMetadataUrl(url, basemapNonceRef.current) }),
      });
    } catch (error) {
      console.error('[map] initialization failed:', error);
      setMapError('The map could not start. Check that WebGL is enabled in your browser, then retry.');
      return;
    }

    mapRef.current = map;
    let loadTimer: number | undefined;
    let waitingForLoad = true;
    const watchLoading = () => {
      waitingForLoad = true;
      window.clearTimeout(loadTimer);
      loadTimer = window.setTimeout(() => {
        // A loaded style with a retrying basemap is handled by the basemap chip;
        // the blocking modal is for a map that cannot load at all.
        if (hasLoadedStyleRef.current && basemapPhaseRef.current === 'retrying') return;
        setMapError('The map is taking too long to load. Check your connection, then retry.');
      }, MAP_INITIAL_LOAD_TIMEOUT_MS);
    };
    const handleIdle = () => {
      if (!map.loaded()) return;
      waitingForLoad = false;
      window.clearTimeout(loadTimer);
      // While the bundled fallback stands in for a basemap that never loaded,
      // keep the blocking notice (and its Retry) visible.
      if (basemapModeRef.current !== 'fallback') setMapError(null);
    };
    const handleContextLost = () => {
      setMapError('The browser lost the map graphics connection. Retry to reload the map.');
    };

    let basemapWatchdogHandle: number | null = null;
    let interactionsInstalled = false;
    let fallbackRasterLoaded = false;
    const loggedBasemapErrors = new Set<string>();

    const clearBasemapWatchdog = () => {
      if (basemapWatchdogHandle !== null) {
        window.clearTimeout(basemapWatchdogHandle);
        basemapWatchdogHandle = null;
      }
      basemapTimerRef.current = null;
    };

    const publishBasemapPhase = (phase: BasemapPhase) => {
      if (basemapPhaseRef.current === phase) return;
      basemapPhaseRef.current = phase;
      setBasemapPhase(phase);
    };

    const remoteStyleUrl = () => (
      mapLightRef.current ? OPENFREEMAP_STYLE_LIGHT : OPENFREEMAP_STYLE_DARK
    );
    /** Fetch a style past any cached copy so a poisoned style cannot win twice. */
    const styleWithNonce = (url: string) => (
      `${url}${url.includes('?') ? '&' : '?'}v=${basemapNonceRef.current}`
    );

    const notifyMapReady = () => {
      if (mapReadyNotifiedRef.current) return;
      mapReadyNotifiedRef.current = true;
      // The node detail dock is an overlay, so selection must never reserve
      // camera padding or shift the visible map.
      map.setPadding({ top: 0, right: 0, bottom: 0, left: 0 });
      onMapReady?.(map);
    };

    const refreshFallbackMarkers = () => {
      fallbackMarkersCleanupRef.current?.();
      fallbackMarkersCleanupRef.current = null;
      void loadFallbackCities()
        .then((cities) => {
          if (mapRef.current !== map || basemapModeRef.current !== 'fallback' || fallbackRasterLoaded) return;
          fallbackMarkersCleanupRef.current = installFallbackMarkers(map, cities);
        })
        .catch(() => {
          // The outline still renders without labels.
        });
    };

    function applyFallbackBasemap(reason: string) {
      if (basemapModeRef.current === 'fallback') return;
      basemapModeRef.current = 'fallback';
      basemapHealthRef.current = noteFallback(basemapHealthRef.current, reason);
      publishBasemapPhase('fallback');
      clearBasemapWatchdog();
      console.warn('[basemap] using bundled fallback:', reason);
      try {
        setBasemapStyle(map, buildFallbackStyle(mapLightRef.current ? 'light' : 'dark'), basemapNonceRef.current);
      } catch (error) {
        console.error('[basemap] fallback style failed:', error);
      }
    }

    function armBasemapWatchdog(timeoutMs: number) {
      clearBasemapWatchdog();
      basemapWatchdogHandle = window.setTimeout(() => {
        basemapWatchdogHandle = null;
        basemapTimerRef.current = null;
        if (mapRef.current !== map) return;
        if (basemapModeRef.current === 'fallback') return;
        if (basemapPhaseRef.current === 'healthy') return;
        // A style with no OpenFreeMap vector source (Regions/custom styles) has
        // no remote basemap to heal.
        if (hasLoadedStyleRef.current && !map.getSource(BASEMAP_VECTOR_SOURCE_ID)) {
          publishBasemapPhase('healthy');
          return;
        }
        recoverBasemap(`no basemap source data within ${timeoutMs}ms`);
      }, timeoutMs);
    }

    function recoverBasemap(reason: string) {
      const decision = decideBasemapAction(basemapHealthRef.current, Date.now());
      if (decision === 'none') {
        // A timer may fire just before the wall-clock deadline, or style.load
        // may have advanced it. Keep checking the remaining absolute budget.
        const remaining = basemapWatchdogDelay(basemapHealthRef.current, Date.now());
        if (remaining !== null) armBasemapWatchdog(remaining);
        return;
      }
      if (decision === 'fallback') {
        applyFallbackBasemap(reason);
        return;
      }
      basemapHealthRef.current = noteRetryStarted(basemapHealthRef.current, Date.now());
      basemapNonceRef.current += 1;
      publishBasemapPhase('retrying');
      console.warn('[basemap] retrying with cache-busted style:', reason);
      try {
        mapLoadedRef.current = false;
        setBasemapStyle(map, styleWithNonce(remoteStyleUrl()), basemapNonceRef.current);
      } catch {
        applyFallbackBasemap(reason);
        return;
      }
      armBasemapWatchdog(BASEMAP_RETRY_TIMEOUT_MS);
    }

    // Manual retry from the map chip: fresh cache-busted attempt, fall back
    // again if it still cannot produce source data.
    basemapRetryRef.current = () => {
      basemapModeRef.current = 'remote';
      fallbackMarkersCleanupRef.current?.();
      fallbackMarkersCleanupRef.current = null;
      basemapNonceRef.current += 1;
      const now = Date.now();
      basemapHealthRef.current = noteRetryStarted(
        { ...createBasemapHealth(), styleLoadedAt: now },
        now,
      );
      publishBasemapPhase('retrying');
      try {
        styleUrlRef.current = remoteStyleUrl();
        mapLoadedRef.current = false;
        setBasemapStyle(map, styleWithNonce(remoteStyleUrl()), basemapNonceRef.current);
      } catch {
        applyFallbackBasemap('manual retry failed');
        return;
      }
      armBasemapWatchdog(BASEMAP_RETRY_TIMEOUT_MS);
    };

    const handleSourceData = (rawEvent: unknown) => {
      const event = rawEvent as { sourceId?: string; tile?: { state?: string } };
      if (basemapModeRef.current === 'fallback') {
        if (event.sourceId === FALLBACK_RASTER_SOURCE_ID && event.tile?.state === 'loaded') {
          fallbackRasterLoaded = true;
          fallbackMarkersCleanupRef.current?.();
          fallbackMarkersCleanupRef.current = null;
        }
        return;
      }
      if (!isSuccessfulBasemapTile(event)) return;
      basemapHealthRef.current = noteSourceData(
        basemapHealthRef.current, BASEMAP_VECTOR_SOURCE_ID, true, Date.now(),
      );
      clearBasemapWatchdog();
      publishBasemapPhase('healthy');
    };

    const handleError = (event: maplibregl.ErrorEvent) => {
      const error = (event as unknown as { error?: Error }).error;
      const message = String(error?.message ?? error ?? 'map error');
      // Terrain DEM gaps are expected at the edge of the tile set and are not a
      // basemap failure (the telemetry noise filter already ignores them).
      if (message.includes('/terrain-tiles/')) return;
      // Basemap failures are owned by the watchdog and the status chip; raising
      // the blocking modal as well would fight the automatic retry/fallback.
      const isBasemapFailure = /tile|style|sprite|glyph|worker|source|ajax|fetch|texture|webgl|context/i.test(message);
      if (isBasemapFailure && basemapModeRef.current !== 'fallback') {
        if (!loggedBasemapErrors.has(message) && loggedBasemapErrors.size < 20) {
          loggedBasemapErrors.add(message);
          console.warn('[basemap] error:', message);
        }
        basemapHealthRef.current = noteTileError(basemapHealthRef.current, Date.now(), message);
        if (decideBasemapAction(basemapHealthRef.current, Date.now()) !== 'none') {
          recoverBasemap(message);
        }
        return;
      }
      if (waitingForLoad) setMapError('The map could not load. Check your connection, then retry.');
    };

    watchLoading();
    map.on('idle', handleIdle);
    map.on('error', handleError);
    map.on('webglcontextlost', handleContextLost);
    map.on('webglcontextrestored', watchLoading);
    map.on('sourcedata', handleSourceData);

    // Start the basemap clock immediately: a style that never loads is also a
    // dead basemap, and `style.load` would never fire to arm this.
    basemapHealthRef.current = { ...createBasemapHealth(), styleLoadedAt: Date.now() };
    armBasemapWatchdog(BASEMAP_SOURCE_LOAD_TIMEOUT_MS);

    const handleStyleLoad = () => {
      // App-initiated style loads get a fresh blocking budget; a basemap retry
      // must not push the 15 s modal past the automatic fallback.
      if (basemapPhaseRef.current !== 'retrying') watchLoading();
      if (basemapModeRef.current === 'fallback') {
        fallbackRasterLoaded = false;
        mapLoadedRef.current = true;
        rehydrateMapStyle(map, false);
        installInteractionHandlers();
        refreshFallbackMarkers();
        notifyMapReady();
        return;
      }
      const targetStyle = remoteStyleUrl();
      if (styleUrlRef.current !== targetStyle) {
        styleUrlRef.current = targetStyle;
        mapLoadedRef.current = false;
        setBasemapStyle(map, styleWithNonce(targetStyle), basemapNonceRef.current);
        return;
      }
      rehydrateMapStyle(map, !hasLoadedStyleRef.current);
      hasLoadedStyleRef.current = true;
      installInteractionHandlers();
      notifyMapReady();

      basemapHealthRef.current = noteStyleLoaded(basemapHealthRef.current, Date.now());
      const basemapSource = map.getSource(BASEMAP_VECTOR_SOURCE_ID);
      if (!basemapSource) {
        // A style without the OpenFreeMap vector source (Regions/custom styles)
        // has no remote basemap to heal.
        publishBasemapPhase('healthy');
        clearBasemapWatchdog();
        return;
      }
      publishBasemapPhase(basemapPhaseRef.current === 'retrying' ? 'retrying' : 'loading');
      armBasemapWatchdog(
        basemapPhaseRef.current === 'retrying' ? BASEMAP_RETRY_TIMEOUT_MS : BASEMAP_SOURCE_LOAD_TIMEOUT_MS,
      );
    };

    function installInteractionHandlers() {
      if (interactionsInstalled) return;
      interactionsInstalled = true;

      map.on('click', 'planned-pins-dot', (e) => {
        if (!viewshedEnabledRef.current) return;
        // Click a planned pin (in any mode) to open its popup with coords,
        // predicted links, and a remove action.
        const feature = e.features?.[0];
        if (!feature) return;
        const planId = (feature.properties as { plan_id: string }).plan_id;
        const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
        customLosNodeClickedRef.current = true; // prevent general map-click from placing a new pin
        legacy.open(planId, { lng: coords[0], lat: coords[1] });
      });

      map.on('click', 'node-dots-hit', (e) => {
        const feature = e.features?.[0];
        if (!feature) return;
        const props = feature.properties as Pick<NodeFeatureProps, 'node_id' | 'is_prohibited'>;

        // In plan repeater mode, node clicks place a repeater on the node's location
        if (viewshedEnabledRef.current && useOverlayStore.getState().planRepeaterMode) {
          customLosNodeClickedRef.current = true;
          if (legacy.canPlace()) {
            const node = liveSources.getNodes().get(props.node_id);
            if (node && hasCoords(node)) {
              void legacy.place(node.lat, node.lon);
            }
          }
          return;
        }

        // In custom LOS mode, intercept node clicks as point picks
        if (useOverlayStore.getState().customLosMode) {
          customLosNodeClickedRef.current = true; // always consume to prevent map-click firing
          if (!props.is_prohibited) {
            const node = liveSources.getNodes().get(props.node_id);
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
          if (legacy.canPlace()) {
            void legacy.place(lat, lng);
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
    }

    map.on('style.load', handleStyleLoad);

    basemapStartRef.current = () => {
      basemapHealthRef.current = noteStyleLoaded(createBasemapHealth(), Date.now());
      publishBasemapPhase('loading');
      armBasemapWatchdog(BASEMAP_SOURCE_LOAD_TIMEOUT_MS);
    };

    map.on('sourcedataloading', (event) => {
      if (event.sourceId !== BASEMAP_VECTOR_SOURCE_ID || !event.tile) return;
      if (basemapHealthRef.current.phase !== 'healthy') return;
      // A new viewport needs new tiles, even after an earlier viewport worked.
      basemapHealthRef.current = noteStyleLoaded(basemapHealthRef.current, Date.now());
      publishBasemapPhase('loading');
      armBasemapWatchdog(BASEMAP_SOURCE_LOAD_TIMEOUT_MS);
    });


    return () => {
      window.clearTimeout(loadTimer);
      map.off('idle', handleIdle);
      map.off('error', handleError);
      map.off('sourcedata', handleSourceData);
      map.off('webglcontextlost', handleContextLost);
      map.off('webglcontextrestored', watchLoading);
      mapLoadedRef.current = false;
      hasLoadedStyleRef.current = false;
      mapReadyNotifiedRef.current = false;
      liveSources.cancel();
      clearBasemapWatchdog();
      fallbackMarkersCleanupRef.current?.();
      fallbackMarkersCleanupRef.current = null;
      basemapModeRef.current = 'remote';
      basemapHealthRef.current = createBasemapHealth();
      basemapPhaseRef.current = 'loading';
      basemapRetryRef.current = () => {};
      // A remount while the fallback was showing must not leave the chip up.
      setBasemapPhase('loading');
      basemapStartRef.current = () => {};
      setClashPathLines([]);
      map.off('style.load', handleStyleLoad);
      map.remove();
      mapRef.current = null;
    };

  // Initial theme is captured on mount; subsequent theme changes are applied by
  // the dedicated theme effect without rebuilding the MapLibre instance.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapAttempt, initialView, onMapReady, onNodeSelect, refreshMapSources, rehydrateMapStyle, setClashPathLines]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    if (basemapModeRef.current === 'fallback') {
      // Rebuild the bundled fallback in the new palette; style.load rehydrates
      // the application layers and the offline city labels.
      try {
        setBasemapStyle(map, buildFallbackStyle(mapLight ? 'light' : 'dark'), basemapNonceRef.current);
      } catch {
        // Ignore: the map is being torn down.
      }
      return;
    }
    const targetStyle = mapLight ? OPENFREEMAP_STYLE_LIGHT : OPENFREEMAP_STYLE_DARK;
    if (!mapLoadedRef.current || styleUrlRef.current === targetStyle) return;

    styleUrlRef.current = targetStyle;
    mapLoadedRef.current = false;
    basemapStartRef.current();
    setBasemapStyle(map, targetStyle, basemapNonceRef.current);
  }, [mapLight]);


  useEffect(() => {
    updatePlannedLinks();
    scheduleRefresh({ nodes: true, links: true, plannedLinks: true, clash: true });
  }, [showLinks, scheduleRefresh, updatePlannedLinks]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current) return;
    if (showTerrain) {
      restoreTerrain(map, true);
    } else {
      map.setTerrain(null);
      if (map.getLayer('hillshade')) map.removeLayer('hillshade');
      map.setSky({});
      if (map.getSource('terrain-dem')) map.removeSource('terrain-dem');
      const finish = () => { if (!showTerrainRef.current) map.setMaxPitch(0); };
      map.once('moveend', finish);
      map.easeTo({ pitch: 0, duration: 400 });
      return () => { map.off('moveend', finish); };
    }
  }, [restoreTerrain, showTerrain]);

  useEffect(() => {
    scheduleRefresh({ nodes: true });
  }, [showClientNodes, scheduleRefresh]);

  useEffect(() => {
    scheduleRefresh({ nodes: true, clash: true });
  }, [showHexClashes, scheduleRefresh]);

  useEffect(() => {
    scheduleRefresh({ nodes: true, clash: true });
  }, [maxHexClashHops, scheduleRefresh]);


  useEffect(() => {
    scheduleRefresh({ nodes: true, clash: true });
  }, [focusedNodeId, focusedPrefixNodeIds, scheduleRefresh]);


  const getNode = useCallback((nodeId: string): MeshNode | undefined => {
    return liveSources.getNodes().get(nodeId);
  }, []);

  useEffect(() => {
    if (!selectedNodeId) { setPopupLinks(null); return; }
    const node = getNode(selectedNodeId);
    if (!node) return;
    let cancelled = false;
    setPopupLinks(null);
    void fetchNodeLinks(selectedNodeId, publicScope)
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

  // Resolve popup props from current nodes map
  const popupNodeProps = useMemo((): PopupNodeView | null => {
    if (!selectedNodeId) return null;
    const node = getNode(selectedNodeId);
    if (!node || !hasCoords(node)) return null;
    const now = Date.now();
    const ageMs = now - new Date(node.last_seen).getTime();
    const masked = maskNodePoint(node as MeshNode & { lat: number; lon: number }, liveSources.getMask());
    return {
      props: {
        node_id: node.node_id,
        name: node.name ?? null,
        role: node.role ?? 2,
        is_online: node.is_online,
        is_stale: ageMs > NODE_STALE_AFTER_MS,
        is_prohibited: isProhibitedMapNode(node),
        replay_active: liveSources.getReplayNodes()?.has(node.node_id.toLowerCase()) ?? false,
        replay_mode: liveSources.getReplayNodes() !== null,
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
    return Array.from(liveSources.getNodes().values()).filter(
      (node) => hasCoords(node)
        && (node.role === undefined || node.role === 2)
        && node.node_id.slice(0, 2).toUpperCase() === prefix,
    ).length || 1;
  }, [selectedNodeId, popupVersion]);


  return (
    <div className="map-area" data-basemap-state={basemapPhase} style={{ position: 'relative', width: '100%', height: '100%' }}>
      {showMapChrome && <NodeSearch map={mapRef.current} scope={publicScope} onNodeSelect={onNodeSelect} />}
      {showMapChrome && <NodeLegend mapLight={mapLight} />}
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {mapError && (
        <div role="alert" style={{
          position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)',
          zIndex: 3, width: 'min(360px, 85%)', padding: 16, borderRadius: 6,
          background: '#07101d', color: '#f8fafc', border: '1px solid #64748b',
        }}>
          <p style={{ margin: '0 0 12px' }}>{mapError}</p>
          <button type="button" onClick={() => setMapAttempt(attempt => attempt + 1)}>Retry map</button>
        </div>
      )}

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

      {/* Basemap health: visible only while recovering or on the offline fallback */}
      {showMapChrome && (basemapPhase === 'retrying' || basemapPhase === 'fallback') && (
        <div
          className={`map-basemap-status map-basemap-status--${basemapPhase}`}
          role="status"
          aria-live="polite"
        >
          <span className="map-basemap-status__text">
            {basemapPhase === 'retrying'
              ? 'Reloading basemap…'
              : 'Using backup basemap'}
          </span>
          {basemapPhase === 'fallback' && (
            <button
              type="button"
              className="map-basemap-status__retry"
              onClick={(event) => {
                event.stopPropagation();
                basemapRetryRef.current();
              }}
            >
              Retry
            </button>
          )}
        </div>
      )}

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

      {selectedNodeId && popupNodeProps && <NodeDock
        nodeId={selectedNodeId} view={popupNodeProps} links={popupLinks} scope={publicScope}
        picking={customLosMode || (viewshedEnabled && planRepeaterMode)} onClose={() => onNodeSelectRef.current?.(null)}
        details={{ onFocusSamePrefix: handleFocusSamePrefix, samePrefixCount: popupSamePrefixCount,
          losActive: popupLosActive, losLoading: popupLosLoading, onToggleLos: handleToggleLos,
          rfCoverageEnabled, rfCoverageActive: !!popupNodeProps.props.public_key
            && selectedRfCoverageNodeKey?.toLowerCase() === popupNodeProps.props.public_key.toLowerCase(),
          rfCoverageState: getRfCoverageNodeState?.(popupNodeProps.props.public_key ?? '') ?? 'pending',
          onShowRfCoverage: publicKey => onShowRfCoverage?.(publicKey), onClearRfCoverage: () => onClearRfCoverage?.(),
          network, observer, privacyGeneration }}
      />}

      {legacy.portal}

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
