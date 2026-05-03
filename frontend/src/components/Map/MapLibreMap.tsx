/**
 * MapLibreMap — replaces MapView (Leaflet).
 *
 * Node dots are rendered as a MapLibre GeoJSON circle layer (GPU, no React fibers).
 * Pan/zoom is pure GPU — zero JS work on move events.
 * Coverage, hex-clash lines, and privacy rings are also GeoJSON layers.
 * Click hit-testing uses MapLibre's built-in R-tree spatial index.
 * deck.gl overlays are integrated via @deck.gl/mapbox (MapboxOverlay).
 */
import { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import type { MeshNode } from '../../hooks/useNodes.js';
import { nodeStore } from '../../hooks/useNodes.js';
import { coverageStore, type NodeCoverage } from '../../hooks/useCoverage.js';
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
  MAP_REFRESH_INTERVAL_MS,
  MAP_STYLE,
  MAP_STYLE_LIGHT,
  SEVEN_DAYS_MS,
  TERRAIN_CONFIG,
  TERRAIN_DEM_SOURCE,
} from './mapConfig.js';
import {
  buildClashLinesGeoJSON,
  buildCoverageGeoJSON,
  buildHiddenMask,
  buildLinksGeoJSON,
  buildNodeGeoJSON,
  buildPlannedCoverageGeoJSON,
  buildPlannedPinGeoJSON,
  buildPrivacyRingsGeoJSON,
  computeClashData,
} from './geojsonBuilders.js';
import { NodePopupContent } from './NodePopupContent.js';
import type {
  CustomLosPoint,
  LosProfile,
  MapLibreMapProps,
  NodeFeatureProps,
  NodeLink,
  PlannedRepeater,
  PopupNodeView,
  PopupState,
} from './types.js';
import { sampleElevationAt } from '../../utils/terrainSampler.js';
import { computeCustomLos } from '../../utils/customLos.js';

// ── Main Component ────────────────────────────────────────────────────────────

export function MapLibreMap({
  inferredNodes,
  inferredActiveNodeIds: _inferredActiveNodeIds,
  showLinks,
  showTerrain,
  showClientNodes,
  showHexClashes,
  maxHexClashHops,
  onMapReady,
}: MapLibreMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const mapLoadedRef = useRef(false);
  const mlPopupRef = useRef<maplibregl.Popup | null>(null);
  const popupContainerRef = useRef<HTMLDivElement>(document.createElement('div'));
  const nodesRef = useRef(nodeStore.getState().nodes);
  const coverageRef = useRef(coverageStore.getState().coverage);
  const selectedCoverageRef = useRef<NodeCoverage | null>(null);
  const viablePairsRef = useRef(linkStateStore.getState().viablePairsArr);
  const linkMetricsRef = useRef(linkStateStore.getState().linkMetrics);
  const inferredNodesRef = useRef(inferredNodes);
  const showLinksRef = useRef(showLinks);
  const showTerrainRef = useRef(showTerrain);
  const showClientNodesRef = useRef(showClientNodes);
  const showHexClashesRef = useRef(showHexClashes);
  const maxHexClashHopsRef = useRef(maxHexClashHops);
  const pathNodeIdsRef = useRef(useOverlayStore.getState().pathNodeIds);
  const hiddenCoordMaskRef = useRef<Map<string, HiddenMaskGeometry>>(new Map());
  const refreshTimerRef = useRef<number | null>(null);
  const popupStateRef = useRef<PopupState | null>(null);

  const customLosNodeClickedRef = useRef(false);
  // handleCustomLosPointRef is assigned after handleCustomLosPoint is defined below
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleCustomLosPointRef = useRef<(point: CustomLosPoint) => Promise<void>>(null as any);
  // Planned repeater placement
  const plannedRepeatersRef = useRef<PlannedRepeater[]>([]);
  const plannedPollRefs = useRef<Map<string, number>>(new Map());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleRemovePlannedRepeaterRef = useRef<(planId: string) => void>(null as any);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placePlannedRepeaterRef = useRef<(lat: number, lon: number) => Promise<void>>(null as any);

  const [popupState, setPopupState] = useState<PopupState | null>(null);
  const [popupLinks, setPopupLinks] = useState<NodeLink[] | null>(null);
  const [selectedCoverageNodeId, setSelectedCoverageNodeId] = useState<string | null>(null);
  const [coverageLoadingNodeId, setCoverageLoadingNodeId] = useState<string | null>(null);
  const [coverageMessage, setCoverageMessage] = useState<string | null>(null);
  const [focusedPrefix, setFocusedPrefix] = useState<string | null>(null);
  const [focusedNodeId, setFocusedNodeId] = useState<string | null>(null);
  const [focusedPrefixNodeIds, setFocusedPrefixNodeIds] = useState<Set<string> | null>(null);
  const [popupVersion, setPopupVersion] = useState(0);
  const focusTimerRef = useRef<number | null>(null);

  // -- Map theme (light/dark) -------------------------------------------------
  const [mapLight, setMapLight] = useState(() => localStorage.getItem('map-theme') === 'light');

  const toggleMapTheme = useCallback(() => {
    setMapLight((prev) => {
      const next = !prev;
      localStorage.setItem('map-theme', next ? 'light' : 'dark');
      const map = mapRef.current;
      if (map && mapLoadedRef.current) {
        const oldId = next ? 'carto-dark' : 'carto-light';
        const newId = next ? 'carto-light' : 'carto-dark';
        const variant = next ? 'light_all' : 'dark_all';
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
          { id: 'bg-fill', type: 'background', paint: { 'background-color': next ? '#e8e8e8' : '#080d14' } },
          firstLayer,
        );
        map.addLayer(
          { id: 'background', type: 'raster', source: newId },
          map.getStyle().layers[1]?.id,  // after bg-fill, before everything else
        );
      }
      return next;
    });
  }, []);

  // -- LOS profiles (client-side, multi-node, auto-expire) -------------------

  const addLosLoading = useOverlayStore((state) => state.addLosLoading);
  const setLosProfilesForNode = useOverlayStore((state) => state.setLosProfilesForNode);
  const removeLosNode = useOverlayStore((state) => state.removeLosNode);

  // Targeted selectors — only re-render MapLibreMap when the POPUP node's LOS
  // status changes (boolean equality), not every time any node's Set changes.
  const popupNodeId = popupState?.nodeId ?? null;
  const popupLosActive = useOverlayStore((state) => popupNodeId != null && state.losNodeIds.has(popupNodeId));
  const popupLosLoading = useOverlayStore((state) => popupNodeId != null && state.losLoadingIds.has(popupNodeId));

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
      const links = await fetch(`/api/nodes/${nodeId}/links`)
        .then((r) => r.json()) as NodeLink[];
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
  }, [addLosLoading, setLosProfilesForNode, removeLosNode, clearLosTimer]);

  // -- Custom LOS (two-point terrain-sampled LOS) ----------------------------

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

  // Keep ref in sync so map event handlers always call the latest version
  useEffect(() => {
    handleCustomLosPointRef.current = handleCustomLosPoint;
  }, [handleCustomLosPoint]);

  // -- Planned repeater placement --------------------------------------------

  const pollPlannedCoverage = useCallback((planId: string) => {
    const iv = window.setInterval(() => {
      void fetch(`/api/coverage/planned/${planId}`)
        .then((r) => r.json() as Promise<{ status: string; coverage?: PlannedRepeater['coverage'] }>)
        .then((data) => {
          if (data.status === 'ready') {
            window.clearInterval(iv);
            plannedPollRefs.current.delete(planId);
            useOverlayStore.getState().updatePlannedRepeater(planId, { status: 'ready', coverage: data.coverage });
          }
        })
        .catch(() => {});
    }, 2000);
    plannedPollRefs.current.set(planId, iv);
  }, []);

  const handleRemovePlannedRepeater = useCallback((planId: string) => {
    const iv = plannedPollRefs.current.get(planId);
    if (iv !== undefined) {
      window.clearInterval(iv);
      plannedPollRefs.current.delete(planId);
    }
    useOverlayStore.getState().removePlannedRepeater(planId);
    void fetch(`/api/coverage/planned/${planId}`, { method: 'DELETE' }).catch(() => {});
  }, []);

  const placePlannedRepeater = useCallback(async (lat: number, lon: number) => {
    try {
      const res = await fetch('/api/coverage/planned', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lat, lon }),
      });
      if (!res.ok) return;
      const data = await res.json() as { plan_id: string };
      useOverlayStore.getState().addPlannedRepeater({ id: data.plan_id, lat, lon, status: 'queued' });
      pollPlannedCoverage(data.plan_id);
    } catch {
      // non-fatal
    }
  }, [pollPlannedCoverage]);

  // Keep handler refs in sync for map event handlers
  useEffect(() => {
    handleRemovePlannedRepeaterRef.current = handleRemovePlannedRepeater;
  }, [handleRemovePlannedRepeater]);

  useEffect(() => {
    placePlannedRepeaterRef.current = placePlannedRepeater;
  }, [placePlannedRepeater]);

  // Keep planned repeaters ref in sync
  useEffect(() => {
    plannedRepeatersRef.current = plannedRepeaters;
  }, [plannedRepeaters]);

  // Update planned coverage and pin layers when planned repeaters change
  useEffect(() => {
    if (!mapLoadedRef.current || !mapRef.current) return;
    (mapRef.current.getSource('planned-coverage') as maplibregl.GeoJSONSource | undefined)
      ?.setData(buildPlannedCoverageGeoJSON(plannedRepeaters));
    (mapRef.current.getSource('planned-pins') as maplibregl.GeoJSONSource | undefined)
      ?.setData(buildPlannedPinGeoJSON(plannedRepeaters));
  }, [plannedRepeaters]);

  // Clean up all planned repeaters and intervals on unmount
  useEffect(() => () => {
    for (const [planId, iv] of plannedPollRefs.current) {
      window.clearInterval(iv);
      void fetch(`/api/coverage/planned/${planId}`, { method: 'DELETE' }).catch(() => {});
    }
    plannedPollRefs.current.clear();
  }, []);

  // Cursor crosshair while in custom LOS mode or plan repeater mode
  useEffect(() => {
    const canvas = mapRef.current?.getCanvas();
    if (!canvas) return;
    canvas.style.cursor = (customLosMode || planRepeaterMode) ? 'crosshair' : '';
  }, [customLosMode, planRepeaterMode]);

  // Escape key clears custom LOS mode or plan repeater mode
  useEffect(() => {
    if (!customLosMode && !planRepeaterMode) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (customLosMode) clearCustomLos();
        if (planRepeaterMode) setPlanRepeaterMode(false);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [customLosMode, planRepeaterMode, clearCustomLos, setPlanRepeaterMode]);

  // -- Focus mode (same-prefix highlight) ------------------------------------

  const clearFocusTimer = useCallback(() => {
    if (focusTimerRef.current !== null) {
      window.clearTimeout(focusTimerRef.current);
      focusTimerRef.current = null;
    }
  }, []);

  const refreshMapSources = useCallback(() => {
    if (!mapLoadedRef.current || !mapRef.current) return;

    const nodes = nodesRef.current;
    const coverage = coverageRef.current;
    const viablePairsArr = viablePairsRef.current;
    const linkMetrics = linkMetricsRef.current;
    const currentPathNodeIds = pathNodeIdsRef.current;
    const currentHiddenCoordMask = buildHiddenMask(nodes);
    hiddenCoordMaskRef.current = currentHiddenCoordMask;

    const clash = computeClashData(
      nodes,
      coverage,
      viablePairsArr,
      linkMetrics,
      showHexClashesRef.current,
      maxHexClashHopsRef.current,
      focusedNodeId,
      focusedPrefixNodeIds,
    );

    const nodeGeoJSON = buildNodeGeoJSON(
      nodes,
      currentHiddenCoordMask,
      showClientNodesRef.current,
      showLinksRef.current,
      new Set(viablePairsArr.flatMap(([aId, bId]) => [aId.toLowerCase(), bId.toLowerCase()])),
      clash.clashOffenderNodeIds,
      clash.clashRelayIds,
      clash.clashModeActive,
      clash.clashModeActive ? null : currentPathNodeIds,
    );
    (mapRef.current.getSource('nodes') as maplibregl.GeoJSONSource | undefined)?.setData(nodeGeoJSON);

    const privacyGeoJSON = buildPrivacyRingsGeoJSON(nodes, currentHiddenCoordMask);
    (mapRef.current.getSource('privacy-rings') as maplibregl.GeoJSONSource | undefined)?.setData(privacyGeoJSON);

    const linksGeoJSON = showLinksRef.current
      ? buildLinksGeoJSON(nodes, viablePairsArr, linkMetrics, currentHiddenCoordMask)
      : EMPTY_FC;
    (mapRef.current.getSource('viable-links') as maplibregl.GeoJSONSource | undefined)?.setData(linksGeoJSON);
    mapRef.current.setLayoutProperty('viable-links-layer', 'visibility', showLinksRef.current ? 'visible' : 'none');

    const coverageGeoJSON = selectedCoverageRef.current && !clash.clashModeActive
      ? buildCoverageGeoJSON([selectedCoverageRef.current])
      : EMPTY_FC;
    (mapRef.current.getSource('coverage') as maplibregl.GeoJSONSource | undefined)?.setData(coverageGeoJSON);
    mapRef.current.setLayoutProperty('coverage-fill', 'visibility',
      selectedCoverageRef.current && !clash.clashModeActive ? 'visible' : 'none');

    const clashGeoJSON = clash.clashModeActive && clash.clashPathLines.length > 0
      ? buildClashLinesGeoJSON(clash.clashPathLines)
      : EMPTY_FC;
    (mapRef.current.getSource('clash-lines') as maplibregl.GeoJSONSource | undefined)?.setData(clashGeoJSON);
    mapRef.current.setLayoutProperty('clash-lines-layer', 'visibility',
      clash.clashModeActive && clash.clashPathLines.length > 0 ? 'visible' : 'none');
  }, [focusedNodeId, focusedPrefixNodeIds]);

  const scheduleRefresh = useCallback(() => {
    if (refreshTimerRef.current !== null) return;
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null;
      refreshMapSources();
    }, MAP_REFRESH_INTERVAL_MS);
  }, [refreshMapSources]);

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
      style: localStorage.getItem('map-theme') === 'light' ? MAP_STYLE_LIGHT : MAP_STYLE,
      center: [DEFAULT_CENTER[1], DEFAULT_CENTER[0]], // [lon, lat]
      zoom: DEFAULT_ZOOM,
      maxPitch: 0,
      minZoom: 6,
      attributionControl: false,
    });

    map.on('load', () => {
      mapLoadedRef.current = true;

      // ── Node dots source + layer ───────────────────────────────────────────
      map.addSource('nodes', { type: 'geojson', data: EMPTY_FC });

      map.addLayer({
        id: 'node-dots',
        type: 'circle',
        source: 'nodes',
        filter: ['==', ['get', 'visible'], true],
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 3, 9, 4, 11, 5, 13, 7, 16, 9,
          ],
          'circle-color': [
            'case',
            ['==', ['get', 'hex_clash_state'], 'offender'], '#ef4444',
            ['==', ['get', 'hex_clash_state'], 'relay'], '#22c55e',
            ['get', 'is_link_only_stale'], '#4b5563',
            ['get', 'is_inferred'], '#7dd3fc',
            ['get', 'is_stale'], '#6b7280',
            ['!', ['get', 'is_online']], '#6b7280',
            ['==', ['get', 'role'], 1], '#ff9f43',
            ['==', ['get', 'role'], 3], '#a78bfa',
            ['==', ['get', 'role'], 4], '#34d399',
            '#00c4ff', // repeater (role 2 / default)
          ],
          'circle-opacity': [
            'case',
            ['get', 'is_link_only_stale'], 0.22,
            ['get', 'is_stale'], 0.4,
            ['!', ['get', 'is_online']], 0.4,
            ['get', 'is_inferred'], 0.7,
            1.0,
          ],
          'circle-stroke-width': 0,
          'circle-stroke-color': '#00c4ff',
          'circle-stroke-opacity': 0.7,
        },
      });

      // ── Privacy rings source + layer ───────────────────────────────────────
      map.addSource('privacy-rings', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'privacy-rings-layer',
        type: 'line',
        source: 'privacy-rings',
        paint: {
          'line-color': '#f59e0b',
          'line-width': 1.4,
          'line-opacity': 0.55,
          'line-dasharray': [4, 6],
        },
      });

      // ── Viable links source + layer ───────────────────────────────────────
      map.addSource('viable-links', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'viable-links-layer',
        type: 'line',
        source: 'viable-links',
        layout: {
          visibility: 'none',
          'line-cap': 'round',
          'line-join': 'round',
        },
        paint: {
          'line-color': ['get', 'color'],
          'line-width': ['get', 'width'],
          'line-opacity': ['get', 'opacity'],
        },
      });

      // ── Coverage source + layer ────────────────────────────────────────────
      map.addSource('coverage', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'coverage-fill',
        type: 'fill',
        source: 'coverage',
        layout: { visibility: 'none' },
        paint: {
          'fill-color': [
            'match', ['get', 'band'],
            'green', '#22c55e',
            'amber', '#fbbf24',
            'red', '#ef4444',
            '#22c55e',
          ],
          'fill-opacity': [
            'match', ['get', 'band'],
            'green', 0.22,
            'amber', 0.16,
            'red', 0.10,
            0.18,
          ],
        },
      });

      // ── Clash lines source + layer ─────────────────────────────────────────
      map.addSource('clash-lines', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'clash-lines-layer',
        type: 'line',
        source: 'clash-lines',
        layout: { visibility: 'none' },
        paint: {
          'line-color': '#f97316',
          'line-width': 2.2,
          'line-opacity': 0.9,
        },
      });

      // ── Planned coverage source + layers ──────────────────────────────────
      map.addSource('planned-coverage', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'planned-coverage-fill',
        type: 'fill',
        source: 'planned-coverage',
        paint: {
          'fill-color': [
            'match', ['get', 'band'],
            'green', '#2dd4bf',   // teal-400
            'amber', '#818cf8',   // indigo-400
            'red',   '#c084fc',   // purple-400
            '#2dd4bf',
          ],
          'fill-opacity': [
            'match', ['get', 'band'],
            'green', 0.30,
            'amber', 0.25,
            'red',   0.20,
            0.25,
          ],
        },
      });
      map.addLayer({
        id: 'planned-coverage-outline',
        type: 'line',
        source: 'planned-coverage',
        paint: {
          'line-color': '#22d3ee', // cyan-400
          'line-width': 1.5,
          'line-opacity': 0.6,
        },
      });

      // ── Planned repeater pins source + layers ──────────────────────────────
      // Styled to match real repeater nodes (role 2, #00c4ff) but visually
      // distinct via white stroke + glow halo + "Planned" label.
      map.addSource('planned-pins', { type: 'geojson', data: EMPTY_FC });

      // Halo: soft glow behind the pin
      map.addLayer({
        id: 'planned-pins-halo',
        type: 'circle',
        source: 'planned-pins',
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 8, 9, 11, 11, 14, 13, 18, 16, 22,
          ],
          'circle-color': '#22d3ee',
          'circle-opacity': [
            'match', ['get', 'status'],
            'ready', 0.20,
            0.10,
          ],
          'circle-stroke-width': 0,
        },
      });

      // Core dot: same size/colour as a real online repeater, white stroke to mark as planned
      map.addLayer({
        id: 'planned-pins-dot',
        type: 'circle',
        source: 'planned-pins',
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 3, 9, 4, 11, 5, 13, 7, 16, 9,
          ],
          'circle-color': [
            'match', ['get', 'status'],
            'ready', '#00c4ff',   // identical to real online repeater
            '#4b5563',            // dark grey while computing
          ],
          'circle-opacity': [
            'match', ['get', 'status'],
            'ready', 1.0,
            0.6,
          ],
          'circle-stroke-color': '#ffffff',
          'circle-stroke-width': 2,
          'circle-stroke-opacity': 0.95,
        },
      });

      // Label: "Planned" below the dot, or "Computing…" while pending
      map.addLayer({
        id: 'planned-pins-label',
        type: 'symbol',
        source: 'planned-pins',
        layout: {
          'text-field': [
            'match', ['get', 'status'],
            'ready', 'Planned',
            'Computing…',
          ],
          'text-size': 10,
          'text-anchor': 'top',
          'text-offset': [0, 1.0],
          'text-allow-overlap': true,
          'text-ignore-placement': true,
        },
        paint: {
          'text-color': '#22d3ee',
          'text-halo-color': 'rgba(0,0,0,0.8)',
          'text-halo-width': 1.2,
        },
      });

      // ── Click handler ──────────────────────────────────────────────────────
      map.on('click', 'planned-pins-dot', (e) => {
        // Click on a planned repeater pin — remove it
        if (!useOverlayStore.getState().planRepeaterMode) return;
        const feature = e.features?.[0];
        if (!feature) return;
        const planId = (feature.properties as { plan_id: string }).plan_id;
        customLosNodeClickedRef.current = true; // prevent general map-click from firing
        // handleRemovePlannedRepeater is stable via useCallback; read from ref to avoid stale closure
        handleRemovePlannedRepeaterRef.current(planId);
      });

      map.on('click', 'node-dots', (e) => {
        const feature = e.features?.[0];
        if (!feature) return;
        const props = feature.properties as NodeFeatureProps;

        // In plan repeater mode, node clicks place a repeater on the node's location
        if (useOverlayStore.getState().planRepeaterMode) {
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
        const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
        setPopupLinks(null);
        setPopupState({ nodeId: props.node_id, lngLat: { lng: coords[0], lat: coords[1] } });
      });

      // General map click — used for custom LOS mode and plan repeater placement on empty areas
      map.on('click', (e) => {
        const { lng, lat } = e.lngLat;

        if (useOverlayStore.getState().planRepeaterMode) {
          if (customLosNodeClickedRef.current) {
            customLosNodeClickedRef.current = false;
            return;
          }
          if (plannedRepeatersRef.current.length < 5) {
            void placePlannedRepeaterRef.current(lat, lng);
          }
          return;
        }

        if (!useOverlayStore.getState().customLosMode) return;
        if (customLosNodeClickedRef.current) {
          customLosNodeClickedRef.current = false;
          return; // Node dot already handled above
        }
        void sampleElevationAt(lng, lat).then((elevation_m) => {
          void handleCustomLosPointRef.current({ lat, lon: lng, elevation_m });
        });
      });

      // Make cursor a pointer over node dots and planned pins
      map.on('mouseenter', 'node-dots', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'node-dots', () => {
        map.getCanvas().style.cursor = useOverlayStore.getState().planRepeaterMode || useOverlayStore.getState().customLosMode ? 'crosshair' : '';
      });
      map.on('mouseenter', 'planned-pins-dot', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'planned-pins-dot', () => {
        map.getCanvas().style.cursor = useOverlayStore.getState().planRepeaterMode ? 'crosshair' : '';
      });

      mapRef.current = map;
      onMapReady?.(map);
      refreshMapSources();

      // Restore terrain if it was saved in preferences
      if (showTerrainRef.current) {
        map.addSource('terrain-dem', TERRAIN_DEM_SOURCE);
        map.addLayer({
          id: 'hillshade', type: 'hillshade', source: 'terrain-dem', minzoom: 7,
          paint: { 'hillshade-exaggeration': 0.7, 'hillshade-shadow-color': '#000000', 'hillshade-highlight-color': '#ffffff', 'hillshade-illumination-anchor': 'viewport' },
        }, 'node-dots');
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        map.addLayer({ id: 'sky', type: 'sky', paint: { 'sky-type': 'atmosphere', 'sky-atmosphere-sun': [0, 90], 'sky-atmosphere-sun-intensity': 15 } } as any);
        map.setMaxPitch(85);
        if (map.getZoom() >= 7) map.setTerrain(TERRAIN_CONFIG);
        map.easeTo({ pitch: 45, duration: 600 });
      }
    });

    return () => {
      mapLoadedRef.current = false;
      map.remove();
      mapRef.current = null;
    };
  }, [onMapReady, refreshMapSources]);

  // -- Imperative source updates ---------------------------------------------

  useEffect(() => {
    inferredNodesRef.current = inferredNodes;
    scheduleRefresh();
  }, [inferredNodes, scheduleRefresh]);

  useEffect(() => {
    showLinksRef.current = showLinks;
    scheduleRefresh();
  }, [showLinks, scheduleRefresh]);

  useEffect(() => {
    showTerrainRef.current = showTerrain;
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current) return;
    if (showTerrain) {
      if (!map.getSource('terrain-dem')) map.addSource('terrain-dem', TERRAIN_DEM_SOURCE);
      if (!map.getLayer('hillshade')) map.addLayer({
        id: 'hillshade', type: 'hillshade', source: 'terrain-dem', minzoom: 7,
        paint: { 'hillshade-exaggeration': 0.7, 'hillshade-shadow-color': '#000000', 'hillshade-highlight-color': '#ffffff', 'hillshade-illumination-anchor': 'viewport' },
      }, 'node-dots');
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      if (!map.getLayer('sky')) map.addLayer({ id: 'sky', type: 'sky', paint: { 'sky-type': 'atmosphere', 'sky-atmosphere-sun': [0, 90], 'sky-atmosphere-sun-intensity': 15 } } as any);
      map.setMaxPitch(85);
      if (map.getZoom() >= 7) map.setTerrain(TERRAIN_CONFIG);
      map.easeTo({ pitch: 45, duration: 600 });
    } else {
      map.setTerrain(null);
      if (map.getLayer('hillshade')) map.removeLayer('hillshade');
      if (map.getLayer('sky')) map.removeLayer('sky');
      if (map.getSource('terrain-dem')) map.removeSource('terrain-dem');
      map.easeTo({ pitch: 0, duration: 400 });
      setTimeout(() => map.setMaxPitch(0), 400);
    }
  }, [showTerrain]);

  useEffect(() => {
    showClientNodesRef.current = showClientNodes;
    scheduleRefresh();
  }, [showClientNodes, scheduleRefresh]);

  useEffect(() => {
    showHexClashesRef.current = showHexClashes;
    scheduleRefresh();
  }, [showHexClashes, scheduleRefresh]);

  useEffect(() => {
    maxHexClashHopsRef.current = maxHexClashHops;
    scheduleRefresh();
  }, [maxHexClashHops, scheduleRefresh]);

  useEffect(() => {
    popupStateRef.current = popupState;
  }, [popupState]);

  useEffect(() => {
    const unsubscribeNodes = nodeStore.subscribe(() => {
      nodesRef.current = nodeStore.getState().nodes;
      scheduleRefresh();
      if (popupStateRef.current) setPopupVersion((value) => value + 1);
    });
    const unsubscribeCoverage = coverageStore.subscribe(() => {
      coverageRef.current = coverageStore.getState().coverage;
      scheduleRefresh();
    });
    const unsubscribeLinks = linkStateStore.subscribe(() => {
      const linkState = linkStateStore.getState();
      viablePairsRef.current = linkState.viablePairsArr;
      linkMetricsRef.current = linkState.linkMetrics;
      scheduleRefresh();
    });
    const unsubscribeOverlay = useOverlayStore.subscribe((overlayState) => {
      if (overlayState.pathNodeIds === pathNodeIdsRef.current) return;
      pathNodeIdsRef.current = overlayState.pathNodeIds;
      scheduleRefresh();
    });

    return () => {
      unsubscribeNodes();
      unsubscribeCoverage();
      unsubscribeLinks();
      unsubscribeOverlay();
    };
  }, [scheduleRefresh]);

  useEffect(() => {
    scheduleRefresh();
  }, [focusedNodeId, focusedPrefixNodeIds, scheduleRefresh]);

  const toggleCoverageForNode = useCallback((nodeId: string) => {
    if (coverageLoadingNodeId === nodeId) return;
    if (selectedCoverageNodeId === nodeId) {
      selectedCoverageRef.current = null;
      setSelectedCoverageNodeId(null);
      setCoverageMessage(null);
      scheduleRefresh();
      return;
    }

    setCoverageLoadingNodeId(nodeId);
    setCoverageMessage(null);
    void fetch(`/api/coverage/${encodeURIComponent(nodeId)}`, { cache: 'no-store' })
      .then(async (response) => {
        const payload = await response.json().catch(() => ({})) as { status?: string; coverage?: NodeCoverage };
        if (response.status === 202 || payload.status === 'queued') {
          selectedCoverageRef.current = null;
          setSelectedCoverageNodeId(null);
          setCoverageMessage('Coverage is being calculated.');
          return;
        }
        if (!response.ok || !payload.coverage) throw new Error('coverage unavailable');
        selectedCoverageRef.current = payload.coverage;
        setSelectedCoverageNodeId(nodeId);
        setCoverageMessage(null);
      })
      .catch(() => {
        selectedCoverageRef.current = null;
        setSelectedCoverageNodeId(null);
        setCoverageMessage('Coverage unavailable.');
      })
      .finally(() => {
        setCoverageLoadingNodeId(null);
        scheduleRefresh();
      });
  }, [coverageLoadingNodeId, selectedCoverageNodeId, scheduleRefresh]);

  // -- Popup management ------------------------------------------------------

  // Find the full MeshNode from nodeId (checks nodes and inferredNodes)
  const getNode = useCallback((nodeId: string): MeshNode | undefined => {
    return nodesRef.current.get(nodeId) ?? inferredNodesRef.current.find((node) => node.node_id === nodeId);
  }, []);

  // Fetch neighbour links for non-repeater node popups
  useEffect(() => {
    if (!popupState) return;
    const node = getNode(popupState.nodeId);
    if (!node || node.role === undefined || node.role === 2) return;
    // Non-repeater — fetch neighbours
    setPopupLinks(null);
    fetch(`/api/nodes/${popupState.nodeId}/links`)
      .then((r) => r.json() as Promise<NodeLink[]>)
      .then(setPopupLinks)
      .catch(() => setPopupLinks([]));
  }, [popupState?.nodeId, getNode]); // eslint-disable-line react-hooks/exhaustive-deps

  // Show/update/close the MapLibre popup when popupState changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current) return;

    if (!popupState) {
      mlPopupRef.current?.remove();
      return;
    }

    if (!mlPopupRef.current) {
      mlPopupRef.current = new maplibregl.Popup({ maxWidth: '280px', closeOnClick: false })
        .setDOMContent(popupContainerRef.current)
        .on('close', () => setPopupState(null));
    }

    mlPopupRef.current.setLngLat(popupState.lngLat).addTo(map);
  }, [popupState]);

  // Resolve popup props from current nodes map
  const popupNodeProps = useMemo((): PopupNodeView | null => {
    if (!popupState) return null;
    const node = getNode(popupState.nodeId);
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
        is_stale: ageMs > SEVEN_DAYS_MS,
        is_link_only_stale: false,
        is_prohibited: isProhibitedMapNode(node),
        is_inferred: !!node.is_inferred,
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
  }, [popupState, popupVersion, getNode]);

  const popupSamePrefixCount = useMemo(() => {
    if (!popupState) return 1;
    const prefix = popupState.nodeId.slice(0, 2).toUpperCase();
    return Array.from(nodesRef.current.values()).filter(
      (node) => hasCoords(node)
        && (node.role === undefined || node.role === 2)
        && node.node_id.slice(0, 2).toUpperCase() === prefix,
    ).length || 1;
  }, [popupState, popupVersion]);

  // -- Render ----------------------------------------------------------------

  return (
    <div className="map-area" style={{ position: 'relative', width: '100%', height: '100%' }}>
      <NodeSearch map={mapRef.current} />
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />

      {/* Map tool buttons */}
      <div className="map-tools">
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
        <button
          type="button"
          className={`map-tools__btn${mapLight ? ' map-tools__btn--active' : ''}`}
          onClick={(e) => { e.stopPropagation(); toggleMapTheme(); }}
          onMouseDown={(e) => e.stopPropagation()}
          onMouseUp={(e) => e.stopPropagation()}
        >
          {mapLight
            ? <><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>Light</>
            : <><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>Dark</>
          }
        </button>
      </div>

      {/* Custom LOS status hint */}
      {customLosMode && (
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
      {planRepeaterMode && (
        <div
          style={{
            position: 'absolute', bottom: 40, left: '50%', transform: 'translateX(-50%)',
            background: 'rgba(0,0,0,0.75)', color: '#22d3ee', padding: '6px 14px',
            borderRadius: 4, fontSize: 12, pointerEvents: 'none', zIndex: 10,
            whiteSpace: 'nowrap',
          }}
        >
          {plannedRepeaters.length >= 5
            ? 'Max 5 repeaters placed — click a pin to remove it — Esc to cancel'
            : 'Click map to place a planned repeater — click a pin to remove it — Esc to cancel'}
        </div>
      )}

      {/* Computing coverage indicator */}
      {plannedRepeaters.some((r) => r.status === 'queued') && (
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

      {/* Popup content rendered into the MapLibre popup's DOM node via portal */}
      {popupState && popupNodeProps && createPortal(
        <NodePopupContent
          props={popupNodeProps.props}
          lat={popupNodeProps.maskedLat}
          lon={popupNodeProps.maskedLon}
          links={popupLinks}
          coverageActive={selectedCoverageNodeId === popupNodeProps.props.node_id}
          coverageLoading={coverageLoadingNodeId === popupNodeProps.props.node_id}
          coverageMessage={popupState?.nodeId === popupNodeProps.props.node_id ? coverageMessage : null}
          onToggleCoverage={toggleCoverageForNode}
          onFocusSamePrefix={handleFocusSamePrefix}
          samePrefixCount={popupSamePrefixCount}
          losActive={popupLosActive}
          losLoading={popupLosLoading}
          onToggleLos={handleToggleLos}
        />,
        popupContainerRef.current,
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
