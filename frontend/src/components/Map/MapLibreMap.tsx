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
  SEVEN_DAYS_MS,
  TERRAIN_CONFIG,
} from './mapConfig.js';
import {
  buildClashLinesGeoJSON,
  buildCoverageGeoJSON,
  buildHiddenMask,
  buildLinksGeoJSON,
  buildNodeGeoJSON,
  buildPrivacyRingsGeoJSON,
  computeClashData,
} from './geojsonBuilders.js';
import { NodePopupContent } from './NodePopupContent.js';
import type {
  MapLibreMapProps,
  NodeFeatureProps,
  NodeLink,
  PopupNodeView,
  PopupState,
} from './types.js';

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
  const showClientNodesRef = useRef(showClientNodes);
  const showHexClashesRef = useRef(showHexClashes);
  const maxHexClashHopsRef = useRef(maxHexClashHops);
  const pathNodeIdsRef = useRef(useOverlayStore.getState().pathNodeIds);
  const hiddenCoordMaskRef = useRef<Map<string, HiddenMaskGeometry>>(new Map());
  const refreshTimerRef = useRef<number | null>(null);
  const popupStateRef = useRef<PopupState | null>(null);

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
      inferredNodesRef.current,
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
      style: MAP_STYLE,
      center: [DEFAULT_CENTER[1], DEFAULT_CENTER[0]], // [lon, lat]
      zoom: DEFAULT_ZOOM,
      maxPitch: 0,
      attributionControl: false,
    });

    map.on('load', () => {
      mapLoadedRef.current = true;

      // ── Sky layer (for 3D terrain mode) ────────────────────────────────────
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      map.addLayer({ id: 'sky', type: 'sky', paint: { 'sky-type': 'atmosphere', 'sky-atmosphere-sun': [0, 90], 'sky-atmosphere-sun-intensity': 15 } } as any);

      // ── Hillshade layer ────────────────────────────────────────────────────
      map.addLayer({
        id: 'hillshade',
        type: 'hillshade',
        source: 'terrain-dem',
        paint: {
          'hillshade-exaggeration': 0.7,
          'hillshade-shadow-color': '#000000',
          'hillshade-highlight-color': '#ffffff',
          'hillshade-illumination-anchor': 'viewport',
        },
      });

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

      // ── Click handler ──────────────────────────────────────────────────────
      map.on('click', 'node-dots', (e) => {
        const feature = e.features?.[0];
        if (!feature) return;
        const props = feature.properties as NodeFeatureProps;
        // MapLibre serialises properties to JSON strings for non-primitive types,
        // but all our props are primitives so this is safe.
        const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
        setPopupLinks(null);
        setPopupState({ nodeId: props.node_id, lngLat: { lng: coords[0], lat: coords[1] } });
      });

      // Make cursor a pointer over node dots
      map.on('mouseenter', 'node-dots', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'node-dots', () => {
        map.getCanvas().style.cursor = '';
      });

      mapRef.current = map;
      onMapReady?.(map);
      refreshMapSources();
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
    const map = mapRef.current;
    if (!map || !mapLoadedRef.current) return;
    if (showTerrain) {
      map.setMaxPitch(85);
      map.setTerrain(TERRAIN_CONFIG);
      map.easeTo({ pitch: 45, duration: 600 });
    } else {
      map.easeTo({ pitch: 0, duration: 400 });
      setTimeout(() => map.setMaxPitch(0), 400);
      map.setTerrain(null);
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
