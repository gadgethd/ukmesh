import React, { useEffect, useMemo, useRef } from 'react';
import type maplibregl from 'maplibre-gl';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import type { MeshNode } from '../../hooks/useNodes.js';
import { buildPathNodePopupContent } from './pathNodePopup.js';

export type ResolvedPath = {
  ok: boolean;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  purplePath: [number, number][] | null;
  redPath: [number, number][] | null;
  redSegments?: [[number, number], [number, number]][];
};

export type LazyPathNode = {
  position: number;
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  appearances: number;
  totalObservations: number;
  ambiguous: boolean;
  isObserver: boolean;
};

export type LazyPath = {
  canonicalPath: LazyPathNode[];
  coordinates: Array<[number, number]>;
  matchedHops: number;
  totalHops: number;
  observerIds: string[];
};

export type LazyPathResult = {
  packetHash: string;
  observerCount: number;
  paths: LazyPath[];
};

const CARTO_TILES = [
  'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
];
const C_PURPLE = '#ce93d8';
const C_RED = '#ff5252';
const C_CYAN = '#00c4ff';
const LAZY_PATH_COLORS = ['#26c6a2', '#00b4d8', '#f59e0b', '#a78bfa', '#f87171'];

export const PathMap: React.FC<{
  results: ResolvedPath[];
  observerPositions?: [number, number][];
  lazyPaths?: LazyPath[];
  nodeMap?: Map<string, MeshNode>;
  isLoading?: boolean;
}> = ({ results, observerPositions = [], lazyPaths = [], nodeMap, isLoading = false }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const maplibreRef = useRef<typeof maplibregl | null>(null);
  const mapReadyRef = useRef(false);
  const nodeMapRef = useRef(nodeMap);
  nodeMapRef.current = nodeMap;

  const lazyPathsRef = useRef(lazyPaths);
  const resultsRef = useRef(results);
  const observerPositionsRef = useRef(observerPositions);
  lazyPathsRef.current = lazyPaths;
  resultsRef.current = results;
  observerPositionsRef.current = observerPositions;

  const toLngLat = ([lat, lon]: [number, number]): [number, number] => [lon, lat];
  const hasData = useMemo(() => {
    if (results.some((result) => result.purplePath?.length || result.redPath?.length || result.redSegments?.length)) return true;
    if (lazyPaths.some((path) => path.canonicalPath.some((node) => node.lat != null && node.lon != null))) return true;
    return observerPositions.some(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
  }, [results, lazyPaths, observerPositions]);

  const applyData = React.useCallback((map: maplibregl.Map, fitBounds: boolean) => {
    const maplibre = maplibreRef.current;
    if (!maplibre) return;
    const lazys = lazyPathsRef.current;
    const res = resultsRef.current;
    const obs = observerPositionsRef.current;
    const bounds = new maplibre.LngLatBounds();

    const allPurpleCoords: [number, number][][] = [];
    const allRedCoords: [number, number][][] = [];
    const allNodeCoords: [number, number][] = [];
    for (const result of res) {
      if (result.purplePath && result.purplePath.length >= 2) {
        const coords = result.purplePath.map(toLngLat);
        allPurpleCoords.push(coords);
        allNodeCoords.push(...coords);
        coords.forEach((coordinate) => bounds.extend(coordinate));
      }
      if (result.redPath && result.redPath.length >= 2) allRedCoords.push(result.redPath.map(toLngLat));
      if (result.redSegments?.length) {
        result.redSegments.forEach(([a, b]) => allRedCoords.push([toLngLat(a), toLngLat(b)]));
      }
      result.redPath?.forEach((point) => bounds.extend(toLngLat(point)));
      result.redSegments?.forEach(([a, b]) => {
        bounds.extend(toLngLat(a));
        bounds.extend(toLngLat(b));
      });
    }
    if (allPurpleCoords.length > 0) {
      const lines = {
        type: 'FeatureCollection' as const,
        features: allPurpleCoords.map((coordinates) => ({
          type: 'Feature' as const,
          geometry: { type: 'LineString' as const, coordinates },
          properties: {},
        })),
      };
      const nodes = {
        type: 'FeatureCollection' as const,
        features: allNodeCoords.map((coordinates) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates },
          properties: {},
        })),
      };
      const lineSource = map.getSource('purple-lines') as maplibregl.GeoJSONSource | undefined;
      if (lineSource) {
        lineSource.setData(lines);
        (map.getSource('purple-nodes') as maplibregl.GeoJSONSource | undefined)?.setData(nodes);
      } else {
        map.addSource('purple-lines', { type: 'geojson', data: lines });
        map.addLayer({ id: 'purple-lines-layer', type: 'line', source: 'purple-lines', paint: { 'line-color': C_PURPLE, 'line-width': 2.5, 'line-opacity': 0.85 } });
        map.addSource('purple-nodes', { type: 'geojson', data: nodes });
        map.addLayer({ id: 'purple-node-circles', type: 'circle', source: 'purple-nodes', paint: { 'circle-radius': 5, 'circle-color': '#0b1725', 'circle-stroke-color': C_CYAN, 'circle-stroke-width': 2 } });
      }
    }
    if (allRedCoords.length > 0) {
      const redData = {
        type: 'FeatureCollection' as const,
        features: allRedCoords.map((coordinates) => ({
          type: 'Feature' as const,
          geometry: { type: 'LineString' as const, coordinates },
          properties: {},
        })),
      };
      const lineSource = map.getSource('red-lines') as maplibregl.GeoJSONSource | undefined;
      if (lineSource) lineSource.setData(redData);
      else {
        map.addSource('red-lines', { type: 'geojson', data: redData });
        map.addLayer({ id: 'red-lines-layer', type: 'line', source: 'red-lines', paint: { 'line-color': C_RED, 'line-width': 1.5, 'line-opacity': 0.65, 'line-dasharray': [4, 4] } });
      }
    }

    lazys.forEach((lazyPath, pathIndex) => {
      const color = LAZY_PATH_COLORS[pathIndex % LAZY_PATH_COLORS.length]!;
      const validNodes = lazyPath.canonicalPath.filter(
        (node) => node.lat != null && node.lon != null && Number.isFinite(node.lat) && Number.isFinite(node.lon),
      );
      if (validNodes.length < 2) return;
      const coordinates = validNodes.map((node) => [node.lon!, node.lat!] as [number, number]);
      coordinates.forEach((coordinate) => bounds.extend(coordinate));
      const lineId = `lazy-line-${pathIndex}`;
      const nodeSourceId = `lazy-nodes-${pathIndex}`;
      const lineData = {
        type: 'Feature' as const,
        geometry: { type: 'LineString' as const, coordinates },
        properties: {},
      };
      const nodeData = {
        type: 'FeatureCollection' as const,
        features: validNodes.map((node, index) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: coordinates[index]! },
          properties: { nodeId: node.nodeId ?? '', name: node.name ?? '', isObserver: node.isObserver },
        })),
      };
      const lineSource = map.getSource(lineId) as maplibregl.GeoJSONSource | undefined;
      if (lineSource) {
        lineSource.setData(lineData);
        (map.getSource(nodeSourceId) as maplibregl.GeoJSONSource | undefined)?.setData(nodeData);
        return;
      }
      map.addSource(lineId, { type: 'geojson', data: lineData });
      map.addLayer({ id: `${lineId}-layer`, type: 'line', source: lineId, paint: { 'line-color': color, 'line-width': 3, 'line-opacity': 0.9 } });
      map.addSource(nodeSourceId, { type: 'geojson', data: nodeData });
      map.addLayer({ id: `${nodeSourceId}-layer`, type: 'circle', source: nodeSourceId, paint: { 'circle-radius': 6, 'circle-color': '#0b1725', 'circle-stroke-color': color, 'circle-stroke-width': 2.5 } });
      map.on('click', `${nodeSourceId}-layer`, (event) => {
        const feature = event.features?.[0];
        if (!feature) return;
        const properties = feature.properties as { nodeId: string; name: string; isObserver: boolean };
        const fullNode = properties.nodeId ? nodeMapRef.current?.get(properties.nodeId) : undefined;
        new maplibre.Popup({ closeButton: true, maxWidth: '320px' })
          .setLngLat(event.lngLat)
          .setDOMContent(buildPathNodePopupContent({
            displayName: properties.name || properties.nodeId.slice(0, 12) || '—',
            publicKey: fullNode?.public_key ?? properties.nodeId ?? '—',
            isObserver: properties.isObserver,
          }))
          .addTo(map);
      });
      map.on('mouseenter', `${nodeSourceId}-layer`, () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', `${nodeSourceId}-layer`, () => { map.getCanvas().style.cursor = ''; });
    });

    const validObservers = obs.filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
    if (validObservers.length > 0) {
      const features = validObservers.map(([lat, lon]) => ({
        type: 'Feature' as const,
        geometry: { type: 'Point' as const, coordinates: [lon, lat] as [number, number] },
        properties: {},
      }));
      const data = { type: 'FeatureCollection' as const, features };
      const source = map.getSource('observer-pos') as maplibregl.GeoJSONSource | undefined;
      if (source) source.setData(data);
      else {
        map.addSource('observer-pos', { type: 'geojson', data });
        map.addLayer({ id: 'observer-pos-layer', type: 'circle', source: 'observer-pos', paint: { 'circle-radius': 8, 'circle-color': '#ffb300', 'circle-stroke-color': '#ffffff', 'circle-stroke-width': 2 } });
      }
      features.forEach((feature) => bounds.extend(feature.geometry.coordinates));
    }

    if (fitBounds && !bounds.isEmpty()) map.fitBounds(bounds, { padding: 24, animate: false });
  }, []);

  useEffect(() => {
    if (!containerRef.current) return undefined;
    let cancelled = false;
    let map: maplibregl.Map | null = null;
    void Promise.all([
      import('maplibre-gl'),
      import('maplibre-gl/dist/maplibre-gl.css'),
    ]).then(([maplibreModule]) => {
      if (cancelled || !containerRef.current) return;
      const maplibre = maplibreModule.default;
      maplibreRef.current = maplibre;
      const nextMap = new maplibre.Map({
        container: containerRef.current,
        style: {
          version: 8,
          sources: { tiles: { type: 'raster', tiles: CARTO_TILES, tileSize: 256, maxzoom: 19, attribution: '© OpenStreetMap © CARTO' } },
          layers: [{ id: 'bg', type: 'raster', source: 'tiles' }],
        },
        center: [0, 51.5],
        zoom: 6,
        attributionControl: false,
      });
      map = nextMap;
      mapRef.current = nextMap;
      nextMap.on('load', () => {
        if (cancelled) return;
        mapReadyRef.current = true;
        applyData(nextMap, true);
      });
    });
    return () => {
      cancelled = true;
      if (mapRef.current === map) mapRef.current = null;
      mapReadyRef.current = false;
      maplibreRef.current = null;
      map?.remove();
    };
  }, [applyData]);

  const previousPathKeyRef = useRef('');
  const previousLoadingRef = useRef(isLoading);
  useEffect(() => {
    if (!mapReadyRef.current || !mapRef.current) return;
    const loadingComplete = previousLoadingRef.current && !isLoading;
    previousLoadingRef.current = isLoading;
    const pathKey = [
      ...lazyPaths.flatMap((path) => path.canonicalPath
        .filter((node) => node.lat != null)
        .map((node) => `${node.lat?.toFixed(4)},${node.lon?.toFixed(4)}`)),
      ...results.flatMap((result) => (result.purplePath ?? [])
        .map(([lat, lon]) => `${lat.toFixed(4)},${lon.toFixed(4)}`)),
    ].join('|');
    const pathsChanged = pathKey !== previousPathKeyRef.current;
    previousPathKeyRef.current = pathKey;
    applyData(mapRef.current, !isLoading && (pathsChanged || loadingComplete));
  }, [lazyPaths, results, observerPositions, isLoading, applyData]);

  return (
    <div style={{ position: 'relative', height: '100%', width: '100%' }}>
      <div ref={containerRef} style={{ height: '100%', width: '100%' }} />
      {isLoading && (
        <LoadingIndicator label="Resolving path..." variant="overlay" className="path-map-loading-overlay" />
      )}
      {!hasData && !isLoading && (
        <div className="feed-detail__no-map" style={{ position: 'absolute', inset: 0 }}>
          Path could not be resolved
        </div>
      )}
    </div>
  );
};
