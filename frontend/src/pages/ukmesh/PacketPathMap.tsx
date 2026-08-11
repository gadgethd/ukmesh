import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type maplibregl from 'maplibre-gl';
import { AnimatedPathOverlay, type AerialPath, type AerialPathNode } from '../../components/Map/AnimatedPathOverlay.js';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import type { MeshNode } from '../../hooks/useNodes.js';
import {
  multiObserverPathRoutes,
  type CanonicalPathNode,
  type MultiObserverBetaResponse,
  type ResolveMode,
  type ServerBetaResponse,
} from '../../hooks/packetPathOverlayUtils.js';
import { buildPathNodePopupContent } from './pathNodePopup.js';

export type { CanonicalPathNode, MultiObserverBetaResponse, ResolveMode, ServerBetaResponse };
export type ResolvedPath = ServerBetaResponse;

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

function lazyPathRuns(nodes: readonly LazyPathNode[]): LazyPathNode[][] {
  const runs: LazyPathNode[][] = [];
  let current: LazyPathNode[] = [];
  const flush = () => {
    if (current.length >= 2) runs.push(current);
    current = [];
  };
  for (const node of nodes) {
    if (node.lat == null || node.lon == null || !Number.isFinite(node.lat) || !Number.isFinite(node.lon)) {
      flush();
      continue;
    }
    current.push(node);
  }
  flush();
  return runs;
}

export function buildAerialPaths(
  results: MultiObserverBetaResponse[],
  lazyPaths: LazyPath[],
  pathScopeId = 'detail',
): AerialPath[] {
  const paths: AerialPath[] = [];
  results.forEach((result) => {
    multiObserverPathRoutes(result).forEach((route) => {
      paths.push({
        id: `canonical-${result.packetHash}`,
        packetHash: result.packetHash,
        confidence: route.confidence,
        nodes: route.nodes.map((node) => ({
          position: [node.lon, node.lat],
          nodeId: node.nodeId ?? undefined,
          name: node.name ?? undefined,
          confidence: node.confidence,
        })),
      });
    });
  });

  // The canonical multi-observer response is authoritative. Lazy paths are
  // used only by the feed view, which does not request the multi response.
  if (results.length > 0) return paths;

  lazyPaths.forEach((path) => {
    lazyPathRuns(path.canonicalPath).forEach((run) => {
      const nodes = run.map((node): AerialPathNode => ({
        position: [node.lon!, node.lat!],
        nodeId: node.nodeId ?? undefined,
        name: node.name ?? undefined,
        isObserver: node.isObserver,
      }));
      const confidence = path.totalHops > 0
        ? Math.max(0, Math.min(1, path.matchedHops / path.totalHops))
        : null;
      paths.push({
        id: `hash-traced-${pathScopeId}`,
        packetHash: pathScopeId,
        confidence,
        nodes,
      });
    });
  });
  return paths;
}

export const PathMap: React.FC<{
  results: MultiObserverBetaResponse[];
  observerPositions?: [number, number][];
  lazyPaths?: LazyPath[];
  pathScopeId?: string;
  nodeMap?: Map<string, MeshNode>;
  isLoading?: boolean;
}> = ({ results, observerPositions = [], lazyPaths = [], pathScopeId = 'detail', nodeMap, isLoading = false }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const maplibreRef = useRef<typeof maplibregl | null>(null);
  const mapReadyRef = useRef(false);
  const nodeMapRef = useRef(nodeMap);
  const observerPositionsRef = useRef(observerPositions);
  const aerialPaths = useMemo(
    () => buildAerialPaths(results, lazyPaths, pathScopeId),
    [results, lazyPaths, pathScopeId],
  );
  const aerialPathsRef = useRef(aerialPaths);
  const [map, setMap] = useState<maplibregl.Map | null>(null);
  nodeMapRef.current = nodeMap;
  observerPositionsRef.current = observerPositions;
  aerialPathsRef.current = aerialPaths;

  const hasData = useMemo(() => aerialPaths.length > 0 || observerPositions.some(
    ([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon),
  ), [aerialPaths, observerPositions]);

  const applyData = useCallback((targetMap: maplibregl.Map, fitBounds: boolean) => {
    const maplibre = maplibreRef.current;
    if (!maplibre) return;
    const bounds = new maplibre.LngLatBounds();
    for (const path of aerialPathsRef.current) {
      for (const node of path.nodes) bounds.extend(node.position);
    }

    const validObservers = observerPositionsRef.current.filter(
      ([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon),
    );
    const features = validObservers.map(([lat, lon]) => ({
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: [lon, lat] as [number, number] },
      properties: {},
    }));
    const data = { type: 'FeatureCollection' as const, features };
    const source = targetMap.getSource('observer-pos') as maplibregl.GeoJSONSource | undefined;
    if (source) source.setData(data);
    else {
      targetMap.addSource('observer-pos', { type: 'geojson', data });
      targetMap.addLayer({
        id: 'observer-pos-layer',
        type: 'circle',
        source: 'observer-pos',
        paint: {
          'circle-radius': 8,
          'circle-color': '#3b82f6',
          'circle-stroke-color': '#ffffff',
          'circle-stroke-width': 2,
        },
      });
    }
    features.forEach((feature) => bounds.extend(feature.geometry.coordinates));
    if (fitBounds && !bounds.isEmpty()) targetMap.fitBounds(bounds, { padding: 24, animate: false });
  }, []);

  useEffect(() => {
    if (!containerRef.current) return undefined;
    let cancelled = false;
    let nextMap: maplibregl.Map | null = null;
    void Promise.all([
      import('maplibre-gl'),
      import('maplibre-gl/dist/maplibre-gl.css'),
    ]).then(([maplibreModule]) => {
      if (cancelled || !containerRef.current) return;
      const maplibre = maplibreModule.default;
      maplibreRef.current = maplibre;
      nextMap = new maplibre.Map({
        container: containerRef.current,
        style: {
          version: 8,
          sources: { tiles: { type: 'raster', tiles: CARTO_TILES, tileSize: 256, maxzoom: 19, attribution: '© OpenStreetMap © CARTO' } },
          layers: [{ id: 'bg', type: 'raster', source: 'tiles' }],
        },
        center: [0, 51.5],
        zoom: 6,
        pitch: 50,
        bearing: -8,
        attributionControl: false,
      });
      mapRef.current = nextMap;
      nextMap.on('load', () => {
        if (cancelled || !nextMap) return;
        mapReadyRef.current = true;
        applyData(nextMap, true);
        setMap(nextMap);
      });
    });
    return () => {
      cancelled = true;
      setMap(null);
      mapReadyRef.current = false;
      maplibreRef.current = null;
      mapRef.current = null;
      nextMap?.remove();
    };
  }, [applyData]);

  const previousPathKeyRef = useRef('');
  const previousLoadingRef = useRef(isLoading);
  useEffect(() => {
    if (!mapReadyRef.current || !mapRef.current) return;
    const loadingComplete = previousLoadingRef.current && !isLoading;
    previousLoadingRef.current = isLoading;
    const pathKey = aerialPaths.map((path) => `${path.id}:${path.nodes.map((node) => node.position.join(',')).join(';')}`).join('|');
    const pathsChanged = pathKey !== previousPathKeyRef.current;
    previousPathKeyRef.current = pathKey;
    applyData(mapRef.current, !isLoading && (pathsChanged || loadingComplete));
  }, [aerialPaths, observerPositions, isLoading, applyData]);

  const handleNodeClick = useCallback((node: AerialPathNode) => {
    const currentMap = mapRef.current;
    const maplibre = maplibreRef.current;
    if (!currentMap || !maplibre) return;
    const fullNode = node.nodeId ? nodeMapRef.current?.get(node.nodeId) : undefined;
    new maplibre.Popup({ closeButton: true, maxWidth: '320px' })
      .setLngLat(node.position)
      .setDOMContent(buildPathNodePopupContent({
        displayName: node.name || node.nodeId?.slice(0, 12) || '—',
        publicKey: fullNode?.public_key ?? node.nodeId ?? '—',
        isObserver: Boolean(node.isObserver),
      }))
      .addTo(currentMap);
  }, []);

  return (
    <div style={{ position: 'relative', height: '100%', width: '100%' }}>
      <div ref={containerRef} style={{ height: '100%', width: '100%' }} />
      <AnimatedPathOverlay map={map} paths={aerialPaths} active={!isLoading} onNodeClick={handleNodeClick} />
      {isLoading && <LoadingIndicator label="Resolving path..." variant="overlay" className="path-map-loading-overlay" />}
      {!hasData && !isLoading && (
        <div className="feed-detail__no-map" style={{ position: 'absolute', inset: 0 }}>Path could not be resolved</div>
      )}
    </div>
  );
};
