import React, { useEffect, useRef } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { DEFAULT_CENTER, MAP_STYLE } from '../../components/Map/mapConfig.js';
import type { MappedPeer } from './ownerPortalModel.js';
export const OwnerMapView: React.FC<{
  ownerCoord: { lat: number; lon: number } | null;
  peers: MappedPeer[];
  allPoints: Array<{ lat: number; lon: number }>;
}> = ({ ownerCoord, peers, allPoints }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const loadedRef = useRef(false);
  const dataRef = useRef({ ownerCoord, peers, allPoints });
  dataRef.current = { ownerCoord, peers, allPoints };

  const applyData = (map: maplibregl.Map) => {
    if (!loadedRef.current) return;
    const current = dataRef.current;
    const nodeSource = map.getSource('owner-nodes') as maplibregl.GeoJSONSource | undefined;
    const lineSource = map.getSource('owner-lines') as maplibregl.GeoJSONSource | undefined;
    nodeSource?.setData({
      type: 'FeatureCollection',
      features: [
        ...(current.ownerCoord ? [{
          type: 'Feature' as const,
          geometry: {
            type: 'Point' as const,
            coordinates: [current.ownerCoord.lon, current.ownerCoord.lat],
          },
          properties: {
            kind: 'owner',
            name: 'Selected owner node',
            details: `${current.ownerCoord.lat.toFixed(4)}, ${current.ownerCoord.lon.toFixed(4)}`,
          },
        }] : []),
        ...current.peers.map((peer) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [peer.lon, peer.lat] },
          properties: {
            kind: 'peer',
            name: peer.name ?? peer.node_id,
            details: `${peer.packets_24h} packets / 24h${peer.iata ? ` · ${peer.iata}` : ''}${peer.network ? ` · ${peer.network}` : ''}`,
          },
        })),
      ],
    });
    lineSource?.setData({
      type: 'FeatureCollection',
      features: current.ownerCoord ? current.peers.map((peer) => ({
        type: 'Feature' as const,
        geometry: {
          type: 'LineString' as const,
          coordinates: [
            [current.ownerCoord!.lon, current.ownerCoord!.lat],
            [peer.lon, peer.lat],
          ],
        },
        properties: {},
      })) : [],
    });

    map.resize();
    if (current.allPoints.length === 0) {
      map.jumpTo({
        center: [DEFAULT_CENTER[1], DEFAULT_CENTER[0]],
        zoom: 7,
        bearing: 0,
        pitch: 0,
      });
      return;
    }
    if (current.allPoints.length === 1) {
      map.jumpTo({
        center: [current.allPoints[0]!.lon, current.allPoints[0]!.lat],
        zoom: 8,
        bearing: 0,
        pitch: 0,
      });
      return;
    }
    const centerPoints = current.peers.length > 0 ? current.peers : current.allPoints;
    const centerLons = centerPoints.map((point) => point.lon);
    const centerLats = centerPoints.map((point) => point.lat);
    const centerLon = (Math.min(...centerLons) + Math.max(...centerLons)) / 2;
    const centerLat = (Math.min(...centerLats) + Math.max(...centerLats)) / 2;
    const maxLonDelta = Math.max(...current.allPoints.map((point) => Math.abs(point.lon - centerLon)));
    const maxLatDelta = Math.max(...current.allPoints.map((point) => Math.abs(point.lat - centerLat)));
    const lonExtent = Math.max(0.02, maxLonDelta * 2.25);
    const latExtent = Math.max(0.02, maxLatDelta * 2.25);
    const paddedBounds = new maplibregl.LngLatBounds(
      [centerLon - lonExtent, centerLat - latExtent],
      [centerLon + lonExtent, centerLat + latExtent],
    );
    const camera = map.cameraForBounds(paddedBounds, {
      padding: { top: 24, right: 24, bottom: 24, left: 24 },
    });
    map.jumpTo({
      center: [centerLon, centerLat],
      zoom: camera?.zoom ?? map.getZoom(),
      bearing: 0,
      pitch: 0,
    });
  };

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: [DEFAULT_CENTER[1], DEFAULT_CENTER[0]],
      zoom: 7,
      attributionControl: false,
    });
    mapRef.current = map;

    // Keep the map as a static regional backdrop while preserving marker clicks.
    map.scrollZoom.disable();
    map.boxZoom.disable();
    map.dragPan.disable();
    map.dragRotate.disable();
    map.doubleClickZoom.disable();
    map.keyboard.disable();
    map.touchZoomRotate.disable();

    const empty: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] };
    const popup = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: true,
      offset: 14,
      maxWidth: '260px',
    });

    map.on('load', () => {
      map.addSource('owner-lines', { type: 'geojson', data: empty });
      map.addSource('owner-nodes', { type: 'geojson', data: empty });

      map.addLayer({ id: 'owner-lines-layer', type: 'line', source: 'owner-lines',
        paint: { 'line-color': '#00c4ff', 'line-width': 1.5, 'line-opacity': 0.6 } });
      map.addLayer({ id: 'owner-nodes-layer', type: 'circle', source: 'owner-nodes',
        paint: {
          'circle-radius': ['case', ['==', ['get', 'kind'], 'owner'], 8, 6.5],
          'circle-color': ['case', ['==', ['get', 'kind'], 'owner'], 'rgba(0,196,255,0.18)', 'rgba(255,179,0,0.18)'],
          'circle-stroke-width': 2,
          'circle-stroke-color': ['case', ['==', ['get', 'kind'], 'owner'], '#00c4ff', '#ffb300'],
        } });

      map.on('mouseenter', 'owner-nodes-layer', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'owner-nodes-layer', () => {
        map.getCanvas().style.cursor = '';
      });
      map.on('click', 'owner-nodes-layer', (event) => {
        const feature = event.features?.[0];
        if (!feature || feature.geometry.type !== 'Point') return;
        const coordinates = feature.geometry.coordinates.slice() as [number, number];
        const name = String(feature.properties?.name ?? 'Node');
        const details = String(feature.properties?.details ?? '');
        const content = document.createElement('div');
        content.className = 'node-popup';
        const title = document.createElement('div');
        title.className = 'node-popup__title';
        title.textContent = name;
        const meta = document.createElement('div');
        meta.className = 'node-popup__meta';
        meta.textContent = details;
        content.append(title, meta);
        popup
          .setLngLat(coordinates)
          .setDOMContent(content)
          .addTo(map);
      });

      loadedRef.current = true;
      applyData(map);
      requestAnimationFrame(() => applyData(map));
    });

    const resizeObserver = new ResizeObserver(() => {
      if (loadedRef.current) map.resize();
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      popup.remove();
      loadedRef.current = false;
      mapRef.current = null;
      map.remove();
    };
  // Map construction is intentionally stable. Live refreshes only update the
  // two GeoJSON sources in the data effect below.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (map && loadedRef.current) applyData(map);
  // applyData reads the current snapshot from dataRef and is intentionally not
  // a dependency; including it would turn every render into a source refresh.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ownerCoord, peers, allPoints]);

  return <div ref={containerRef} className="owner-map" />;
};


