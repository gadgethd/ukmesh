import { useEffect, useRef, useState } from 'react';
import type { ImageSource, Map as MapLibreMap } from 'maplibre-gl';
import { Map, Marker, NavigationControl, Popup } from './maplibre.js';
import { buildFallbackStyle } from './basemapFallback.js';
import type { TropoBounds, TropoPeak } from '../../pages/tropoModel.js';

type TropoMapProps = {
  bounds: TropoBounds;
  imageUrl: string;
  peak: TropoPeak;
  maxIndex: number;
};

function imageCoordinates(bounds: TropoBounds): [[number, number], [number, number], [number, number], [number, number]] {
  return [
    [bounds.west, bounds.north],
    [bounds.east, bounds.north],
    [bounds.east, bounds.south],
    [bounds.west, bounds.south],
  ];
}

function frameGeoJson(bounds: TropoBounds): GeoJSON.Feature<GeoJSON.Polygon> {
  return {
    type: 'Feature',
    properties: {},
    geometry: {
      type: 'Polygon',
      coordinates: [[
        [bounds.west, bounds.north],
        [bounds.east, bounds.north],
        [bounds.east, bounds.south],
        [bounds.west, bounds.south],
        [bounds.west, bounds.north],
      ]],
    },
  };
}

export function TropoMap({ bounds, imageUrl, peak, maxIndex }: TropoMapProps): React.ReactElement {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const markerRef = useRef<Marker | null>(null);
  const popupRef = useRef<Popup | null>(null);
  const imageUrlRef = useRef(imageUrl);
  const boundsRef = useRef(bounds);
  const [ready, setReady] = useState(false);

  imageUrlRef.current = imageUrl;
  boundsRef.current = bounds;

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return undefined;
    const map = new Map({
      container: containerRef.current,
      style: buildFallbackStyle('dark'),
      center: [-2.4, 54.5],
      zoom: 5.1,
      minZoom: 4,
      maxZoom: 9,
      attributionControl: false,
      cooperativeGestures: false,
    });
    mapRef.current = map;
    map.addControl(new NavigationControl({ showCompass: false }), 'bottom-right');

    let fitted = false;
    const install = () => {
      if (!map.getStyle()) return;
      const boundsForImage = boundsRef.current;
      if (!fitted) {
        map.fitBounds(
          [[boundsForImage.west, boundsForImage.south], [boundsForImage.east, boundsForImage.north]],
          { padding: 26, duration: 0 },
        );
        fitted = true;
      }
      if (!map.getSource('tropo')) {
        map.addSource('tropo', {
          type: 'image',
          url: imageUrlRef.current,
          coordinates: imageCoordinates(boundsForImage),
        });
      }
      if (!map.getLayer('tropo')) {
        map.addLayer({
          id: 'tropo',
          type: 'raster',
          source: 'tropo',
          paint: {
            'raster-opacity': 0.82,
            'raster-fade-duration': 160,
            'raster-resampling': 'linear',
          },
        });
      }
      if (!map.getSource('tropo-frame')) {
        map.addSource('tropo-frame', { type: 'geojson', data: frameGeoJson(boundsForImage) });
      }
      if (!map.getLayer('tropo-frame')) {
        map.addLayer({
          id: 'tropo-frame',
          type: 'line',
          source: 'tropo-frame',
          paint: {
            'line-color': '#00c4ff',
            'line-width': 1,
            'line-opacity': 0.18,
            'line-dasharray': [3, 3],
          },
        });
      }
      setReady(true);
    };
    map.on('style.load', install);
    map.on('load', install);

    return () => {
      map.off('style.load', install);
      map.off('load', install);
      markerRef.current?.remove();
      markerRef.current = null;
      popupRef.current = null;
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!ready || !mapRef.current) return;
    const map = mapRef.current;
    const source = map.getSource('tropo') as ImageSource | undefined;
    if (!source) return;
    const image = new Image();
    image.onload = () => {
      const activeSource = map.getSource('tropo') as ImageSource | undefined;
      activeSource?.updateImage({
        url: imageUrl,
        coordinates: imageCoordinates(boundsRef.current),
      });
    };
    image.src = imageUrl;
  }, [imageUrl, ready]);

  useEffect(() => {
    if (!ready || !mapRef.current) return;
    const map = mapRef.current;
    if (!markerRef.current) {
      const element = document.createElement('div');
      element.className = 'tropo-peak';
      element.setAttribute('aria-hidden', 'true');
      markerRef.current = new Marker({ element, anchor: 'center' })
        .setLngLat([peak.lon, peak.lat])
        .addTo(map);
      popupRef.current = new Popup({ offset: 16, closeButton: false, closeOnClick: false })
        .setHTML(`<strong>Peak ducting potential</strong><br/>Index ${Math.round(maxIndex)}`);
      markerRef.current.setPopup(popupRef.current);
    }
    markerRef.current.setLngLat([peak.lon, peak.lat]);
    markerRef.current.getElement().style.opacity = maxIndex >= 40 ? '1' : '0';
    popupRef.current?.setHTML(
      `<strong>Peak ducting potential</strong><br/>Index ${Math.round(maxIndex)} · ${peak.lat.toFixed(2)}°N, ${Math.abs(peak.lon).toFixed(2)}°${peak.lon >= 0 ? 'E' : 'W'}`,
    );
  }, [peak, maxIndex, ready]);

  return <div ref={containerRef} className="tropo-map" aria-label="Tropospheric ducting forecast map" />;
}
