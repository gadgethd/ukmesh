/**
 * Bundled offline basemap.
 *
 * Used only when the remote OpenFreeMap style/tiles cannot be recovered. It is
 * intentionally tiny and dependency-free: Natural Earth land polygons clipped to
 * the UK/IE window (served from our own origin) plus DOM city labels. No glyph
 * PBFs, no third-party hosts — so it still renders when the map CDN is blocked.
 */
import * as maplibregl from 'maplibre-gl';
import type { StyleSpecification } from 'maplibre-gl';
import type { MapTheme } from './mapConfig.js';

export const FALLBACK_LAND_URL = '/basemap/uk-land.geojson';
export const FALLBACK_CITIES_URL = '/basemap/uk-cities.geojson';
export const FALLBACK_LABEL_CLASS = 'map-fallback-label';
/** Above this zoom the DOM labels crowd the view; the outline still shows. */
export const FALLBACK_LABEL_MAX_ZOOM = 10;

export function buildFallbackStyle(theme: MapTheme): StyleSpecification {
  const dark = theme === 'dark';
  return {
    version: 8,
    sources: {
      'fallback-land': {
        type: 'geojson',
        data: FALLBACK_LAND_URL,
      },
    },
    layers: [
      {
        id: 'fallback-background',
        type: 'background',
        paint: { 'background-color': dark ? '#080d14' : '#eef2f6' },
      },
      {
        id: 'fallback-land',
        type: 'fill',
        source: 'fallback-land',
        paint: {
          'fill-color': dark ? '#121a23' : '#fbfdff',
          'fill-opacity': 1,
        },
      },
      {
        id: 'fallback-coast',
        type: 'line',
        source: 'fallback-land',
        paint: {
          'line-color': dark ? '#2f4a63' : '#a7bacb',
          'line-width': ['interpolate', ['linear'], ['zoom'], 4, 0.7, 8, 1.4, 12, 2.2],
        },
      },
    ],
  };
}

let citiesPromise: Promise<GeoJSON.FeatureCollection> | null = null;

export function loadFallbackCities(): Promise<GeoJSON.FeatureCollection> {
  if (!citiesPromise) {
    citiesPromise = fetch(FALLBACK_CITIES_URL, { headers: { accept: 'application/geo+json' } })
      .then((response) => {
        if (!response.ok) throw new Error(`cities ${response.status}`);
        return response.json() as Promise<GeoJSON.FeatureCollection>;
      })
      .catch((error) => {
        citiesPromise = null;
        throw error;
      });
  }
  return citiesPromise;
}

function labelElement(name: string, rank: number): HTMLDivElement {
  const element = document.createElement('div');
  element.className = `${FALLBACK_LABEL_CLASS} ${FALLBACK_LABEL_CLASS}--rank-${Math.min(3, Math.max(0, rank))}`;
  element.textContent = name;
  element.setAttribute('aria-hidden', 'true');
  return element;
}

/**
 * Add city labels as DOM markers (no glyphs required) and hide them when the
 * view is zoomed in past {@link FALLBACK_LABEL_MAX_ZOOM}. Returns a cleanup fn.
 */
export function installFallbackMarkers(
  map: maplibregl.Map,
  cities: GeoJSON.FeatureCollection,
): () => void {
  const markers: maplibregl.Marker[] = [];
  for (const feature of cities.features) {
    if (feature.geometry.type !== 'Point') continue;
    const [lon, lat] = feature.geometry.coordinates as [number, number];
    const properties = (feature.properties ?? {}) as { name?: string; rank?: number };
    if (!properties.name) continue;
    const marker = new maplibregl.Marker({
      element: labelElement(properties.name, Number(properties.rank) || 0),
      anchor: 'center',
    })
      .setLngLat([lon, lat])
      .addTo(map);
    markers.push(marker);
  }

  const syncVisibility = () => {
    const visible = map.getZoom() < FALLBACK_LABEL_MAX_ZOOM;
    for (const marker of markers) marker.getElement().style.display = visible ? '' : 'none';
  };
  map.on('zoom', syncVisibility);
  syncVisibility();

  return () => {
    map.off('zoom', syncVisibility);
    for (const marker of markers) marker.remove();
  };
}
