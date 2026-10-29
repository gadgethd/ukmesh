/**
 * Basemap labels.
 *
 * Two jobs:
 * 1. `installUkPlaceLabels` adds a small UK/IE city layer for low zooms, where
 *    the OpenFreeMap dark style is sparse (country labels stop at z6, city
 *    labels thin out fast). Runs on our own GeoJSON, so it costs no tiles.
 * 2. `applyBasemapLabelTheme` lifts the style's own place/water labels out of
 *    near-invisibility on the dark basemap (default text is rgb(101,101,101)).
 */
import type * as maplibregl from 'maplibre-gl';
import type { MapTheme } from './mapConfig.js';
import { loadFallbackCities } from './basemapFallback.js';

export const UK_PLACE_SOURCE_ID = 'uk-place-labels';
export const UK_PLACE_LAYER_ID = 'uk-place-labels';
const UK_PLACE_MIN_ZOOM = 4.5;
const UK_PLACE_MAX_ZOOM = 9.6;

const LABEL_THEME = {
  dark: {
    text: '#c3ccd6',
    halo: 'rgba(0,0,0,0.88)',
    water: '#8fa3b8',
    haloWidth: 1.5,
  },
  light: {
    text: '#33414f',
    halo: 'rgba(255,255,255,0.92)',
    water: '#4a6478',
    haloWidth: 1.5,
  },
} as const satisfies Record<MapTheme, { text: string; halo: string; water: string; haloWidth: number }>;

function pickFontStack(map: maplibregl.Map): string[] {
  const layers = map.getStyle()?.layers ?? [];
  for (const layer of layers) {
    if (layer.type !== 'symbol') continue;
    const font = (layer.layout as Record<string, unknown> | undefined)?.['text-font'];
    if (Array.isArray(font) && font.length > 0 && font.every((entry) => typeof entry === 'string')) {
      return font as string[];
    }
  }
  return ['Noto Sans Regular'];
}

/** Add the low-zoom UK city layer. Safe to call on every style load. */
export async function installUkPlaceLabels(map: maplibregl.Map, theme: MapTheme): Promise<void> {
  if (map.getSource(UK_PLACE_SOURCE_ID)) return;
  let cities: GeoJSON.FeatureCollection;
  try {
    cities = await loadFallbackCities();
  } catch {
    return;
  }
  if (map.getSource(UK_PLACE_SOURCE_ID)) return;
  const colors = LABEL_THEME[theme];
  const fontStack = pickFontStack(map);
  const beforeId = map.getLayer('node-dots') ? 'node-dots' : undefined;
  try {
    map.addSource(UK_PLACE_SOURCE_ID, {
      type: 'geojson',
      data: cities,
      attribution: 'Natural Earth',
    });
    map.addLayer({
      id: UK_PLACE_LAYER_ID,
      type: 'symbol',
      source: UK_PLACE_SOURCE_ID,
      minzoom: UK_PLACE_MIN_ZOOM,
      maxzoom: UK_PLACE_MAX_ZOOM,
      layout: {
        'text-field': ['get', 'name'],
        'text-font': fontStack,
        'text-size': ['interpolate', ['linear'], ['zoom'], 4.5, 10, 6, 11.5, 8, 13, 9.6, 14],
        'text-allow-overlap': false,
        'text-ignore-placement': false,
        'text-padding': 6,
        'symbol-sort-key': ['get', 'rank'],
      },
      paint: {
        'text-color': colors.text,
        'text-halo-color': colors.halo,
        'text-halo-width': colors.haloWidth,
      },
    }, beforeId);
  } catch {
    // A concurrent style swap can remove the source between the guards; the
    // next style.load re-installs it.
  }
}

/** Recolour the style's own labels for legibility. Idempotent per style load. */
export function applyBasemapLabelTheme(map: maplibregl.Map, theme: MapTheme): void {
  const colors = LABEL_THEME[theme];
  const layers = map.getStyle()?.layers ?? [];
  const seen = new Set<string>();
  for (const layer of layers) {
    const id = layer.id;
    if (seen.has(id)) continue;
    seen.add(id);
    if (!map.getLayer(id)) continue;
    const isWater = id === 'water_name';
    const isPlace = id.startsWith('place_');
    const isRoad = id.startsWith('highway_name');
    if (!isWater && !isPlace && !isRoad) continue;
    try {
      map.setPaintProperty(id, 'text-color', isWater ? colors.water : colors.text);
      map.setPaintProperty(id, 'text-halo-color', colors.halo);
      map.setPaintProperty(id, 'text-halo-width', colors.haloWidth);
    } catch {
      // Layer type without text paint (or removed mid-iteration) — ignore.
    }
  }
}
