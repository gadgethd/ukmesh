import type * as maplibregl from 'maplibre-gl';

export const DEFAULT_CENTER: [number, number] = [54.57, -1.23];
export const DEFAULT_ZOOM = 11;
// Weekly adverts remain comfortably fresh; retain a second two-week window so
// stale nodes are still visible as stale before the presence filter hides them.
export const NODE_STALE_AFTER_MS = 14 * 24 * 60 * 60 * 1000;
export const NODE_HIDE_AFTER_MS = 28 * 24 * 60 * 60 * 1000;
export const MAP_REFRESH_INTERVAL_MS = 250;
export const MAP_ARC_REFRESH_INTERVAL_MS = 100;

export const LINK_GREEN_THRESHOLD_DB = 121.5;
export const LINK_AMBER_THRESHOLD_DB = 129.5;

export const EMPTY_FC: GeoJSON.FeatureCollection = {
  type: 'FeatureCollection',
  features: [],
};

export const TERRAIN_CONFIG = { source: 'terrain-dem', exaggeration: 3 };

const CARTO_VECTOR_TILES = 'https://tiles.basemaps.cartocdn.com/vector/carto.streets/v1/tiles.json';
export const CARTO_GLYPHS = 'https://tiles.basemaps.cartocdn.com/fonts/{fontstack}/{range}.pbf';

type MapLabelColors = {
  place: string;
  water: string;
  road: string;
  halo: string;
};

export const MAP_LABEL_COLORS = {
  dark: {
    place: '#f8fafc',
    water: '#bfdbfe',
    road: '#e2e8f0',
    halo: '#080d14',
  },
  light: {
    place: '#1f2937',
    water: '#1d4ed8',
    road: '#334155',
    halo: '#ffffff',
  },
} as const;

// Overlay colours are independent of the raster basemap palette. Every solid
// marker and link colour meets the 3:1 non-text contrast threshold against its
// theme background (#080d14 dark, #edf2f7 light).
export const MAP_OVERLAY_COLORS = {
  dark: {
    repeater: '#00c4ff',
    companion: '#ff9f43',
    roomServer: '#a78bfa',
    sensor: '#34d399',
    replay: '#fbbf24',
    stale: '#94a3b8',
    clashRelay: '#22c55e',
    clashOffender: '#ef4444',
    nodeStroke: '#020617',
    selected: '#8af4ff',
    selectedStroke: '#ffffff',
    selectedHalo: '#22e0ff',
    linkUnknown: '#d1d5db',
    linkGood: '#22c55e',
    linkMarginal: '#fbbf24',
    linkPoor: '#ef4444',
    privacy: '#f59e0b',
    clashLine: '#f97316',
    coverageGood: '#22c55e',
    coverageMarginal: '#fbbf24',
    coveragePoor: '#ef4444',
    plannedGood: '#2dd4bf',
    plannedMarginal: '#818cf8',
    plannedPoor: '#c084fc',
    plannedOutline: '#22d3ee',
    plannedPending: '#94a3b8',
    dimmedOpacity: 0.72,
    staleOpacity: 0.7,
  },
  light: {
    repeater: '#006a8f',
    companion: '#9a3e00',
    roomServer: '#6d28d9',
    sensor: '#087f5b',
    replay: '#8a5d00',
    stale: '#4b5563',
    clashRelay: '#087b38',
    clashOffender: '#b91c1c',
    nodeStroke: '#ffffff',
    selected: '#005a7a',
    selectedStroke: '#111827',
    selectedHalo: '#006d8f',
    linkUnknown: '#475569',
    linkGood: '#087b38',
    linkMarginal: '#8a5d00',
    linkPoor: '#b91c1c',
    privacy: '#8a5d00',
    clashLine: '#a13d00',
    coverageGood: '#087b38',
    coverageMarginal: '#8a5d00',
    coveragePoor: '#b91c1c',
    plannedGood: '#0f766e',
    plannedMarginal: '#4338ca',
    plannedPoor: '#7e22ce',
    plannedOutline: '#036983',
    plannedPending: '#475569',
    dimmedOpacity: 0.8,
    staleOpacity: 0.7,
  },
} as const;

export type MapTheme = keyof typeof MAP_OVERLAY_COLORS;

// The no-label raster variants leave label rendering to the vector layers
// below, so labels stay readable in both themes instead of being baked into
// a tile with the wrong contrast.
export const MAP_RASTER_PAINT = {
  dark: {
    'raster-contrast': 0.18,
    'raster-brightness-min': 0.02,
    'raster-brightness-max': 0.92,
  },
  light: {
    'raster-contrast': 0.08,
    'raster-brightness-min': 0.04,
    'raster-brightness-max': 1,
  },
} as const;

const CARTO_LABEL_SOURCE: maplibregl.VectorSourceSpecification = {
  type: 'vector',
  url: CARTO_VECTOR_TILES,
};

const mapLabelLayers = (
  colors: MapLabelColors,
): maplibregl.LayerSpecification[] => [
  {
    id: 'map-labels-water',
    type: 'symbol',
    source: 'carto-labels',
    'source-layer': 'water_name',
    minzoom: 5,
    filter: ['all', ['has', 'name'], ['==', '$type', 'Point']],
    layout: {
      'text-field': ['coalesce', ['get', 'name_en'], ['get', 'name']],
      'text-font': ['Open Sans Regular', 'Noto Sans Regular'],
      'text-size': ['interpolate', ['linear'], ['zoom'], 5, 10, 10, 12, 14, 15],
      'text-padding': 2,
      'text-allow-overlap': false,
      'text-ignore-placement': false,
    },
    paint: {
      'text-color': colors.water,
      'text-halo-color': colors.halo,
      'text-halo-width': 1.6,
      'text-halo-blur': 0.1,
    },
  },
  {
    id: 'map-labels-place',
    type: 'symbol',
    source: 'carto-labels',
    'source-layer': 'place',
    minzoom: 5,
    filter: [
      'all',
      ['has', 'name'],
      ['==', '$type', 'Point'],
      ['in', 'class', 'city', 'town', 'village', 'suburb', 'hamlet', 'municipality'],
    ],
    layout: {
      'text-field': ['coalesce', ['get', 'name_en'], ['get', 'name']],
      'text-font': ['Open Sans Regular', 'Noto Sans Regular'],
      'text-size': ['interpolate', ['linear'], ['zoom'], 5, 9, 9, 10, 13, 13, 16, 15],
      'text-max-width': 10,
      'text-padding': 2,
      'text-allow-overlap': false,
      'text-ignore-placement': false,
    },
    paint: {
      'text-color': colors.place,
      'text-halo-color': colors.halo,
      'text-halo-width': 1.7,
      'text-halo-blur': 0.1,
    },
  },
  {
    id: 'map-labels-road',
    type: 'symbol',
    source: 'carto-labels',
    'source-layer': 'transportation_name',
    minzoom: 10,
    filter: ['all', ['has', 'name'], ['==', '$type', 'LineString']],
    layout: {
      'text-field': ['get', 'name'],
      'text-font': ['Open Sans Regular', 'Noto Sans Regular'],
      'text-size': ['interpolate', ['linear'], ['zoom'], 10, 8, 14, 10, 17, 12],
      'symbol-placement': 'line',
      'symbol-spacing': 300,
      'text-padding': 2,
      'text-max-angle': 30,
      'text-allow-overlap': false,
      'text-ignore-placement': false,
    },
    paint: {
      'text-color': colors.road,
      'text-halo-color': colors.halo,
      'text-halo-width': 1.4,
      'text-halo-blur': 0.1,
    },
  },
];

export const TERRAIN_DEM_SOURCE: maplibregl.RasterDEMSourceSpecification = {
  type: 'raster-dem',
  tiles: ['/terrain-tiles/{z}/{x}/{y}.png'],
  encoding: 'terrarium',
  tileSize: 512,
  minzoom: 5,
  maxzoom: 12,
};

export const MAP_STYLE: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    'carto-dark': {
      type: 'raster',
      tiles: [
        'https://a.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
        'https://b.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
        'https://c.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
        'https://d.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png',
      ],
      tileSize: 256,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
      maxzoom: 19,
    },
    'carto-labels': CARTO_LABEL_SOURCE,
  },
  glyphs: CARTO_GLYPHS,
  layers: [
    { id: 'bg-fill', type: 'background', paint: { 'background-color': '#080d14' } },
    { id: 'background', type: 'raster', source: 'carto-dark', paint: MAP_RASTER_PAINT.dark },
    ...mapLabelLayers(MAP_LABEL_COLORS.dark),
  ],
};

export const MAP_STYLE_LIGHT: maplibregl.StyleSpecification = {
  version: 8,
  sources: {
    'carto-light': {
      type: 'raster',
      tiles: [
        'https://a.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
        'https://b.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
        'https://c.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
        'https://d.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
      ],
      tileSize: 256,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
      maxzoom: 19,
    },
    'carto-labels': CARTO_LABEL_SOURCE,
  },
  glyphs: CARTO_GLYPHS,
  layers: [
    { id: 'bg-fill', type: 'background', paint: { 'background-color': '#edf2f7' } },
    { id: 'background', type: 'raster', source: 'carto-light', paint: MAP_RASTER_PAINT.light },
    ...mapLabelLayers(MAP_LABEL_COLORS.light),
  ],
};
