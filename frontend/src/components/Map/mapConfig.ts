import type * as maplibregl from 'maplibre-gl';

export const OPENFREEMAP_STYLE_DARK = 'https://tiles.openfreemap.org/styles/dark';
export const OPENFREEMAP_STYLE_LIGHT = 'https://tiles.openfreemap.org/styles/positron';

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

export const TERRAIN_DEM_SOURCE: maplibregl.RasterDEMSourceSpecification = {
  type: 'raster-dem',
  tiles: ['/terrain-tiles/{z}/{x}/{y}.png'],
  encoding: 'terrarium',
  tileSize: 256,
  maxzoom: 12,
};

export const MAP_STYLE = OPENFREEMAP_STYLE_DARK;
export const MAP_STYLE_LIGHT = OPENFREEMAP_STYLE_LIGHT;
