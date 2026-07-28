import type { Filters } from '../components/FilterPanel/FilterPanel.js';

const BOOLEAN_FILTER_KEYS = [
  'livePackets',
  'links',
  'terrain',
  'clientNodes',
  'packetHistory',
  'heatmap',
  'betaPaths',
  'hexClashes',
] as const satisfies ReadonlyArray<keyof Filters>;

const FILTER_QUERY_NAMES: Record<(typeof BOOLEAN_FILTER_KEYS)[number], string> = {
  livePackets: 'feed',
  links: 'links',
  terrain: 'terrain',
  clientNodes: 'clients',
  packetHistory: 'paths',
  heatmap: 'heatmap',
  betaPaths: 'live-path',
  hexClashes: 'clashes',
};

export type InitialMapView = { lat: number; lon: number; zoom: number };

export function filtersFromUrl(defaults: Filters, search = window.location.search): Filters {
  const params = new URLSearchParams(search);
  if (!params.has('layers')) return defaults;
  const enabled = new Set((params.get('layers') ?? '').split(',').filter(Boolean));
  const next = { ...defaults };
  for (const key of BOOLEAN_FILTER_KEYS) next[key] = enabled.has(FILTER_QUERY_NAMES[key]);

  const clashHops = Number(params.get('clashHops'));
  if (Number.isInteger(clashHops) && clashHops >= 0 && clashHops <= 3) next.hexClashMaxHops = clashHops;
  return next;
}

export function writeFiltersToUrl(filters: Filters, mode: string | null): void {
  const url = new URL(window.location.href);
  const enabled = BOOLEAN_FILTER_KEYS.filter((key) => filters[key]).map((key) => FILTER_QUERY_NAMES[key]);
  url.searchParams.set('layers', enabled.join(','));
  if (filters.hexClashes) url.searchParams.set('clashHops', String(filters.hexClashMaxHops));
  else url.searchParams.delete('clashHops');
  if (mode) url.searchParams.set('mode', mode);
  else url.searchParams.delete('mode');
  window.history.replaceState(null, '', url);
}

export function initialMapViewFromUrl(search = window.location.search): InitialMapView | null {
  const value = new URLSearchParams(search).get('map');
  if (!value) return null;
  const [lat, lon, zoom] = value.split(',').map(Number);
  if (
    !Number.isFinite(lat) || !Number.isFinite(lon) || !Number.isFinite(zoom)
    || lat! < -90 || lat! > 90 || lon! < -180 || lon! > 180 || zoom! < 6 || zoom! > 19
  ) return null;
  return { lat: lat!, lon: lon!, zoom: zoom! };
}

export function writeMapViewToUrl(view: InitialMapView): void {
  const url = new URL(window.location.href);
  url.searchParams.set('map', `${view.lat.toFixed(5)},${view.lon.toFixed(5)},${view.zoom.toFixed(2)}`);
  window.history.replaceState(null, '', url);
}
