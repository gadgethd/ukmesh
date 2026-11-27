import type { MeshNode } from '../types/domain.js';
import { PROHIBITED_NODE_MARKER, hasCoords, isValidMapCoord } from './pathing.js';
import { hashWithRegion } from './regionUrlState.js';

export function isPublicRepeater(node: MeshNode): boolean {
  return !node.name?.includes(PROHIBITED_NODE_MARKER) && (node.role == null || node.role === 2);
}

/** Rank public repeater positions using device coordinates held only in memory. */
export function nearestRepeaters(nodes: readonly MeshNode[], { latitude, longitude }: Pick<GeolocationCoordinates, 'latitude' | 'longitude'>, limit: number): MeshNode[] {
  const radians = Math.PI / 180;
  return nodes.filter(isPublicRepeater).filter(hasCoords).map(node => {
    const latitudeDifference = (node.lat - latitude) * radians / 2;
    const longitudeDifference = (node.lon - longitude) * radians / 2;
    const score = Math.sin(latitudeDifference) ** 2
      + Math.cos(latitude * radians) * Math.cos(node.lat * radians) * Math.sin(longitudeDifference) ** 2;
    return { node, score };
  }).sort((a, b) => a.score - b.score).slice(0, limit).map(({ node }) => node);
}

export function liveMapLink(base: string, { region, node }: { region?: string; node?: {
  id: string; lat?: number | null; lon?: number | null;
} }): string {
  const url = new URL(base);
  if (node) {
    url.searchParams.set('node', node.id);
    if (node.lat != null && node.lon != null && isValidMapCoord(node.lat, node.lon)) {
      url.searchParams.set('map', `${node.lat.toFixed(5)},${node.lon.toFixed(5)},13`);
    }
  }
  if (region) url.hash = hashWithRegion(url.hash, region);
  return url.href;
}
