import type { MeshNode } from './types.js';

export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function isValidMapCoord(lat: number | null | undefined, lon: number | null | undefined): boolean {
  if (typeof lat !== 'number' || typeof lon !== 'number') return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (Math.abs(lat) < 5 && Math.abs(lon) < 5) return false;
  return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

export function hasCoords(n: MeshNode | null | undefined): n is MeshNode {
  return Boolean(n && isValidMapCoord(n.lat, n.lon));
}

export function linkKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}
