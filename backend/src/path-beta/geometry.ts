import { R_EFF_M } from './constants.js';
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

export function distKm(a: MeshNode, b: MeshNode): number {
  const midLat = ((a.lat! + b.lat!) / 2) * (Math.PI / 180);
  const dlat = (a.lat! - b.lat!) * 111;
  const dlon = (a.lon! - b.lon!) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

export function hasLoS(a: MeshNode, b: MeshNode): boolean {
  const hA = Math.max(0, (a.elevation_m ?? 0)) + 5;
  const hB = Math.max(0, (b.elevation_m ?? 0)) + 5;
  const d = distKm(a, b) * 1000;
  if (d < 1) return true;
  // Radio horizon precheck: maximum geometric LoS distance for these antenna heights
  const maxLoSM = Math.sqrt(2 * R_EFF_M * hA) + Math.sqrt(2 * R_EFF_M * hB);
  if (d > maxLoSM) return false;
  // Check LoS line clears Earth bulge at intermediate points
  for (let i = 1; i < 20; i++) {
    const t = i / 20;
    const x = t * d;
    const los = hA + (hB - hA) * t;
    const bulge = x * (d - x) / (2 * R_EFF_M);
    if (los < bulge) return false;
  }
  return true;
}

export function nodeRange(nodeId: string, coverageByNode: Map<string, number>): number {
  const radiusM = coverageByNode.get(nodeId);
  if (!radiusM) return 50;
  return Math.min(80, Math.max(50, radiusM / 1000));
}

export function canReach(a: MeshNode, b: MeshNode, coverageByNode: Map<string, number>): boolean {
  const threshold = Math.max(nodeRange(a.node_id, coverageByNode), nodeRange(b.node_id, coverageByNode));
  return distKm(a, b) < threshold;
}

export function cosine2d(ax: number, ay: number, bx: number, by: number): number {
  const mag = Math.hypot(ax, ay) * Math.hypot(bx, by);
  return mag > 1e-9 ? clamp((ax * bx + ay * by) / mag, -1, 1) : 0;
}

export function sourceProgressScore(candidate: MeshNode, prev: MeshNode, src: MeshNode | null): number {
  if (!hasCoords(src)) return 0;
  const toSrcX = src.lon! - prev.lon!;
  const toSrcY = src.lat! - prev.lat!;
  const toCandidateX = candidate.lon! - prev.lon!;
  const toCandidateY = candidate.lat! - prev.lat!;
  const align = cosine2d(toCandidateX, toCandidateY, toSrcX, toSrcY);
  const prevToSrc = distKm(prev, src);
  const candidateToSrc = distKm(candidate, src);
  const progress = prevToSrc > 1e-6 ? clamp((prevToSrc - candidateToSrc) / prevToSrc, -1, 1) : 0;
  return align * 0.7 + progress * 0.6;
}

export function turnContinuityScore(candidate: MeshNode, prev: MeshNode, nextTowardRx: MeshNode | null): number {
  if (!hasCoords(nextTowardRx)) return 0;
  const incomingX = prev.lon! - candidate.lon!;
  const incomingY = prev.lat! - candidate.lat!;
  const outgoingX = nextTowardRx.lon! - prev.lon!;
  const outgoingY = nextTowardRx.lat! - prev.lat!;
  return cosine2d(incomingX, incomingY, outgoingX, outgoingY);
}
