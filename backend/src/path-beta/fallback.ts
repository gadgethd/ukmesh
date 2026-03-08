import { LOOSE_LINK_PATHLOSS_MAX_DB, SOFT_FALLBACK_HOP_KM, WEAK_LINK_PATHLOSS_MAX_DB } from './constants.js';
import { canReach, clamp, distKm, hasLoS, linkKey, sourceProgressScore, turnContinuityScore } from './geometry.js';
import type { LinkMetrics, MeshNode } from './types.js';

export function isWeakOrBetter(meta: LinkMetrics | undefined): boolean {
  const pathLoss = meta?.itm_path_loss_db;
  return pathLoss != null && pathLoss <= WEAK_LINK_PATHLOSS_MAX_DB;
}

export function isLooseOrBetter(meta: LinkMetrics | undefined): boolean {
  const pathLoss = meta?.itm_path_loss_db;
  return pathLoss != null && pathLoss <= LOOSE_LINK_PATHLOSS_MAX_DB;
}

export function fallbackEdgeAllowed(
  a: MeshNode,
  b: MeshNode,
  coverageByNode: Map<string, number>,
  linkMetrics: Map<string, LinkMetrics>,
): boolean {
  const meta = linkMetrics.get(linkKey(a.node_id, b.node_id));
  const reachOk = canReach(a, b, coverageByNode);
  const losOk = hasLoS(a, b);
  return (reachOk && losOk) || (reachOk && isLooseOrBetter(meta)) || isWeakOrBetter(meta);
}

export function fallbackEdgeScore(
  candidate: MeshNode,
  prev: MeshNode,
  src: MeshNode | null,
  nextTowardRx: MeshNode | null,
  coverageByNode: Map<string, number>,
  linkMetrics: Map<string, LinkMetrics>,
): number {
  const meta = linkMetrics.get(linkKey(candidate.node_id, prev.node_id));
  const pathLoss = meta?.itm_path_loss_db;
  const reachOk = canReach(candidate, prev, coverageByNode);
  const losOk = hasLoS(candidate, prev);
  const distPenalty = distKm(candidate, prev) / 50;
  const pathLossBoost = pathLoss == null ? 0 : clamp((LOOSE_LINK_PATHLOSS_MAX_DB - pathLoss) / 18, 0, 1.2);
  const observedBoost = Math.min(0.35, Math.log10((meta?.observed_count ?? 0) + 1) * 0.18);
  const physicalBoost = (reachOk ? 0.45 : 0) + (losOk ? 0.2 : 0);
  const directionalBoost = sourceProgressScore(candidate, prev, src) * 0.45
    + turnContinuityScore(candidate, prev, nextTowardRx) * 0.6;
  return directionalBoost + pathLossBoost + observedBoost + physicalBoost - distPenalty;
}

export function compareFallbackCandidates(
  a: MeshNode,
  b: MeshNode,
  prev: MeshNode,
  src: MeshNode | null,
  nextTowardRx: MeshNode | null,
  coverageByNode: Map<string, number>,
  linkMetrics: Map<string, LinkMetrics>,
): number {
  const distA = distKm(a, prev);
  const distB = distKm(b, prev);
  if (Math.abs(distA - distB) > 0.25) return distA - distB;
  return fallbackEdgeScore(b, prev, src, nextTowardRx, coverageByNode, linkMetrics)
    - fallbackEdgeScore(a, prev, src, nextTowardRx, coverageByNode, linkMetrics);
}

export function softFallbackCandidateAllowed(
  candidate: MeshNode,
  prev: MeshNode,
  src: MeshNode | null,
  nextTowardRx: MeshNode | null,
): boolean {
  const hopKm = distKm(candidate, prev);
  if (hopKm > SOFT_FALLBACK_HOP_KM) return false;
  const sourceProgress = sourceProgressScore(candidate, prev, src);
  const turnContinuity = turnContinuityScore(candidate, prev, nextTowardRx);
  return sourceProgress >= -0.15 && turnContinuity >= -0.35;
}

export function samePoint(a: [number, number], b: [number, number], epsilon = 1e-4): boolean {
  return Math.abs(a[0] - b[0]) <= epsilon && Math.abs(a[1] - b[1]) <= epsilon;
}

export function trimRedToPurpleStitch(
  redPath: [number, number][] | null,
  purplePath: [number, number][] | null,
): [number, number][] | null {
  if (!redPath || redPath.length < 2) return null;
  if (!purplePath || purplePath.length < 2) return redPath;
  const stitch = purplePath[0];
  if (!stitch) return redPath;
  const idx = redPath.findIndex((point) => samePoint(point, stitch));
  if (idx <= 0) return redPath;
  const trimmed = redPath.slice(0, idx + 1);
  return trimmed.length >= 2 ? trimmed : null;
}

export function trimPathToStartStitch(
  path: [number, number][] | null,
  startStitch: [number, number] | null,
): [number, number][] | null {
  if (!path || path.length < 2 || !startStitch) return path;
  const idx = path.findIndex((point) => samePoint(point, startStitch));
  if (idx < 0 || idx >= path.length - 1) return path;
  const trimmed = path.slice(idx);
  return trimmed.length >= 2 ? trimmed : null;
}

export function trimPathBetweenStitches(
  path: [number, number][] | null,
  startStitch: [number, number] | null,
  endPath: [number, number][] | null,
): [number, number][] | null {
  return trimRedToPurpleStitch(trimPathToStartStitch(path, startStitch), endPath);
}

export function retargetRedPathStart(
  path: [number, number][] | null,
  startStitch: [number, number] | null,
): [number, number][] | null {
  if (!path || path.length < 2 || !startStitch) return path;
  const first = path[0];
  if (!first) return path;
  if (samePoint(first, startStitch)) return path;
  const rewritten: [number, number][] = [startStitch, ...path.slice(1)];
  return rewritten.length >= 2 ? rewritten : null;
}

export function segmentizePath(path: [number, number][] | null): Array<[[number, number], [number, number]]> {
  if (!path || path.length < 2) return [];
  const segments: Array<[[number, number], [number, number]]> = [];
  for (let i = 0; i < path.length - 1; i++) {
    const a = path[i];
    const b = path[i + 1];
    if (!a || !b) continue;
    segments.push([a, b]);
  }
  return segments;
}
