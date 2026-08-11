import {
  buildObserverBounds,
  decodePathWithScore,
  evaluatePhysicalHop,
  indexCandidatesByHash,
  scoreFixedPath,
  type CandidateNodeEvidence,
  type DecodedHop,
  type ObserverAnchor,
  type PathDecoderEvidence,
} from '../path-core/decoder.js';
import { pathingConfig } from '../platform/config/pathing.js';
import {
  AMBIG_DELTA,
  DIST_DECAY_KM,
  MAX_COL,
  ML_DOMINANT_THRESHOLD,
  NULL_BASELINE,
  SCORE,
} from '../path-shared/scoring.js';
import { nodePathHash, normalizePathHash } from '../path-hash/utils.js';
import { clamp, hasCoords, linkKey } from './geometry.js';
import type { BetaResolveContext, MeshNode, PathPacket } from './types.js';

export type BetaObserverEntry = {
  observerId: string;
  packet: PathPacket;
  rx: MeshNode;
  hashes: string[];
  hops: string[];
};

export type BetaCanonicalGroup = {
  canonicalHashes: string[];
  members: BetaObserverEntry[];
};

export type BetaSharedDecode = {
  canonicalHashes: string[];
  hops: ReadonlyMap<number, DecodedHop>;
  hopConfidences: ReadonlyMap<number, number>;
  score?: number;
  baselineScore?: number;
};

export const HELD_PATH_REFINEMENT_MARGIN = AMBIG_DELTA;

export type BetaPathProjection = {
  confidence: number;
  extraPurplePaths: [number, number][][];
  nodeIds: string[];
  purplePath: [number, number][] | null;
  remainingHops: number;
  resolvedHopCount: number;
};

function pathsArePrefixCompatible(a: readonly string[], b: readonly string[]): boolean {
  const length = Math.min(a.length, b.length);
  for (let position = 0; position < length; position++) {
    if (normalizePathHash(a[position]) !== normalizePathHash(b[position])) return false;
  }
  return true;
}

/** Group observer views exactly as the lazy resolver does: compatible prefixes share one decode. */
export function groupCompatibleObservations(entries: readonly BetaObserverEntry[]): BetaCanonicalGroup[] {
  const sorted = [...entries]
    .filter((entry) => entry.hops.length > 0)
    .sort((a, b) => b.hops.length - a.hops.length || a.observerId.localeCompare(b.observerId));
  const groups: BetaCanonicalGroup[] = [];
  for (const entry of sorted) {
    const existing = groups.find((group) => pathsArePrefixCompatible(group.canonicalHashes, entry.hops));
    if (existing) {
      existing.members.push(entry);
      if (entry.hops.length > existing.canonicalHashes.length) existing.canonicalHashes = [...entry.hops];
    } else {
      groups.push({ canonicalHashes: [...entry.hops], members: [entry] });
    }
  }
  return groups;
}

function currentHourBucket(bucketHours: number): number {
  return Math.floor(new Date().getUTCHours() / bucketHours);
}

function uniqueRegions(entries: readonly BetaObserverEntry[]): string[] {
  const regions = new Set(entries.map((entry) => entry.rx.iata ?? 'unknown'));
  regions.add('unknown');
  return [...regions];
}

function directAnchorMap(group: BetaCanonicalGroup): Map<string, ObserverAnchor[]> {
  const anchors = new Map<string, ObserverAnchor[]>();
  for (const entry of group.members) {
    if (!hasCoords(entry.rx)) continue;
    const position = entry.hops.length - 1;
    if (position < 0) continue;
    const hash = entry.hops[position];
    if (!hash || normalizePathHash(group.canonicalHashes[position]) !== normalizePathHash(hash)) continue;
    const key = `${position}:${normalizePathHash(hash)}`;
    const values = anchors.get(key) ?? [];
    if (!values.some((value) => value.nodeId === entry.observerId)) {
      values.push({
        lat: entry.rx.lat!,
        lon: entry.rx.lon!,
        nodeId: entry.observerId,
        elevationM: entry.rx.elevation_m,
      });
    }
    anchors.set(key, values);
  }
  return anchors;
}

function regionalMaximum(regions: readonly string[], read: (region: string) => number | undefined): number {
  let best = 0;
  for (const region of regions) best = Math.max(best, read(region) ?? 0);
  return best;
}

function marginConfidence(hop: DecodedHop): number {
  if (hop.nodeId === null) return 0;
  if (!Number.isFinite(hop.margin)) return 1;
  return clamp(1 - Math.exp(-Math.max(0, hop.margin) / Math.max(AMBIG_DELTA, 0.001)), 0, 1);
}

/** Build beta's evidence provider and invoke the repository-wide shared Viterbi decoder. */
export function decodeBetaCanonicalGroup(
  group: BetaCanonicalGroup,
  context: BetaResolveContext,
  stickyMap?: ReadonlyMap<string, string>,
  stickyAgeFraction?: number,
  fixed?: ReadonlyMap<number, string>,
  baselineNodeIds?: readonly string[],
): BetaSharedDecode {
  const canonicalHashes = group.canonicalHashes.map(normalizePathHash).filter(Boolean);
  const observerCoordinates = group.members
    .map((entry) => entry.rx)
    .filter(hasCoords)
    .map((node) => ({ lat: node.lat!, lon: node.lon! }));
  const bounds = buildObserverBounds(observerCoordinates, pathingConfig.maxHopKm);
  const candidateEvidence: CandidateNodeEvidence[] = context.repeaterNodes.map((node) => ({
    nodeId: node.node_id,
    name: node.name,
    lat: node.lat,
    lon: node.lon,
    elevationM: node.elevation_m,
  }));
  const candidatesByHash = indexCandidatesByHash(canonicalHashes, candidateEvidence, bounds);
  const anchors = directAnchorMap(group);
  const regions = uniqueRegions(group.members);
  const bucketHours = context.learningModel.bucketHours || 6;
  const bucket = currentHourBucket(bucketHours);
  const stickyStrength = clamp(1 - Number(stickyAgeFraction ?? 0), 0, 1);
  const sourceNode = group.members
    .map((entry) => entry.packet.src_node_id
      ? context.nodesById.get(entry.packet.src_node_id) ?? null
      : null)
    .find(hasCoords);

  const evidence: PathDecoderEvidence = {
    weights: SCORE,
    maxHopKm: pathingConfig.maxHopKm,
    distanceDecayKm: DIST_DECAY_KM,
    mlDominantThreshold: ML_DOMINANT_THRESHOLD,
    unresolvedBaseline: NULL_BASELINE,
    ambiguityDelta: AMBIG_DELTA,
    maxColumnCandidates: MAX_COL,
    candidatesByHash,
    fixed,
    sourceAnchor: sourceNode ? {
      lat: sourceNode.lat!,
      lon: sourceNode.lon!,
      nodeId: sourceNode.node_id,
      elevationM: sourceNode.elevation_m,
    } : undefined,
    endpointTransitionsGateOnly: true,
    directAnchors: (position, hash) => anchors.get(`${position}:${normalizePathHash(hash)}`) ?? [],
    prefixProbability: (nodeId, hash, previousHash) => {
      const prefix = normalizePathHash(hash);
      const previous = normalizePathHash(previousHash);
      const learned = regionalMaximum(regions, (region) => (
        context.learningModel.prefixProbabilities.get(`${region}|${prefix}|${previous}|${nodeId}`)
        ?? context.learningModel.prefixProbabilities.get(`${region}|${prefix}||${nodeId}`)
      ));
      const sticky = stickyMap?.get(prefix) === nodeId ? stickyStrength : 0;
      return Math.max(learned, sticky);
    },
    mlPrefixScore: () => 0,
    directedEdgeScore: (fromNodeId, toNodeId) => regionalMaximum(regions, (region) => (
      context.learningModel.edgeScores.get(`${region}|${bucket}|${fromNodeId}|${toNodeId}`)
      ?? context.learningModel.edgeScores.get(`${region}|-1|${fromNodeId}|${toNodeId}`)
    )),
    transitionProbability: (fromNodeId, toNodeId) => regionalMaximum(regions, (region) => (
      context.learningModel.transitionProbabilities.get(`${region}|${fromNodeId}|${toNodeId}`)
    )),
    motifProbability: (fromNodeId, toNodeId) => regionalMaximum(regions, (region) => (
      context.learningModel.motifProbabilities.get(`${region}|${bucket}|2|${fromNodeId}>${toNodeId}`)
      ?? context.learningModel.motifProbabilities.get(`${region}|-1|2|${fromNodeId}>${toNodeId}`)
    )),
    hasObservedLink: (fromNodeId, toNodeId) => {
      const metrics = context.linkMetrics.get(linkKey(fromNodeId, toNodeId));
      return Math.max(
        Number(metrics?.observed_count ?? 0),
        Number(metrics?.multibyte_observed_count ?? 0),
      ) >= 2;
    },
    physicalTransition: (from, to) => {
      const metrics = from.nodeId && to.nodeId
        ? context.linkMetrics.get(linkKey(from.nodeId, to.nodeId))
        : undefined;
      const physics = evaluatePhysicalHop(from, to, {
        maxHopKm: pathingConfig.maxHopKm,
        earthEffectiveRadiusM: pathingConfig.earthEffectiveRadiusM,
        behindEarthToleranceKm: pathingConfig.behindEarthToleranceKm,
        impossibleLinkPathlossDb: pathingConfig.impossibleLinkPathlossDb,
        pathlossDb: metrics?.itm_path_loss_db,
        antennaHeightM: pathingConfig.defaultAntennaHeightM,
      });
      return {
        possible: physics.possible,
        score: pathingConfig.physicsSoftMarginWeight * physics.softMargin,
      };
    },
  };

  const decoded = decodePathWithScore(canonicalHashes, evidence);
  const hops = decoded.hops;
  const hopConfidences = new Map<number, number>();
  for (const [position, hop] of hops) hopConfidences.set(position, marginConfidence(hop));
  return {
    canonicalHashes,
    hops,
    hopConfidences,
    score: decoded.score,
    ...(baselineNodeIds
      ? { baselineScore: scoreFixedPath(canonicalHashes, baselineNodeIds, evidence) }
      : {}),
  };
}

function hardHopPossible(from: MeshNode, to: MeshNode, context: BetaResolveContext): boolean {
  if (!hasCoords(from) || !hasCoords(to)) return false;
  return evaluatePhysicalHop(
    { lat: from.lat!, lon: from.lon!, elevationM: from.elevation_m },
    { lat: to.lat!, lon: to.lon!, elevationM: to.elevation_m },
    {
      maxHopKm: pathingConfig.maxHopKm,
      earthEffectiveRadiusM: pathingConfig.earthEffectiveRadiusM,
      behindEarthToleranceKm: pathingConfig.behindEarthToleranceKm,
      impossibleLinkPathlossDb: pathingConfig.impossibleLinkPathlossDb,
      pathlossDb: context.linkMetrics.get(linkKey(from.node_id, to.node_id))?.itm_path_loss_db,
      antennaHeightM: pathingConfig.defaultAntennaHeightM,
    },
  ).possible;
}

/** Check every held relay hop, including source and repeater→observer endpoints. */
export function heldPathIsPhysicallyPossible(
  group: BetaCanonicalGroup,
  context: BetaResolveContext,
  nodeIds: readonly string[],
): boolean {
  if (nodeIds.length !== group.canonicalHashes.length || nodeIds.length === 0) return false;
  const nodes = nodeIds.map((nodeId, position) => {
    const node = context.nodesById.get(nodeId);
    const hash = normalizePathHash(group.canonicalHashes[position]);
    return node && nodePathHash(node.node_id, hash) === hash ? node : null;
  });
  if (nodes.some((node) => !hasCoords(node))) return false;
  for (let position = 1; position < nodes.length; position++) {
    if (!hardHopPossible(nodes[position - 1]!, nodes[position]!, context)) return false;
  }
  for (const member of group.members) {
    const source = member.packet.src_node_id
      ? context.nodesById.get(member.packet.src_node_id)
      : undefined;
    if (source && !hardHopPossible(source, nodes[0]!, context)) return false;
    const terminalPosition = member.hops.length - 1;
    const terminalRelay = nodes[terminalPosition];
    if (!terminalRelay || !hardHopPossible(terminalRelay, member.rx, context)) return false;
  }
  return true;
}

export type HeldPathDecode = BetaSharedDecode & {
  held: boolean;
  physical: boolean;
  refined: boolean;
};

function materializeHeldDecode(
  group: BetaCanonicalGroup,
  context: BetaResolveContext,
  nodeIds: readonly string[],
): BetaSharedDecode {
  const canonicalHashes = group.canonicalHashes.map(normalizePathHash).filter(Boolean);
  const hops = new Map<number, DecodedHop>();
  const hopConfidences = new Map<number, number>();
  for (let position = 0; position < canonicalHashes.length; position++) {
    const node = context.nodesById.get(nodeIds[position]!);
    hops.set(position, {
      hash: canonicalHashes[position]!,
      nodeId: node?.node_id ?? null,
      name: node?.name ?? null,
      lat: node?.lat ?? null,
      lon: node?.lon ?? null,
      margin: Infinity,
      ambiguous: false,
    });
    hopConfidences.set(position, node ? 1 : 0);
  }
  return { canonicalHashes, hops, hopConfidences, score: 0 };
}

/** Hold a physical path and refine only the two observer-nearest positions. */
export function decodeBetaCanonicalGroupWithHeldPath(
  group: BetaCanonicalGroup,
  context: BetaResolveContext,
  heldNodeIds: readonly string[] | undefined,
  stickyMap?: ReadonlyMap<string, string>,
  stickyAgeFraction?: number,
): HeldPathDecode {
  if (!heldNodeIds || !heldPathIsPhysicallyPossible(group, context, heldNodeIds)) {
    return {
      ...decodeBetaCanonicalGroup(group, context, stickyMap, stickyAgeFraction),
      held: false,
      physical: false,
      refined: false,
    };
  }

  let current = [...heldNodeIds];
  let refined = false;
  const lastPosition = current.length - 1;
  for (let position = lastPosition; position >= Math.max(0, lastPosition - 1); position--) {
    const fixed = new Map<number, string>();
    current.forEach((nodeId, index) => { if (index !== position) fixed.set(index, nodeId); });
    const candidate = decodeBetaCanonicalGroup(
      group,
      context,
      stickyMap,
      stickyAgeFraction,
      fixed,
      current,
    );
    const candidateHop = candidate.hops.get(position);
    if (candidateHop?.nodeId
        && candidateHop.nodeId !== current[position]
        && Number.isFinite(candidate.score)
        && Number.isFinite(candidate.baselineScore)
        && candidate.score! >= candidate.baselineScore! + HELD_PATH_REFINEMENT_MARGIN) {
      current[position] = candidateHop.nodeId;
      refined = true;
    }
  }
  return {
    ...materializeHeldDecode(group, context, current),
    held: true,
    physical: true,
    refined,
  };
}

type PositionedPoint = {
  coordinate: [number, number];
  nodeId: string;
  position: number;
  receiver: boolean;
};

function shouldPrependSource(entry: BetaObserverEntry, source: MeshNode | null): source is MeshNode {
  if (!hasCoords(source)) return false;
  if (entry.packet.packet_type === 4) return true;
  const firstHash = entry.hops[0];
  return Boolean(firstHash && nodePathHash(source.node_id, firstHash) !== normalizePathHash(firstHash));
}

/** Project one observer's DTO path from its group's canonical decode without inventing fallback edges. */
export function projectCanonicalPathForObserver(
  entry: BetaObserverEntry,
  decoded: BetaSharedDecode,
  context: BetaResolveContext,
  source: MeshNode | null,
): BetaPathProjection {
  const points: PositionedPoint[] = [];
  if (shouldPrependSource(entry, source)) {
    points.push({
      coordinate: [source.lat!, source.lon!],
      nodeId: source.node_id,
      position: -1,
      receiver: false,
    });
  }

  let confidenceTotal = 0;
  let resolvedHopCount = 0;
  for (let position = 0; position < entry.hops.length; position++) {
    const hop = decoded.hops.get(position);
    confidenceTotal += decoded.hopConfidences.get(position) ?? 0;
    if (!hop || hop.nodeId === null || hop.lat === null || hop.lon === null) continue;
    resolvedHopCount += 1;
    points.push({
      coordinate: [hop.lat, hop.lon],
      nodeId: hop.nodeId,
      position,
      receiver: false,
    });
  }
  points.push({
    coordinate: [entry.rx.lat!, entry.rx.lon!],
    nodeId: entry.rx.node_id,
    position: entry.hops.length,
    receiver: true,
  });

  const rawSegments: Array<{ path: [number, number][]; receiver: boolean }> = [];
  let current: PositionedPoint[] = [];
  for (const point of points) {
    const previous = current[current.length - 1];
    if (!previous || point.position === previous.position + 1) {
      current.push(point);
      continue;
    }
    if (current.length >= 2) {
      rawSegments.push({ path: current.map((value) => value.coordinate), receiver: current.some((value) => value.receiver) });
    }
    current = [point];
  }
  if (current.length >= 2) {
    rawSegments.push({ path: current.map((value) => value.coordinate), receiver: current.some((value) => value.receiver) });
  }

  let primaryIndex = rawSegments.findIndex((segment) => segment.receiver);
  if (primaryIndex < 0 && rawSegments.length > 0) {
    primaryIndex = rawSegments.reduce(
      (best, segment, index) => segment.path.length > rawSegments[best]!.path.length ? index : best,
      0,
    );
  }
  const purplePath = primaryIndex >= 0 ? rawSegments[primaryIndex]!.path : null;
  const extraPurplePaths = rawSegments
    .filter((_, index) => index !== primaryIndex)
    .map((segment) => segment.path);
  const rawConfidence = entry.hops.length > 0 ? confidenceTotal / entry.hops.length : 0;
  const confidence = clamp(
    rawConfidence * context.learningModel.confidenceScale + context.learningModel.confidenceBias,
    0,
    1,
  );

  return {
    confidence,
    extraPurplePaths,
    nodeIds: [...new Set(points.map((point) => point.nodeId))],
    purplePath,
    remainingHops: Math.max(0, entry.hops.length - resolvedHopCount),
    resolvedHopCount,
  };
}
