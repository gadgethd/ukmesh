/**
 * Evidence-driven Viterbi decoder shared by every path-resolution surface.
 *
 * This module is deliberately database-agnostic. Callers load candidate nodes,
 * priors, ML scores, observer anchors, and optional experimental evidence, then
 * expose them through {@link PathDecoderEvidence}. The decoder owns geographic
 * candidate bounding/capping, emissions, transitions, the unresolved baseline,
 * max-product trellis traversal, and per-position margin/ambiguity calculation.
 */
import {
  OBSERVER_DISTANCE_DECAY_KM,
  type PathScoreWeights,
} from '../path-shared/scoring.js';

export type GeographicPoint = { lat: number; lon: number };

export type GeographicBounds = {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
};

export type CandidateNode = {
  nodeId: string;
  name: string | null;
  lat: number;
  lon: number;
};

export type CandidateNodeEvidence = {
  nodeId: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
};

export type ObserverAnchor = GeographicPoint & { nodeId?: string };

export type DecodedHop = {
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  /** Best-chain score gap to the next-best candidate at this position. */
  margin: number;
  ambiguous: boolean;
};

export type PathDecoderEvidence = {
  weights: PathScoreWeights;
  maxHopKm: number;
  distanceDecayKm: number;
  mlDominantThreshold: number;
  unresolvedBaseline: number;
  ambiguityDelta: number;
  maxColumnCandidates: number;
  candidatesByHash: ReadonlyMap<string, readonly CandidateNode[]>;
  directAnchors: (position: number, hash: string) => readonly ObserverAnchor[];
  sourceAnchor?: ObserverAnchor;
  terminalAnchors?: readonly ObserverAnchor[];
  prefixProbability: (nodeId: string, hash: string, prevHash: string | null) => number;
  positionPrefixCount?: (position: number, nodeId: string, hash: string) => number;
  corridorPriorCount?: (position: number, nodeId: string) => number;
  observerDistance?: (candidate: CandidateNode) => number;
  mlPrefixScore: (hash: string, nodeId: string) => number;
  directedEdgeScore: (fromNodeId: string, toNodeId: string) => number;
  transitionProbability: (fromNodeId: string, toNodeId: string) => number;
  motifProbability: (fromNodeId: string, toNodeId: string) => number;
  hasObservedLink: (fromNodeId: string, toNodeId: string) => boolean;
  /** Optional Phase-4 emission evidence. Return a normalized candidate fit. */
  corridorInterpolation?: (
    position: number,
    pathLength: number,
    candidate: CandidateNode,
  ) => number;
  /** Optional Phase-4 transition evidence for (position, from)→to. */
  positionConditionalTransition?: (
    position: number,
    fromNodeId: string,
    toNodeId: string,
  ) => number;
  /** Optional Phase-4 bonus evidence. False is neutral, never a hard gate. */
  isItmViable?: (fromNodeId: string, toNodeId: string) => boolean;
};

type DecoderCandidate = {
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
};

const UNRESOLVED_CANDIDATE: DecoderCandidate = {
  nodeId: null,
  name: null,
  lat: null,
  lon: null,
};

export function distanceKm(a: GeographicPoint, b: GeographicPoint): number {
  const earthRadiusKm = 6371;
  const dLat = (b.lat - a.lat) * (Math.PI / 180);
  const dLon = (b.lon - a.lon) * (Math.PI / 180);
  const sinLat = Math.sin(dLat / 2) ** 2;
  const sinLon = Math.sin(dLon / 2) ** 2;
  const cosA = Math.cos(a.lat * (Math.PI / 180));
  const cosB = Math.cos(b.lat * (Math.PI / 180));
  return 2 * earthRadiusKm * Math.asin(Math.sqrt(sinLat + cosA * cosB * sinLon));
}

function minDistanceToSet(pt: GeographicPoint, anchors: readonly GeographicPoint[]): number {
  let min = Infinity;
  for (const anchor of anchors) min = Math.min(min, distanceKm(pt, anchor));
  return min;
}

function isInBounds(pt: GeographicPoint, bounds: GeographicBounds): boolean {
  return pt.lat >= bounds.minLat && pt.lat <= bounds.maxLat
    && pt.lon >= bounds.minLon && pt.lon <= bounds.maxLon;
}

/** Build the observer-region bound used to reject geographically remote candidates. */
export function buildObserverBounds(
  observerPositions: readonly GeographicPoint[],
  paddingKm: number,
): GeographicBounds | null {
  if (observerPositions.length === 0) return null;
  const lats = observerPositions.map((position) => position.lat);
  const lons = observerPositions.map((position) => position.lon);
  const padLat = paddingKm / 111;
  const midLat = (Math.min(...lats) + Math.max(...lats)) / 2;
  const padLon = paddingKm / (111 * Math.cos(midLat * (Math.PI / 180)));
  return {
    minLat: Math.min(...lats) - padLat,
    maxLat: Math.max(...lats) + padLat,
    minLon: Math.min(...lons) - padLon,
    maxLon: Math.max(...lons) + padLon,
  };
}

/**
 * Convert loaded candidate evidence into hash-indexed decoder candidates while
 * applying the same coordinate and observer-region gates as the lazy resolver.
 */
export function indexCandidatesByHash(
  hashes: readonly string[],
  candidates: readonly CandidateNodeEvidence[],
  bounds: GeographicBounds | null,
): Map<string, CandidateNode[]> {
  const result = new Map<string, CandidateNode[]>();
  for (const candidate of candidates) {
    if (candidate.lat == null || candidate.lon == null
        || candidate.lat === 0 || candidate.lon === 0) continue;
    const point = { lat: candidate.lat, lon: candidate.lon };
    if (bounds && !isInBounds(point, bounds)) continue;
    const upperNodeId = candidate.nodeId.toUpperCase();
    for (const hash of hashes) {
      if (!upperNodeId.startsWith(hash)) continue;
      if (!result.has(hash)) result.set(hash, []);
      result.get(hash)!.push({
        nodeId: candidate.nodeId,
        name: candidate.name,
        lat: candidate.lat,
        lon: candidate.lon,
      });
      break;
    }
  }
  return result;
}

function emissionScore(
  position: number,
  pathLength: number,
  hash: string,
  prevHash: string | null,
  candidate: DecoderCandidate,
  anchors: readonly ObserverAnchor[],
  evidence: PathDecoderEvidence,
): number {
  if (candidate.nodeId === null) return evidence.unresolvedBaseline;

  const positioned = candidate.lat != null && candidate.lon != null;
  if (anchors.length > 0) {
    if (!positioned) return -Infinity;
    if (minDistanceToSet({ lat: candidate.lat!, lon: candidate.lon! }, anchors) > evidence.maxHopKm) {
      return -Infinity;
    }
  }

  let score = evidence.weights.prefix
    * Math.log1p(Math.max(0, evidence.prefixProbability(candidate.nodeId, hash, prevHash)));
  const mlScore = evidence.mlPrefixScore(hash, candidate.nodeId);
  score += mlScore >= evidence.mlDominantThreshold
    ? Math.min(evidence.weights.mlDominantCap, mlScore * evidence.weights.mlDominantCap)
    : Math.min(evidence.weights.mlWeakCap, mlScore * evidence.weights.mlWeakCap);

  if (anchors.length > 0 && positioned) {
    const distance = minDistanceToSet({ lat: candidate.lat!, lon: candidate.lon! }, anchors);
    score -= evidence.weights.anchor * (distance / OBSERVER_DISTANCE_DECAY_KM);
  }

  if (positioned && evidence.weights.observerDistance !== 0 && evidence.observerDistance) {
    score += evidence.weights.observerDistance
      * evidence.observerDistance(candidate as CandidateNode);
  }

  if (evidence.weights.positionPrefixFrequency !== 0 && evidence.positionPrefixCount) {
    score += evidence.weights.positionPrefixFrequency
      * Math.log1p(Math.max(0, evidence.positionPrefixCount(position, candidate.nodeId, hash)));
  }
  if (evidence.weights.corridorPrior !== 0 && evidence.corridorPriorCount) {
    score += evidence.weights.corridorPrior
      * Math.log1p(Math.max(0, evidence.corridorPriorCount(position, candidate.nodeId)));
  }

  if (evidence.weights.corridorInterpolation !== 0 && evidence.corridorInterpolation) {
    score += evidence.weights.corridorInterpolation
      * evidence.corridorInterpolation(position, pathLength, candidate as CandidateNode);
  }
  return score;
}

function transitionScore(
  position: number,
  previous: DecoderCandidate,
  current: DecoderCandidate,
  evidence: PathDecoderEvidence,
): number {
  if (previous.nodeId === null || current.nodeId === null) return 0;
  let score = 0;
  if (previous.lat != null && previous.lon != null && current.lat != null && current.lon != null) {
    const distance = distanceKm(
      { lat: previous.lat, lon: previous.lon },
      { lat: current.lat, lon: current.lon },
    );
    if (distance > evidence.maxHopKm) return -Infinity;
    score -= evidence.weights.dist * (distance / evidence.distanceDecayKm);
  }
  score += evidence.weights.edge
    * evidence.directedEdgeScore(previous.nodeId, current.nodeId);
  score += evidence.weights.transition
    * Math.log1p(Math.max(0, evidence.transitionProbability(previous.nodeId, current.nodeId)));
  score += evidence.weights.motif
    * evidence.motifProbability(previous.nodeId, current.nodeId);
  if (evidence.hasObservedLink(previous.nodeId, current.nodeId)) {
    score += evidence.weights.link;
  }
  if (evidence.weights.positionConditionalTransition !== 0
      && evidence.positionConditionalTransition) {
    score += evidence.weights.positionConditionalTransition
      * Math.log1p(Math.max(0,
        evidence.positionConditionalTransition(position, previous.nodeId, current.nodeId),
      ));
  }
  if (evidence.weights.itmViability !== 0
      && evidence.isItmViable?.(previous.nodeId, current.nodeId)) {
    score += evidence.weights.itmViability;
  }
  return score;
}

function buildColumn(
  position: number,
  pathLength: number,
  hash: string,
  prevHash: string | null,
  evidence: PathDecoderEvidence,
): DecoderCandidate[] {
  const raw = evidence.candidatesByHash.get(hash) ?? [];
  let candidates: DecoderCandidate[] = raw.map((candidate) => ({ ...candidate }));
  if (candidates.length > evidence.maxColumnCandidates) {
    const anchors = evidence.directAnchors(position, hash);
    candidates = candidates
      .map((candidate) => ({
        candidate,
        score: emissionScore(position, pathLength, hash, prevHash, candidate, anchors, evidence),
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, evidence.maxColumnCandidates)
      .map(({ candidate }) => candidate);
  }
  // The experiment always chose an argmax. Keep NULL only as a structural
  // fallback for a genuinely empty column; ambiguity remains a marginal flag.
  if (candidates.length === 0) candidates.push(UNRESOLVED_CANDIDATE);
  return candidates;
}

function endpointCandidate(anchor: ObserverAnchor): DecoderCandidate {
  return {
    nodeId: anchor.nodeId ?? null,
    name: null,
    lat: anchor.lat,
    lon: anchor.lon,
  };
}

function bestTerminalTransition(
  position: number,
  candidate: DecoderCandidate,
  evidence: PathDecoderEvidence,
): number {
  const terminals = evidence.terminalAnchors ?? [];
  if (terminals.length === 0) return 0;
  let best = -Infinity;
  for (const terminal of terminals) {
    best = Math.max(best, transitionScore(position, candidate, endpointCandidate(terminal), evidence));
  }
  return best;
}

/** Decode a coherent node assignment for every canonical path-hash position. */
export function decodePath(
  canonicalHashes: readonly string[],
  evidence: PathDecoderEvidence,
): Map<number, DecodedHop> {
  const pathLength = canonicalHashes.length;
  const decoded = new Map<number, DecodedHop>();
  if (pathLength === 0) return decoded;

  const columns: DecoderCandidate[][] = [];
  const emissions: number[][] = [];
  for (let position = 0; position < pathLength; position++) {
    const hash = canonicalHashes[position]!;
    const prevHash = position > 0 ? (canonicalHashes[position - 1] ?? null) : null;
    const anchors = evidence.directAnchors(position, hash);
    const column = buildColumn(position, pathLength, hash, prevHash, evidence);
    columns.push(column);
    emissions.push(column.map((candidate) => emissionScore(
      position,
      pathLength,
      hash,
      prevHash,
      candidate,
      anchors,
      evidence,
    )));
  }

  // Best prefix score ending at columns[position][candidate].
  const forward: number[][] = columns.map((column) => new Array(column.length).fill(-Infinity));
  for (let candidate = 0; candidate < columns[0]!.length; candidate++) {
    const startTransition = evidence.sourceAnchor
      ? transitionScore(0, endpointCandidate(evidence.sourceAnchor), columns[0]![candidate]!, evidence)
      : 0;
    if (isFinite(startTransition) && isFinite(emissions[0]![candidate]!)) {
      forward[0]![candidate] = emissions[0]![candidate]! + startTransition;
    }
  }
  for (let position = 1; position < pathLength; position++) {
    for (let current = 0; current < columns[position]!.length; current++) {
      const currentEmission = emissions[position]![current]!;
      if (!isFinite(currentEmission)) continue;
      let best = -Infinity;
      for (let previous = 0; previous < columns[position - 1]!.length; previous++) {
        if (!isFinite(forward[position - 1]![previous]!)) continue;
        const transition = transitionScore(
          position,
          columns[position - 1]![previous]!,
          columns[position]![current]!,
          evidence,
        );
        if (!isFinite(transition)) continue;
        const total = forward[position - 1]![previous]! + transition;
        if (total > best) best = total;
      }
      if (isFinite(best)) forward[position]![current] = best + currentEmission;
    }
  }

  // Best suffix score starting immediately after columns[position][candidate].
  const backward: number[][] = columns.map((column) => new Array(column.length).fill(-Infinity));
  for (let candidate = 0; candidate < columns[pathLength - 1]!.length; candidate++) {
    backward[pathLength - 1]![candidate] = bestTerminalTransition(
      pathLength,
      columns[pathLength - 1]![candidate]!,
      evidence,
    );
  }
  for (let position = pathLength - 2; position >= 0; position--) {
    for (let current = 0; current < columns[position]!.length; current++) {
      let best = -Infinity;
      for (let next = 0; next < columns[position + 1]!.length; next++) {
        const nextEmission = emissions[position + 1]![next]!;
        if (!isFinite(nextEmission) || !isFinite(backward[position + 1]![next]!)) continue;
        const transition = transitionScore(
          position + 1,
          columns[position]![current]!,
          columns[position + 1]![next]!,
          evidence,
        );
        if (!isFinite(transition)) continue;
        const total = transition + nextEmission + backward[position + 1]![next]!;
        if (total > best) best = total;
      }
      backward[position]![current] = isFinite(best) ? best : 0;
    }
  }

  for (let position = 0; position < pathLength; position++) {
    const hash = canonicalHashes[position]!;
    let bestCandidate = -1;
    let bestMarginal = -Infinity;
    let secondMarginal = -Infinity;
    for (let candidate = 0; candidate < columns[position]!.length; candidate++) {
      if (!isFinite(forward[position]![candidate]!)
          || !isFinite(backward[position]![candidate]!)) continue;
      const marginal = forward[position]![candidate]! + backward[position]![candidate]!;
      if (marginal > bestMarginal) {
        secondMarginal = bestMarginal;
        bestMarginal = marginal;
        bestCandidate = candidate;
      } else if (marginal > secondMarginal) {
        secondMarginal = marginal;
      }
    }
    const best = bestCandidate >= 0
      ? columns[position]![bestCandidate]!
      : UNRESOLVED_CANDIDATE;
    const margin = isFinite(secondMarginal) ? bestMarginal - secondMarginal : Infinity;
    decoded.set(position, {
      hash,
      nodeId: best.nodeId,
      name: best.name,
      lat: best.lat,
      lon: best.lon,
      margin,
      ambiguous: best.nodeId !== null && isFinite(secondMarginal)
        && margin < evidence.ambiguityDelta,
    });
  }

  return decoded;
}
