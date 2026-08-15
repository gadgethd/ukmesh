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
  elevationM?: number | null;
};

export type CandidateNodeEvidence = {
  nodeId: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  elevationM?: number | null;
};

export type ObserverAnchor = GeographicPoint & { nodeId?: string; elevationM?: number | null };

export type PhysicalHopLimits = {
  maxHopKm: number;
  earthEffectiveRadiusM: number;
  behindEarthToleranceKm: number;
  impossibleLinkPathlossDb: number;
  pathlossDb?: number | null;
  antennaHeightM?: number;
};

export type PhysicalHopEvaluation = {
  possible: boolean;
  distanceKm: number;
  horizonMarginKm: number | null;
  softMargin: number;
};

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
  /** Optional fixed node assignment at a trellis position. */
  fixed?: ReadonlyMap<number, string>;
  /** Optional hard-physics gate plus small ranking-only score. */
  physicalTransition?: (
    from: CandidateNode,
    to: CandidateNode,
    distanceKm: number,
  ) => { possible: boolean; score: number };
  /** Endpoint anchors use only hard gates plus the bounded physics tiebreaker. */
  endpointTransitionsGateOnly?: boolean;
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
  elevationM?: number | null;
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

/** Evaluate calibrated hard-impossibility gates for one radio hop.
 * Missing elevation deliberately disables only the curvature gate. */
export function evaluatePhysicalHop(
  from: GeographicPoint & { elevationM?: number | null },
  to: GeographicPoint & { elevationM?: number | null },
  limits: PhysicalHopLimits,
): PhysicalHopEvaluation {
  const distance = distanceKm(from, to);
  const lossImpossible = limits.pathlossDb != null
    && Number.isFinite(limits.pathlossDb)
    && limits.pathlossDb > limits.impossibleLinkPathlossDb;
  let horizonMarginKm: number | null = null;
  if (from.elevationM != null && to.elevationM != null
      && Number.isFinite(from.elevationM) && Number.isFinite(to.elevationM)) {
    const antennaHeightM = limits.antennaHeightM ?? 15;
    const radiusKm = limits.earthEffectiveRadiusM / 1_000;
    const fromHeightKm = Math.max(0, from.elevationM + antennaHeightM) / 1_000;
    const toHeightKm = Math.max(0, to.elevationM + antennaHeightM) / 1_000;
    const horizonKm = Math.sqrt(2 * radiusKm * fromHeightKm)
      + Math.sqrt(2 * radiusKm * toHeightKm);
    horizonMarginKm = horizonKm - distance;
  }
  const curvatureImpossible = horizonMarginKm != null
    && horizonMarginKm < -limits.behindEarthToleranceKm;
  const possible = distance <= limits.maxHopKm && !lossImpossible && !curvatureImpossible;
  return {
    possible,
    distanceKm: distance,
    horizonMarginKm,
    // Ranking only: bounded so physics cannot swamp the tuned evidence.
    softMargin: horizonMarginKm == null
      ? 0
      : Math.max(-1, Math.min(1, horizonMarginKm / limits.behindEarthToleranceKm)),
  };
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
  let directPhysicsScore = 0;
  if (anchors.length > 0) {
    if (!positioned) return -Infinity;
    if (minDistanceToSet({ lat: candidate.lat!, lon: candidate.lon! }, anchors) > evidence.maxHopKm) {
      return -Infinity;
    }
    if (evidence.physicalTransition) {
      let bestPhysics = -Infinity;
      for (const anchor of anchors) {
        const physics = evidence.physicalTransition(
          candidate as CandidateNode,
          endpointCandidate(anchor) as CandidateNode,
          distanceKm(candidate as CandidateNode, anchor),
        );
        if (physics.possible) bestPhysics = Math.max(bestPhysics, physics.score);
      }
      if (!isFinite(bestPhysics)) return -Infinity;
      directPhysicsScore = bestPhysics;
    }
  }

  let score = evidence.weights.prefix
    * Math.log1p(Math.max(0, evidence.prefixProbability(candidate.nodeId, hash, prevHash)));
  score += directPhysicsScore;
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
    if (evidence.physicalTransition) {
      const physics = evidence.physicalTransition(
        previous as CandidateNode,
        current as CandidateNode,
        distance,
      );
      if (!physics.possible) return -Infinity;
      score += physics.score;
    }
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

function endpointTransitionScore(
  from: DecoderCandidate,
  to: DecoderCandidate,
  evidence: PathDecoderEvidence,
): number {
  if (!evidence.endpointTransitionsGateOnly) return transitionScore(0, from, to, evidence);
  if (from.lat == null || from.lon == null || to.lat == null || to.lon == null) return -Infinity;
  const distance = distanceKm(from as CandidateNode, to as CandidateNode);
  if (distance > evidence.maxHopKm) return -Infinity;
  if (!evidence.physicalTransition) return 0;
  const physics = evidence.physicalTransition(from as CandidateNode, to as CandidateNode, distance);
  return physics.possible ? physics.score : -Infinity;
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
  const fixedNodeId = evidence.fixed?.get(position);
  if (fixedNodeId) candidates = candidates.filter((candidate) => candidate.nodeId === fixedNodeId);
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
    elevationM: anchor.elevationM,
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
    best = Math.max(best, evidence.endpointTransitionsGateOnly
      ? endpointTransitionScore(candidate, endpointCandidate(terminal), evidence)
      : transitionScore(position, candidate, endpointCandidate(terminal), evidence));
  }
  return best;
}

/** Decode a coherent node assignment for every canonical path-hash position. */
export type DecodedPathResult = { hops: Map<number, DecodedHop>; score: number };

/** Score one fully specified path without running a trellis traversal. */
export function scoreFixedPath(
  canonicalHashes: readonly string[],
  nodeIds: readonly string[],
  evidence: PathDecoderEvidence,
): number {
  if (canonicalHashes.length === 0 || nodeIds.length !== canonicalHashes.length) return -Infinity;
  const chosen: DecoderCandidate[] = [];
  let score = 0;
  for (let position = 0; position < canonicalHashes.length; position++) {
    const hash = canonicalHashes[position]!;
    const previousHash = position > 0 ? canonicalHashes[position - 1] ?? null : null;
    const candidate = (evidence.candidatesByHash.get(hash) ?? [])
      .find((value) => value.nodeId === nodeIds[position]);
    if (!candidate) return -Infinity;
    const emission = emissionScore(
      position,
      canonicalHashes.length,
      hash,
      previousHash,
      candidate,
      evidence.directAnchors(position, hash),
      evidence,
    );
    if (!isFinite(emission)) return -Infinity;
    score += emission;
    const previous = chosen[position - 1];
    const transition = previous
      ? transitionScore(position, previous, candidate, evidence)
      : evidence.sourceAnchor
        ? endpointTransitionScore(endpointCandidate(evidence.sourceAnchor), candidate, evidence)
        : 0;
    if (!isFinite(transition)) return -Infinity;
    score += transition;
    chosen.push(candidate);
  }
  const terminal = bestTerminalTransition(
    canonicalHashes.length,
    chosen[chosen.length - 1]!,
    evidence,
  );
  return isFinite(terminal) ? score + terminal : -Infinity;
}

export function decodePathWithScore(
  canonicalHashes: readonly string[],
  evidence: PathDecoderEvidence,
): DecodedPathResult {
  const pathLength = canonicalHashes.length;
  const decoded = new Map<number, DecodedHop>();
  if (pathLength === 0) return { hops: decoded, score: 0 };

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
  const previousCandidate: number[][] = columns.map((column) => new Array(column.length).fill(-1));
  for (let candidate = 0; candidate < columns[0]!.length; candidate++) {
    const startTransition = evidence.sourceAnchor
      ? endpointTransitionScore(endpointCandidate(evidence.sourceAnchor), columns[0]![candidate]!, evidence)
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
      let bestPrevious = -1;
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
        if (total > best) {
          best = total;
          bestPrevious = previous;
        }
      }
      if (isFinite(best)) {
        forward[position]![current] = best + currentEmission;
        previousCandidate[position]![current] = bestPrevious;
      }
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
      backward[position]![current] = best;
    }
  }

  // Backtrack one globally coherent Viterbi chain. Max-marginal argmaxes are
  // useful for confidence, but choosing them independently can splice two
  // equally-scored physical paths into an adjacent hop that was never viable.
  let score = -Infinity;
  let terminalCandidate = -1;
  for (let candidate = 0; candidate < columns[pathLength - 1]!.length; candidate++) {
    const total = forward[pathLength - 1]![candidate]!
      + backward[pathLength - 1]![candidate]!;
    if (total > score) {
      score = total;
      terminalCandidate = candidate;
    }
  }
  const chosenCandidates = new Array<number>(pathLength).fill(-1);
  if (terminalCandidate >= 0 && isFinite(score)) {
    chosenCandidates[pathLength - 1] = terminalCandidate;
    for (let position = pathLength - 1; position > 0; position--) {
      const chosen = chosenCandidates[position]!;
      if (chosen < 0) break;
      chosenCandidates[position - 1] = previousCandidate[position]![chosen]!;
    }
  }

  for (let position = 0; position < pathLength; position++) {
    const hash = canonicalHashes[position]!;
    const chosenCandidate = chosenCandidates[position]!;
    const chosenMarginal = chosenCandidate >= 0
      ? forward[position]![chosenCandidate]! + backward[position]![chosenCandidate]!
      : -Infinity;
    let bestAlternativeMarginal = -Infinity;
    for (let candidate = 0; candidate < columns[position]!.length; candidate++) {
      if (candidate === chosenCandidate) continue;
      if (!isFinite(forward[position]![candidate]!)
          || !isFinite(backward[position]![candidate]!)) continue;
      const marginal = forward[position]![candidate]! + backward[position]![candidate]!;
      if (marginal > bestAlternativeMarginal) bestAlternativeMarginal = marginal;
    }
    const best = chosenCandidate >= 0
      ? columns[position]![chosenCandidate]!
      : UNRESOLVED_CANDIDATE;
    const margin = isFinite(bestAlternativeMarginal)
      ? chosenMarginal - bestAlternativeMarginal
      : Infinity;
    decoded.set(position, {
      hash,
      nodeId: best.nodeId,
      name: best.name,
      lat: best.lat,
      lon: best.lon,
      margin,
      ambiguous: best.nodeId !== null && isFinite(bestAlternativeMarginal)
        && margin < evidence.ambiguityDelta,
    });
  }
  return { hops: decoded, score };
}

/** Decode a coherent node assignment for every canonical path-hash position. */
export function decodePath(
  canonicalHashes: readonly string[],
  evidence: PathDecoderEvidence,
): Map<number, DecodedHop> {
  return decodePathWithScore(canonicalHashes, evidence).hops;
}
