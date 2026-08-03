/**
 * Shared scoring weights and prior key-formats for the path resolvers.
 *
 * The shared decoder scores candidate hops with prefix/transition/edge/motif
 * priors, ML scores, node_links, and hop distance. The lazy resolver uses it
 * today; the beta resolvers will adopt it in later extraction phases.
 *
 * This module is the single source of truth for:
 *   - the prior key formats (must match how `path-learning/rebuild.ts` stores them);
 *   - the relative scoring weights used by the Viterbi decoder.
 *
 * `path-beta/resolver.ts` predates this module and still has its own inline
 * weights; later phases will converge it here. New code should import from here.
 */

/** Generous but realistic upper bound for a single LoRa hop, in km. */
export const MAX_HOP_KM = 150;

/**
 * Directed transition / edge key: source-side node → receiver-side node.
 * `rebuild.ts` stores priors with from = node closer to the source and
 * to = node closer to the receiver (fullNodes = [src, relay0..relayN, rx]).
 */
export function transitionKey(fromNodeId: string, toNodeId: string): string {
  return `${fromNodeId}|${toNodeId}`;
}

/** Directed 2-gram motif key, matching rebuild.ts `${from}>${to}`. */
export function motif2Key(fromNodeId: string, toNodeId: string): string {
  return `${fromNodeId}>${toNodeId}`;
}

/**
 * Weights consumed by the shared Viterbi decoder. Evidence providers may pass
 * a different table for experiments, but production resolvers should use
 * {@link SCORE} so there is one tuning surface.
 */
export type PathScoreWeights = Readonly<{
  prefix: number;
  positionPrefixFrequency: number;
  mlDominantCap: number;
  mlWeakCap: number;
  observerDistance: number;
  anchor: number;
  corridorPrior: number;
  corridorInterpolation: number;
  edge: number;
  transition: number;
  positionConditionalTransition: number;
  motif: number;
  link: number;
  itmViability: number;
  dist: number;
}>;

/** Scoring weights for the shared Viterbi decoder. */
export const SCORE: PathScoreWeights = {
  // Emission (how well a node fits a hash position on its own)
  prefix: 0.3, // log(1 + count) global 1-byte frequency backoff
  positionPrefixFrequency: 1.2, // log(1 + count) for (byte-prefix, position, node)
  mlDominantCap: 0, // the champion did not use the ML candidate scorer
  mlWeakCap: 0,
  observerDistance: 1.0, // -distance to the receiving observer / 80 km
  anchor: 0.9, // direct multi-observer position anchor, distance-shaped over 80 km
  corridorPrior: 1.0, // log(1 + count) for (source, receiver, position, node)
  corridorInterpolation: 1.0, // -distance to source→receiver interpolated point / 55 km
  // Transition (how well two adjacent nodes form a real hop)
  edge: 0,
  transition: 2.0, // log(1 + global directed transition count)
  positionConditionalTransition: 1.2, // log(1 + count) for (position, from)→to
  motif: 0,
  link: 0,
  itmViability: 0.8, // bonus-only; never a hard gate
  dist: 1.0, // -distance / DIST_DECAY_KM
};

/** ML score at/above this is treated as strong (dominant) evidence. */
export const ML_DOMINANT_THRESHOLD = 0.85;

/** Lowest ML prefix score loaded as candidate evidence. */
export const ML_SCORE_LOAD_THRESHOLD = 0.80;

/** Distance-decay constant (km) for the transition distance-shaping term. */
export const DIST_DECAY_KM = 40;

/** Corridor interpolation distance divisor from the champion experiment. */
export const CORRIDOR_INTERPOLATION_KM = 55;

/** Direct-observer anchor distance divisor from the champion experiment. */
export const OBSERVER_DISTANCE_DECAY_KM = 80;

/** Structural score used only when a position has no real candidates. */
export const NULL_BASELINE = 0;

/** Two candidates whose best-path marginals differ by less than this are
 * flagged ambiguous. */
export const AMBIG_DELTA = 0.12;

/** Max candidate nodes kept per hash position (trellis column cap). */
export const MAX_COL = 64;
