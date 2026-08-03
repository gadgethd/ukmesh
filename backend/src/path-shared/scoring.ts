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
  mlDominantCap: number;
  mlWeakCap: number;
  anchor: number;
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
  prefix: 0.30, // historical prefix→node probability
  mlDominantCap: 0.20, // ML score >= mlDominantThreshold
  mlWeakCap: 0.06, // ML score below the dominant threshold
  anchor: 0.12, // proximity to a direct-receiver observer anchor (0..1)
  // Reserved for Phase 4. Kept inert until the matching evidence is loaded.
  corridorInterpolation: 0, // source→receiver corridor position fit
  // Transition (how well two adjacent nodes form a real hop)
  edge: 0.28, // path_edge_priors directed score
  transition: 0.25, // path_transition_priors directed probability
  // Reserved for Phase 4. Kept inert until the matching evidence is loaded.
  positionConditionalTransition: 0, // (position, from)→to probability
  motif: 0.18, // path_motif_priors 2-gram directed probability
  link: 0.20, // confirmed node_links pair
  // Reserved for Phase 4. Bonus-only; never use ITM viability as a hard gate.
  itmViability: 0,
  dist: 0.10, // distance shaping exp(-d/22), only when both ends are positioned
};

/** ML score at/above this is treated as strong (dominant) evidence. */
export const ML_DOMINANT_THRESHOLD = 0.85;

/** Lowest ML prefix score loaded as candidate evidence. */
export const ML_SCORE_LOAD_THRESHOLD = 0.80;

/** Distance-decay constant (km) for the transition distance-shaping term. */
export const DIST_DECAY_KM = 22;

/** Baseline score for the synthetic "unresolved" candidate at every position.
 * A real candidate must accumulate at least this much positive evidence to be
 * chosen over leaving the position unresolved — this is what stops the decoder
 * from guessing on pure geography with no supporting evidence. */
export const NULL_BASELINE = 0.06;

/** Two candidates whose best-path marginals differ by less than this are
 * flagged ambiguous. */
export const AMBIG_DELTA = 0.12;

/** Max candidate nodes kept per hash position (trellis column cap). */
export const MAX_COL = 24;
