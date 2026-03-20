import { pathingConfig } from '../platform/config/pathing.js';

export const MAX_BETA_HOPS = pathingConfig.maxBetaHops;
export const BETA_PURPLE_THRESHOLD = pathingConfig.purpleThreshold;
export const R_EFF_M = pathingConfig.earthEffectiveRadiusM;
export const PREFIX_AMBIGUITY_FLOOR_KM = pathingConfig.prefixAmbiguityFloorKm;
export const WEAK_LINK_PATHLOSS_MAX_DB = pathingConfig.weakLinkPathlossMaxDb;
export const LOOSE_LINK_PATHLOSS_MAX_DB = pathingConfig.looseLinkPathlossMaxDb;
// Hard block threshold: path loss high enough to indicate a genuine terrain barrier
// above the current conservative physical-link model.
export const IMPOSSIBLE_LINK_PATHLOSS_DB = pathingConfig.impossibleLinkPathlossDb;
export const MAX_HOP_KM = pathingConfig.maxHopKm;
export const CONTEXT_TTL_MS = pathingConfig.contextTtlMs; // 15 minutes - nodes/links rarely change
export const MODEL_LIMIT = pathingConfig.modelLimit;
export const MAX_PERMUTATION_HOP_KM = MAX_HOP_KM;
export const MAX_RENDER_PERMUTATIONS = pathingConfig.maxRenderPermutations;
export const MAX_PERMUTATION_STATES = pathingConfig.maxPermutationStates; // Increased - more complete searches
export const SOFT_FALLBACK_HOP_KM = pathingConfig.softFallbackHopKm;
export const OBSERVER_HOP_WEIGHT_CONFIRMED = pathingConfig.observerHopWeightConfirmed;
export const OBSERVER_HOP_WEIGHT_REACHABLE = pathingConfig.observerHopWeightReachable;
export const OBSERVER_HOP_WEIGHT_FALLBACK = pathingConfig.observerHopWeightFallback;
export const ANCHOR_CONFIDENCE_DEFAULT = pathingConfig.anchorConfidenceDefault;
