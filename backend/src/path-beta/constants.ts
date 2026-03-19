export const MAX_BETA_HOPS = 25;
export const BETA_PURPLE_THRESHOLD = 0.45;
export const R_EFF_M = 6_371_000 / (1 - 0.25);
export const PREFIX_AMBIGUITY_FLOOR_KM = 45;
export const WEAK_LINK_PATHLOSS_MAX_DB = 145.0;
export const LOOSE_LINK_PATHLOSS_MAX_DB = 146.0;
// Hard block threshold: path loss high enough to indicate a genuine terrain barrier
// (well above the loose threshold — links just over 138dB may still work in practice)
export const IMPOSSIBLE_LINK_PATHLOSS_DB = 165.0;
export const MAX_HOP_KM = 100;
export const CONTEXT_TTL_MS = 900_000; // 15 minutes - nodes/links rarely change
export const MODEL_LIMIT = 6000;
export const MAX_PERMUTATION_HOP_KM = MAX_HOP_KM;
export const MAX_RENDER_PERMUTATIONS = 24;
export const MAX_PERMUTATION_STATES = 200_000; // Increased - more complete searches
export const SOFT_FALLBACK_HOP_KM = 60;
export const OBSERVER_HOP_WEIGHT_CONFIRMED = 0.18;
export const OBSERVER_HOP_WEIGHT_REACHABLE = 0.22;
export const OBSERVER_HOP_WEIGHT_FALLBACK = 0.20;
export const ANCHOR_CONFIDENCE_DEFAULT = 0.65;
