import { pathingConfig } from '../platform/config/pathing.js';

export const MAX_BETA_HOPS = pathingConfig.maxBetaHops;
export const BETA_PURPLE_THRESHOLD = pathingConfig.purpleThreshold;
export const CONTEXT_TTL_MS = pathingConfig.contextTtlMs; // 15 minutes - nodes/links rarely change
export const MODEL_LIMIT = pathingConfig.modelLimit;
