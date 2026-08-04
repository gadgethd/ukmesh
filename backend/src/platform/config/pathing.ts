export const pathingConfig = {
  maxBetaHops: 25,
  purpleThreshold: 0.45,
  earthEffectiveRadiusM: 6_371_000 / (1 - 0.25),
  defaultAntennaHeightM: 15,
  behindEarthToleranceKm: 25,
  // Physics is a bounded tiebreaker; full-strength transition physics reduced
  // the bake-off accuracy by overwhelming the tuned evidence model.
  physicsSoftMarginWeight: 0.05,
  prefixAmbiguityFloorKm: 45,
  weakLinkPathlossMaxDb: 137.5,
  looseLinkPathlossMaxDb: 146.0,
  impossibleLinkPathlossDb: 165.0,
  maxHopKm: 100,
  contextTtlMs: 900_000,
  modelLimit: 6000,
  maxRenderPermutations: 24,
  maxPermutationStates: 200_000,
  softFallbackHopKm: 60,
  observerHopWeightConfirmed: 0.18,
  observerHopWeightReachable: 0.22,
  observerHopWeightFallback: 0.20,
  anchorConfidenceDefault: 0.65,
  // Slow mode: wait out the packet propagation window before the final
  // multi-observer resolution (real spread first->last observer p50 11s,
  // p95 34s, p99 50s). Env overrides: PATH_SLOW_MODE_ENABLED /
  // PATH_SLOW_MODE_WINDOW_MS / PATH_SLOW_MODE_MIN_HOPS.
  slowModeEnabled: (process.env['PATH_SLOW_MODE_ENABLED'] ?? 'true') !== 'false',
  slowModeWindowMs: Math.min(
    300_000,
    Math.max(1_000, Number(process.env['PATH_SLOW_MODE_WINDOW_MS'] ?? 60_000) || 60_000),
  ),
  slowModeMinPathHops: Math.min(
    21,
    Math.max(1, Number(process.env['PATH_SLOW_MODE_MIN_HOPS'] ?? 2) || 2),
  ),
} as const;
