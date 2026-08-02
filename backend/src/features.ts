const DISABLED_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);

export function envFlagEnabled(value: string | undefined, defaultValue = true): boolean {
  if (value === undefined || value.trim() === '') return defaultValue;
  return !DISABLED_VALUES.has(value.trim().toLowerCase());
}

export type PublicRuntimeFeatureConfig = {
  version: 1;
  inferredNodes: boolean;
  packetArcs: boolean;
  heatmap: boolean;
  privacyGeneration: number;
  refreshAfterSeconds: number;
};

const MIN_RUNTIME_FEATURE_REFRESH_SECONDS = 5;
const MAX_RUNTIME_FEATURE_REFRESH_SECONDS = 300;

function boundedRefreshSeconds(value: string | undefined): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 30;
  return Math.min(
    MAX_RUNTIME_FEATURE_REFRESH_SECONDS,
    Math.max(MIN_RUNTIME_FEATURE_REFRESH_SECONDS, Math.trunc(parsed)),
  );
}

/**
 * Public, presentation-only feature configuration. Every risky map layer is
 * deliberately fail-closed and is read for each request so a backend
 * recreation with updated environment values does not require a frontend
 * image rebuild.
 */
export function getPublicRuntimeFeatureConfig(
  env: NodeJS.ProcessEnv = process.env,
  privacyGeneration = 0,
): PublicRuntimeFeatureConfig {
  return {
    version: 1,
    inferredNodes: envFlagEnabled(env['PUBLIC_FEATURE_INFERRED_NODES_ENABLED'], false),
    packetArcs: envFlagEnabled(env['PUBLIC_FEATURE_PACKET_ARCS_ENABLED'], false),
    heatmap: envFlagEnabled(env['PUBLIC_FEATURE_HEATMAP_ENABLED'], false),
    privacyGeneration: Number.isSafeInteger(privacyGeneration) && privacyGeneration >= 0
      ? privacyGeneration
      : 0,
    refreshAfterSeconds: boundedRefreshSeconds(env['PUBLIC_FEATURE_CONFIG_TTL_SECONDS']),
  };
}
