const DISABLED_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);

function envFlagEnabled(value: string | undefined, defaultValue = true): boolean {
  if (value === undefined || value.trim() === '') return defaultValue;
  return !DISABLED_VALUES.has(value.trim().toLowerCase());
}

// Legacy per-node viewshed and planner UI are retired. Keep this compile-time
// false through the rollback release so no environment toggle can revive the
// rejected API consumers.
export const VIEWSHED_ENABLED = false;
export const RF_COVERAGE_ENABLED = envFlagEnabled(
  import.meta.env.VITE_RF_COVERAGE_ENABLED,
  false,
);
export const PACKET_ARCS_CAPABLE = envFlagEnabled(
  import.meta.env.VITE_PACKET_ARCS_ENABLED,
  true,
);
export const HEATMAP_CAPABLE = envFlagEnabled(
  import.meta.env.VITE_HEATMAP_ENABLED,
  true,
);
