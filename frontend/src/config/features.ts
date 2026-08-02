const DISABLED_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);

function envFlagEnabled(value: string | undefined, defaultValue = true): boolean {
  if (value === undefined || value.trim() === '') return defaultValue;
  return !DISABLED_VALUES.has(value.trim().toLowerCase());
}

export const VIEWSHED_ENABLED = envFlagEnabled(import.meta.env.VITE_VIEWSHED_ENABLED, false);
export const INFERRED_NODES_CAPABLE = envFlagEnabled(
  import.meta.env.VITE_INFERRED_NODES_ENABLED,
  true,
);
export const PACKET_ARCS_CAPABLE = envFlagEnabled(
  import.meta.env.VITE_PACKET_ARCS_ENABLED,
  true,
);
export const HEATMAP_CAPABLE = envFlagEnabled(
  import.meta.env.VITE_HEATMAP_ENABLED,
  true,
);
