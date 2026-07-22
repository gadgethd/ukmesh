const DISABLED_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);

function envFlagEnabled(value: string | undefined, defaultValue = true): boolean {
  if (value === undefined || value.trim() === '') return defaultValue;
  return !DISABLED_VALUES.has(value.trim().toLowerCase());
}

export const VIEWSHED_ENABLED = envFlagEnabled(import.meta.env.VITE_VIEWSHED_ENABLED, false);
