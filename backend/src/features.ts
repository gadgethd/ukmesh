const DISABLED_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);

export function envFlagEnabled(value: string | undefined, defaultValue = true): boolean {
  if (value === undefined || value.trim() === '') return defaultValue;
  return !DISABLED_VALUES.has(value.trim().toLowerCase());
}

export function isViewshedFeatureEnabled(): boolean {
  return envFlagEnabled(process.env['VIEWSHED_ENABLED'], false);
}
