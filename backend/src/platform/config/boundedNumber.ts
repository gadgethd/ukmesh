export function boundedIntegerSetting(
  name: string,
  rawValue: string | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  if (!Number.isSafeInteger(fallback) || fallback < minimum || fallback > maximum) {
    throw new Error(`Invalid fallback for ${name}`);
  }
  if (rawValue === undefined || rawValue.trim() === '') return fallback;
  const normalized = rawValue.trim();
  if (!/^(?:0|[1-9][0-9]*)$/.test(normalized)) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return parsed;
}

/** Lenient bounded positive integer read: invalid or non-positive values fall
 *  back instead of failing startup. Use only for tuning knobs where the
 *  fallback is safe. */
export function positiveIntegerSetting(
  name: string,
  fallback: number,
  max: number,
  rawValue: string | undefined = process.env[name],
): number {
  const parsed = Number(rawValue);
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(max, Math.floor(parsed)) : fallback;
}
