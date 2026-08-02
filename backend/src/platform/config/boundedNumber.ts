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
