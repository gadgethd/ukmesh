import { ApiInputError } from '../errors.js';

function singleString(value: unknown, name: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'string') {
    throw new ApiInputError(`${name} must be supplied once`, 'AMBIGUOUS_PARAMETER');
  }
  return value;
}

export function parseBoundedInteger(
  value: unknown,
  options: {
    name: string;
    defaultValue?: number;
    min: number;
    max: number;
  },
): number {
  const raw = singleString(value, options.name);
  if (raw === undefined || raw === '') {
    if (options.defaultValue !== undefined) return options.defaultValue;
    throw new ApiInputError(`${options.name} is required`);
  }
  if (!/^(?:0|[1-9]\d*)$/.test(raw)) {
    throw new ApiInputError(`${options.name} must be a canonical integer`);
  }
  const parsed = Number(raw);
  if (
    !Number.isSafeInteger(parsed)
    || parsed < options.min
    || parsed > options.max
  ) {
    throw new ApiInputError(
      `${options.name} must be between ${options.min} and ${options.max}`,
    );
  }
  return parsed;
}

export function parseBoundedString(
  value: unknown,
  options: {
    name: string;
    required?: boolean;
    minLength?: number;
    maxLength: number;
    pattern?: RegExp;
  },
): string | undefined {
  const raw = singleString(value, options.name);
  if (raw === undefined || raw.trim() === '') {
    if (options.required) throw new ApiInputError(`${options.name} is required`);
    return undefined;
  }
  const parsed = raw.trim();
  if (
    parsed.length < (options.minLength ?? 1)
    ||
    parsed.length > options.maxLength
    || /[\u0000-\u001f\u007f]/.test(parsed)
    || (options.pattern && !options.pattern.test(parsed))
  ) {
    throw new ApiInputError(`${options.name} is invalid`);
  }
  return parsed;
}

export function parseHexIdentifier(
  value: unknown,
  options: { name: string; minLength?: number; maxLength: number },
): string {
  return parseBoundedString(value, {
    name: options.name,
    required: true,
    maxLength: options.maxLength,
    pattern: new RegExp(`^[0-9a-fA-F]{${options.minLength ?? 1},${options.maxLength}}$`),
  })!;
}

export function parseCoordinate(
  value: unknown,
  options: { name: string; min: number; max: number },
): number {
  const raw = singleString(value, options.name);
  if (raw === undefined || raw.trim() === '') {
    throw new ApiInputError(`${options.name} is required`);
  }
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new ApiInputError(`${options.name} must be a finite decimal`);
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < options.min || parsed > options.max) {
    throw new ApiInputError(
      `${options.name} must be between ${options.min} and ${options.max}`,
    );
  }
  return parsed;
}

export function parseBoundedFloat(
  value: unknown,
  options: {
    name: string;
    defaultValue?: number;
    min: number;
    max: number;
  },
): number {
  const raw = singleString(value, options.name);
  if (raw === undefined || raw === '') {
    if (options.defaultValue !== undefined) return options.defaultValue;
    throw new ApiInputError(`${options.name} is required`);
  }
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new ApiInputError(`${options.name} must be a canonical finite decimal`);
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < options.min || parsed > options.max) {
    throw new ApiInputError(
      `${options.name} must be between ${options.min} and ${options.max}`,
    );
  }
  return parsed;
}

export function parseBoolean(
  value: unknown,
  options: { name: string; defaultValue?: boolean },
): boolean {
  const raw = singleString(value, options.name);
  if (raw === undefined || raw === '') return options.defaultValue ?? false;
  if (raw === '1' || raw === 'true') return true;
  if (raw === '0' || raw === 'false') return false;
  throw new ApiInputError(`${options.name} must be true, false, 1, or 0`);
}

export function parseEnum<T extends string>(
  value: unknown,
  options: {
    name: string;
    values: readonly T[];
    defaultValue?: T;
  },
): T | undefined {
  const raw = singleString(value, options.name);
  if (raw === undefined || raw === '') return options.defaultValue;
  if ((options.values as readonly string[]).includes(raw)) return raw as T;
  throw new ApiInputError(
    `${options.name} must be one of ${options.values.join(', ')}`,
  );
}

export function parseCursor(
  value: unknown,
  options: { name: string; maxLength?: number } = { name: 'cursor' },
): string | undefined {
  return parseBoundedString(value, {
    name: options.name,
    maxLength: options.maxLength ?? 1024,
    pattern: /^[A-Za-z0-9_-]+$/,
  });
}
