import { ApiInputError } from '../errors.js';

export function normalizeObserverQuery(value: unknown): string | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value !== 'string') {
    throw new ApiInputError('observer must be supplied once', 'AMBIGUOUS_PARAMETER');
  }
  const observer = value.trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(observer)) {
    throw new ApiInputError('observer must be a 64-character hexadecimal identifier');
  }
  return observer;
}
