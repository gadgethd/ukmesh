import { createHash, timingSafeEqual } from 'node:crypto';

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

export function operatorTokenIsConfigured(expected: string | undefined): expected is string {
  return typeof expected === 'string' && expected.length >= 32;
}

export function verifyOperatorToken(expected: string | undefined, provided: string | undefined): boolean {
  if (!operatorTokenIsConfigured(expected) || typeof provided !== 'string') return false;
  return timingSafeEqual(digest(expected), digest(provided));
}
