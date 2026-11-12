import type { ApiScope } from './api.js';

export type PublicScope = Readonly<ApiScope & { privacyGeneration: number }>;

/** Defaults and identifier normalization belong to the scope's resolver. */
export function scopeKey(scope: PublicScope): string {
  return JSON.stringify([
    scope.network ?? null,
    scope.observer ?? null,
    scope.region ?? null,
    scope.privacyGeneration,
  ]);
}

export function scopeIsReady(scope: PublicScope): boolean {
  return Number.isSafeInteger(scope.privacyGeneration) && scope.privacyGeneration > 0;
}
