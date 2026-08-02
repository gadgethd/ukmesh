import type { IncomingHttpHeaders } from 'node:http';

export type NetworkScope = 'ukmesh' | 'test' | 'all';
type ForcedScope = Exclude<NetworkScope, 'all'>;
export type PublicNetworkScope = Exclude<NetworkScope, 'all'>;

export type VisibilityScope = Readonly<{
  access: 'public';
  network: PublicNetworkScope;
  observer?: string;
}>;

export class PublicAllScopeForbiddenError extends Error {
  constructor() {
    super('PUBLIC_ALL_NETWORK_SCOPE_FORBIDDEN');
    this.name = 'PublicAllScopeForbiddenError';
  }
}

export class InvalidPublicNetworkScopeError extends Error {
  constructor() {
    super('INVALID_PUBLIC_NETWORK_SCOPE');
    this.name = 'InvalidPublicNetworkScopeError';
  }
}

export function normalizeNetworkValue(value: unknown): NetworkScope | undefined {
  const normalized = String(value ?? '').trim().toLowerCase();
  if (normalized === 'all') return 'all';
  // Legacy production labels stay compatible with the unified UKMesh scope.
  if (normalized === 'ukmesh' || normalized === 'teesside' || normalized === 'northeast') {
    return 'ukmesh';
  }
  if (normalized === 'test') return 'test';
  return undefined;
}

export function inferForcedNetwork(_headers: IncomingHttpHeaders): ForcedScope | undefined {
  // With a single unified network there is no host-based network forcing; the
  // explicit query param or the caller's fallback decides scope.
  return undefined;
}

export function resolveRequestNetwork(
  requested: unknown,
  headers: IncomingHttpHeaders,
  fallback?: Exclude<NetworkScope, 'all'>,
): NetworkScope | undefined {
  const forced = inferForcedNetwork(headers);
  if (forced) return forced;
  const normalized = normalizeNetworkValue(requested);
  return normalized ?? fallback;
}

export function resolvePublicNetworkScope(
  requested: unknown,
  headers: IncomingHttpHeaders,
  fallback: PublicNetworkScope = 'ukmesh',
): PublicNetworkScope {
  if (
    requested !== undefined
    && requested !== null
    && String(requested).trim() !== ''
    && normalizeNetworkValue(requested) === undefined
  ) {
    throw new InvalidPublicNetworkScopeError();
  }
  const scope = resolveRequestNetwork(requested, headers, fallback) ?? fallback;
  if (scope === 'all') throw new PublicAllScopeForbiddenError();
  return scope;
}

export function resolvePublicVisibilityScope(
  requested: unknown,
  headers: IncomingHttpHeaders,
  observer?: string,
): VisibilityScope {
  const network = resolvePublicNetworkScope(requested, headers);
  return Object.freeze({
    access: 'public',
    network,
    ...(observer ? { observer } : {}),
  });
}
