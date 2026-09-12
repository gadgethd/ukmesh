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

export type PublicScopeHandshakeResult =
  | { ok: true }
  | { ok: false; statusCode: 400; reason: PublicScopeHandshakeReason; message: string };

export type PublicScopeHandshakeReason =
  | 'invalid_scope'
  | 'all_scope_forbidden'
  | 'unexpected_error';

/**
 * Total, fail-closed validation for HTTP/WebSocket handshakes. This function
 * never throws: every failure is returned as a bounded rejection reason so a
 * caller's upgrade callback can always respond exactly once instead of leaking
 * an exception into the process-level uncaughtException handler.
 */
export function validatePublicNetworkScopeForHandshake(
  requested: unknown,
  headers: IncomingHttpHeaders,
): PublicScopeHandshakeResult {
  try {
    resolvePublicNetworkScope(requested, headers);
    return { ok: true };
  } catch (error) {
    if (error instanceof PublicAllScopeForbiddenError) {
      return {
        ok: false,
        statusCode: 400,
        reason: 'all_scope_forbidden',
        message: 'The all-network scope is not available',
      };
    }
    if (error instanceof InvalidPublicNetworkScopeError) {
      return {
        ok: false,
        statusCode: 400,
        reason: 'invalid_scope',
        message: 'Invalid network scope',
      };
    }
    return {
      ok: false,
      statusCode: 400,
      reason: 'unexpected_error',
      message: 'Bad Request',
    };
  }
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
