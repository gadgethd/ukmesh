import type { IncomingHttpHeaders } from 'node:http';

type NetworkScope = 'teesside' | 'ukmesh' | 'test' | 'all';
type ForcedScope = Exclude<NetworkScope, 'ukmesh'>;

function firstHeaderValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] ?? '') : String(value ?? '');
}

function extractHostname(raw: string): string {
  const trimmed = raw.trim().toLowerCase();
  if (!trimmed) return '';
  try {
    return new URL(trimmed).hostname.toLowerCase();
  } catch {
    return trimmed.split(',')[0]?.trim().split(':')[0]?.toLowerCase() ?? '';
  }
}

export function normalizeNetworkValue(value: unknown): NetworkScope | undefined {
  const normalized = String(value ?? '').trim().toLowerCase();
  if (normalized === 'ukmesh' || normalized === 'teesside' || normalized === 'test' || normalized === 'all') {
    return normalized;
  }
  return undefined;
}

export function inferForcedNetwork(headers: IncomingHttpHeaders): ForcedScope | undefined {
  const host = extractHostname(firstHeaderValue(headers['x-forwarded-host']) || firstHeaderValue(headers['host']));
  const origin = extractHostname(firstHeaderValue(headers['origin']));
  const referer = extractHostname(firstHeaderValue(headers['referer']));
  const candidates = [host, origin, referer];

  if (candidates.some((value) => value === 'test.ukmesh.com' || value.endsWith('.test.ukmesh.com'))) return 'test';
  if (candidates.some((value) => value === 'app.ukmesh.com' || value === 'www.ukmesh.com' || value === 'ukmesh.com')) return 'all';
  if (candidates.some((value) => value === 'app.teessidemesh.com' || value === 'www.teessidemesh.com' || value === 'teessidemesh.com')) return 'teesside';
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
