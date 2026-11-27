import type { WSReadyState } from '../realtime/protocol.js';

export type PathTreeStatus = 'idle' | 'unavailable' | 'settling' | 'loading' | 'ready' | 'error';
export type FeedConnectionStatus =
  | 'live'
  | 'connected-quiet'
  | 'reconnecting'
  | 'offline-cached'
  | 'offline';

export function feedConnectionStatus(
  readyState: WSReadyState,
  lastMessageAt: number | null,
  hasCachedPackets: boolean,
  now = Date.now(),
): FeedConnectionStatus {
  if (readyState === 'connected') {
    return lastMessageAt !== null && now - lastMessageAt < 60_000
      ? 'live'
      : 'connected-quiet';
  }
  if (readyState === 'connecting') return 'reconnecting';
  return hasCachedPackets ? 'offline-cached' : 'offline';
}

export function feedConnectionLabel(status: FeedConnectionStatus): string {
  if (status === 'live') return 'Live';
  if (status === 'connected-quiet') return 'Connected · quiet';
  if (status === 'reconnecting') return 'Reconnecting…';
  if (status === 'offline-cached') return 'Offline · cached data';
  return 'Offline';
}

export function feedConnectionDot(status: FeedConnectionStatus): 'live' | 'stale' | 'dead' {
  if (status === 'live') return 'live';
  if (status === 'connected-quiet' || status === 'reconnecting') return 'stale';
  return 'dead';
}

export function initialPathTreeStatus(
  hasPathHashes: boolean,
  hasCachedPath: boolean,
): PathTreeStatus {
  if (!hasPathHashes) return 'unavailable';
  return hasCachedPath ? 'ready' : 'idle';
}

export function validatedRegionSelection(
  selected: string,
  options: readonly string[],
  optionsStatus: 'loading' | 'ready' | 'failed',
): string {
  if (selected === 'all' || optionsStatus !== 'ready') return selected;
  return options.includes(selected) ? selected : 'all';
}

export function parseFeedRegions(
  value: unknown,
  scope: { network?: string; observer?: string; privacyGeneration: number },
): string[] | null {
  if (value === null || typeof value !== 'object') return null;
  const { network, observer, visibilityGeneration, regions } = value as Record<string, unknown>;
  if (network !== (scope.network ?? 'ukmesh')
    || observer !== (scope.observer?.toUpperCase() ?? null)
    || visibilityGeneration !== scope.privacyGeneration
    || !Array.isArray(regions)
    || !regions.every(region => typeof region === 'string' && /^[A-Z0-9]{2,8}$/.test(region))) return null;
  return regions;
}
