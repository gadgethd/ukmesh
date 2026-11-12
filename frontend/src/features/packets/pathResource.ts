import { fetchJson, withScopeParams } from '../../utils/api.js';
import { scopeIsReady, scopeKey, type PublicScope } from '../../utils/scope.js';
import { RequestLimiter } from '../../utils/requestLimiter.js';
import { ScopedCache } from '../../utils/scopedCache.js';
import { feedPathCacheKey, type FeedPacket, type LazyPathResult } from './feedModel.js';

const requests = new RequestLimiter(1, 1);
export const feedPathCache = new ScopedCache<LazyPathResult>({
  name: 'feed-selected-paths', ttlMs: 5 * 60_000, maxEntries: 128,
  maxBytes: 16 * 1024 * 1024, maxInflight: 2, loadTimeoutMs: 15_000,
});

export function loadFeedPath(packet: FeedPacket, scope: PublicScope, consumer: AbortSignal): Promise<LazyPathResult> {
  const key = scopeKey(scope);
  feedPathCache.activateScope(key);
  if (!scopeIsReady(scope)) return Promise.reject(new DOMException('Privacy authority is unavailable', 'AbortError'));
  return feedPathCache.getOrLoad(key, feedPathCacheKey(packet), signal => requests.run(signal, () =>
    fetchJson<LazyPathResult>(withScopeParams(`/api/path-lazy/resolve?hash=${encodeURIComponent(packet.packet_hash)}`, scope),
      { cache: 'no-store', signal }, { timeoutMs: 15_000, maxBytes: 4 * 1024 * 1024 })), consumer);
}
