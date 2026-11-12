import { fetchJson, withScopeParams } from '../../utils/api.js';
import { scopeIsReady, scopeKey, type PublicScope } from '../../utils/scope.js';
import { ScopedCache } from '../../utils/scopedCache.js';
import type { NodeLink } from './model.js';

const cache = new ScopedCache<NodeLink[]>({
  name: 'map-node-links', ttlMs: 5 * 60_000,
  maxEntries: 512, maxBytes: 8 * 1024 * 1024, maxInflight: 8,
});

export function activateNodeLinksScope(scope: PublicScope): void {
  cache.activateScope(scopeKey(scope));
}

export function fetchNodeLinks(nodeId: string, scope: PublicScope, consumerSignal?: AbortSignal): Promise<NodeLink[]> {
  activateNodeLinksScope(scope);
  if (!scopeIsReady(scope)) return Promise.reject(new DOMException('Privacy authority is unavailable', 'AbortError'));
  return cache.getOrLoad(scopeKey(scope), nodeId.toUpperCase(), async signal => {
    const payload = await fetchJson<unknown>(
      withScopeParams(`/api/nodes/${encodeURIComponent(nodeId)}/links`, scope),
      { signal }, { timeoutMs: 15_000, maxBytes: 2 * 1024 * 1024 },
    );
    if (!Array.isArray(payload)) throw new Error('node links response was not an array');
    return payload as NodeLink[];
  }, consumerSignal);
}
