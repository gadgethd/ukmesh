import { uncachedEndpoint, withScopeParams } from '../../utils/api.js';
import { fetchPathResolution } from '../../utils/pathResolutionRequest.js';
import type { AggregatedPredictionState, MultiObserverBetaResponse } from './model.js';

type Pending = { status: 'pending'; remainingMs: number };
const pending = (value: MultiObserverBetaResponse | Pending): value is Pending =>
  (value as Pending).status === 'pending' && typeof (value as Pending).remainingMs === 'number';

function wait(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const aborted = () => { clearTimeout(timer); reject(signal.reason); };
    const timer = setTimeout(() => { signal.removeEventListener('abort', aborted); resolve(); }, ms);
    signal.addEventListener('abort', aborted, { once: true });
  });
}

/** One dashboard session owns request lifetimes; eager and post-window work stay distinct. */
export function createPredictionResource(initialScope: string, request = fetchPathResolution, now = Date.now) {
  let scope = initialScope;
  let generation = 0;
  const cache = new Map<string, { response: MultiObserverBetaResponse; ts: number }>();
  const recent = new Map<string, AggregatedPredictionState>();
  const active = new Map<string, { promise: Promise<MultiObserverBetaResponse | null>; controller: AbortController }>();
  function prune<Value extends { ts: number }>(values: Map<string, Value>, ttl: number, maximum: number) {
    for (const [key, value] of values) if (now() - value.ts > ttl) values.delete(key);
    const overflow = values.size - maximum;
    if (overflow > 0) {
      const oldest = [...values.entries()].sort((a, b) => a[1].ts - b[1].ts);
      for (let index = 0; index < overflow; index++) values.delete(oldest[index]![0]);
    }
  }
  const clear = () => {
    generation++;
    cache.clear(); recent.clear();
    for (const job of active.values()) job.controller.abort();
  };
  async function fetchResult(hash: string, network: string | undefined, mode: 'slow' | undefined, signal: AbortSignal) {
    for (let attempt = 0; attempt < (mode === 'slow' ? 3 : 1); attempt++) {
      if (signal.aborted) throw signal.reason;
      const endpoint = withScopeParams(`/api/path-beta/resolve-multi?hash=${encodeURIComponent(hash)}${mode ? '&mode=slow' : ''}`, { network });
      const response = await request(uncachedEndpoint(endpoint), { cache: 'no-store', signal });
      if (!response.ok) return null;
      const value = await response.json() as MultiObserverBetaResponse | Pending;
      if (!pending(value)) return value;
      if (mode !== 'slow') return null;
      await wait(Math.min(Math.max(value.remainingMs + 500, 1_000), 30_000), signal);
    }
    return null;
  }
  const resolve = (requestScope: string, hash: string, observerIds: string[], network?: string,
    minFreshTs = 0, mode?: 'slow'): Promise<MultiObserverBetaResponse | null> => {
    if (requestScope !== scope) return Promise.resolve(null);
    prune(cache, 120_000, 1200);
    const key = JSON.stringify([network ?? null, [...observerIds].sort(), hash]);
    if (mode !== 'slow') {
      const hit = cache.get(key);
      if (hit && hit.ts >= minFreshTs) return Promise.resolve(hit.response);
    }
    const token = generation;
    const inflightKey = JSON.stringify([token, key, mode ?? 'eager']);
    const previous = active.get(inflightKey);
    if (previous) return previous.promise;
    if (active.size >= 2) return Promise.resolve(null);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), mode === 'slow' ? 126_000 : 12_000);
    const promise = fetchResult(hash, network, mode, controller.signal)
      .then(response => {
        if (!controller.signal.aborted && generation === token && response) {
          cache.set(key, { response, ts: now() });
          prune(cache, 120_000, 1200);
          return response;
        }
        return null;
      })
      .catch(() => null)
      .finally(() => { clearTimeout(timer); active.delete(inflightKey); });
    active.set(inflightKey, { promise, controller });
    return promise;
  };
  return {
    resolve,
    setScope(next: string) { if (next !== scope) { clear(); scope = next; } },
    dispose: clear,
    remember(hash: string, value: AggregatedPredictionState) { recent.set(hash, value); prune(recent, 45_000, 48); },
    recent(hash: string) { prune(recent, 45_000, 48); return recent.get(hash); },
    snapshot() { prune(cache, 120_000, 1200); prune(recent, 45_000, 48); return { entries: cache.size, recent: recent.size, inflight: active.size }; },
  };
}
