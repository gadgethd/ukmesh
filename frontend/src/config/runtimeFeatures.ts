import { useSyncExternalStore } from 'react';
import { fetchJson } from '../utils/api.js';
import { POLLING } from './polling.js';
import { createVisibilityPoller } from '../utils/visibilityPoller.js';

export type RuntimeFeatureSnapshot = {
  packetArcs: boolean;
  heatmap: boolean;
  regionScoping: boolean;
  privacyGeneration: number;
  refreshAfterSeconds: number;
};

type RuntimeFeatureResponse = RuntimeFeatureSnapshot & {
  version: 1;
};


export const FAIL_CLOSED_RUNTIME_FEATURES: RuntimeFeatureSnapshot = Object.freeze({
  packetArcs: false,
  heatmap: false,
  regionScoping: false,
  privacyGeneration: 0,
  refreshAfterSeconds: POLLING.runtimeDefaultSeconds,
});

let snapshot: RuntimeFeatureSnapshot = FAIL_CLOSED_RUNTIME_FEATURES;
const listeners = new Set<() => void>();

function sameSnapshot(a: RuntimeFeatureSnapshot, b: RuntimeFeatureSnapshot): boolean {
  return a.packetArcs === b.packetArcs
    && a.heatmap === b.heatmap
    && a.regionScoping === b.regionScoping
    && a.privacyGeneration === b.privacyGeneration
    && a.refreshAfterSeconds === b.refreshAfterSeconds;
}

function publish(next: RuntimeFeatureSnapshot): RuntimeFeatureSnapshot {
  if (sameSnapshot(snapshot, next)) return snapshot;
  snapshot = Object.freeze(next);
  for (const listener of listeners) listener();
  return snapshot;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function parseRuntimeFeatureConfig(value: unknown): RuntimeFeatureSnapshot {
  if (!isRecord(value)
    || value['version'] !== 1
    || typeof value['packetArcs'] !== 'boolean'
    || typeof value['heatmap'] !== 'boolean'
    || typeof value['privacyGeneration'] !== 'number'
    || !Number.isSafeInteger(value['privacyGeneration'])
    || value['privacyGeneration'] < 0
    || typeof value['refreshAfterSeconds'] !== 'number'
    || !Number.isInteger(value['refreshAfterSeconds'])
    || value['refreshAfterSeconds'] < POLLING.runtimeMinSeconds
    || value['refreshAfterSeconds'] > POLLING.runtimeMaxSeconds) {
    return FAIL_CLOSED_RUNTIME_FEATURES;
  }

  const parsed = value as RuntimeFeatureResponse;
  return {
    packetArcs: parsed.packetArcs,
    heatmap: parsed.heatmap,
    // Older servers omit the field. Only an explicit true enables Phase B.
    regionScoping: value['regionScoping'] === true,
    privacyGeneration: parsed.privacyGeneration,
    refreshAfterSeconds: parsed.refreshAfterSeconds,
  };
}

export async function refreshRuntimeFeatures(
  fetchImpl: typeof fetch = globalThis.fetch,
  signal?: AbortSignal,
): Promise<RuntimeFeatureSnapshot> {
  try {
    const payload = await fetchJson<unknown>('/api/runtime-config', {
      cache: 'no-store',
      credentials: 'same-origin',
      signal,
    }, { timeoutMs: POLLING.runtimeTimeout, maxBytes: 16 * 1024, fetchImpl });
    if (signal?.aborted) return signal.reason?.name === 'TimeoutError' ? publish(FAIL_CLOSED_RUNTIME_FEATURES) : snapshot;
    const parsed = parseRuntimeFeatureConfig(payload);
    return publish(parsed);
  } catch {
    if (signal?.aborted) return signal.reason?.name === 'TimeoutError' ? publish(FAIL_CLOSED_RUNTIME_FEATURES) : snapshot;
    return publish(FAIL_CLOSED_RUNTIME_FEATURES);
  }
}

export function initializeRuntimeFeatures(): Promise<RuntimeFeatureSnapshot> {
  return refreshRuntimeFeatures();
}

export function startRuntimeFeaturePolling(): () => void {
  const poller = createVisibilityPoller({
    poll: async signal => { await refreshRuntimeFeatures(globalThis.fetch, signal); },
    intervalMs: () => snapshot.refreshAfterSeconds * 1_000,
    timeoutMs: POLLING.runtimeTimeout,
    immediate: false,
    // Privacy authority refreshes use the server's bounded cadence, without jitter.
    jitterRatio: 0,
    isVisible: () => document.visibilityState === 'visible',
    subscribeVisibility: listener => {
      document.addEventListener('visibilitychange', listener);
      return () => document.removeEventListener('visibilitychange', listener);
    },
  });
  return poller.stop;
}

export function getRuntimeFeatureSnapshot(): RuntimeFeatureSnapshot {
  return snapshot;
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useRuntimeFeatures(): RuntimeFeatureSnapshot {
  return useSyncExternalStore(subscribe, getRuntimeFeatureSnapshot, getRuntimeFeatureSnapshot);
}

export function resetRuntimeFeaturesForTests(): void {
  publish(FAIL_CLOSED_RUNTIME_FEATURES);
}
