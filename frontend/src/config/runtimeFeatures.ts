import { useSyncExternalStore } from 'react';

export type RuntimeFeatureSnapshot = {
  packetArcs: boolean;
  heatmap: boolean;
  privacyGeneration: number;
  refreshAfterSeconds: number;
};

type RuntimeFeatureResponse = RuntimeFeatureSnapshot & {
  version: 1;
};

const DEFAULT_REFRESH_SECONDS = 30;
const MIN_REFRESH_SECONDS = 5;
const MAX_REFRESH_SECONDS = 300;
const REQUEST_TIMEOUT_MS = 3_000;

export const FAIL_CLOSED_RUNTIME_FEATURES: RuntimeFeatureSnapshot = Object.freeze({
  packetArcs: false,
  heatmap: false,
  privacyGeneration: 0,
  refreshAfterSeconds: DEFAULT_REFRESH_SECONDS,
});

let snapshot: RuntimeFeatureSnapshot = FAIL_CLOSED_RUNTIME_FEATURES;
const listeners = new Set<() => void>();

function sameSnapshot(a: RuntimeFeatureSnapshot, b: RuntimeFeatureSnapshot): boolean {
  return a.packetArcs === b.packetArcs
    && a.heatmap === b.heatmap
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
    || value['refreshAfterSeconds'] < MIN_REFRESH_SECONDS
    || value['refreshAfterSeconds'] > MAX_REFRESH_SECONDS) {
    return FAIL_CLOSED_RUNTIME_FEATURES;
  }

  const parsed = value as RuntimeFeatureResponse;
  return {
    packetArcs: parsed.packetArcs,
    heatmap: parsed.heatmap,
    privacyGeneration: parsed.privacyGeneration,
    refreshAfterSeconds: parsed.refreshAfterSeconds,
  };
}

export async function refreshRuntimeFeatures(
  fetchImpl: typeof fetch = globalThis.fetch,
): Promise<RuntimeFeatureSnapshot> {
  try {
    const response = await fetchImpl('/api/runtime-config', {
      cache: 'no-store',
      credentials: 'same-origin',
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) throw new Error(`runtime config request failed: ${response.status}`);
    const payload = await response.json() as unknown;
    const parsed = parseRuntimeFeatureConfig(payload);
    return publish(parsed);
  } catch {
    return publish(FAIL_CLOSED_RUNTIME_FEATURES);
  }
}

export function initializeRuntimeFeatures(): Promise<RuntimeFeatureSnapshot> {
  return refreshRuntimeFeatures();
}

export function startRuntimeFeaturePolling(): () => void {
  let stopped = false;
  let running = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  const clearScheduled = () => {
    if (timer !== null) globalThis.clearTimeout(timer);
    timer = null;
  };

  const schedule = () => {
    clearScheduled();
    if (stopped || document.visibilityState !== 'visible') return;
    timer = globalThis.setTimeout(() => {
      timer = null;
      void refreshOnce();
    }, snapshot.refreshAfterSeconds * 1_000);
  };

  const refreshOnce = async () => {
    if (stopped || running || document.visibilityState !== 'visible') return;
    running = true;
    try {
      await refreshRuntimeFeatures();
    } finally {
      running = false;
      schedule();
    }
  };

  const onVisibilityChange = () => {
    if (document.visibilityState !== 'visible') {
      clearScheduled();
      return;
    }
    void refreshOnce();
  };

  document.addEventListener('visibilitychange', onVisibilityChange);
  schedule();
  return () => {
    stopped = true;
    clearScheduled();
    document.removeEventListener('visibilitychange', onVisibilityChange);
  };
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
