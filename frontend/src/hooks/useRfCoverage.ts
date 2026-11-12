import { availableRfCoverageTiers, rfCoveragePollingPolicy, type RfPollingOptions, type RfCoverageMeta, type RfCoverageProgress, type RfCoverageTierName } from '../features/rf/model.js';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useCommittedRef } from './useCommittedRef.js';
import { useVisibilityPoll } from './useVisibilityPoll.js';
import { POLLING } from '../config/polling.js';
import { fetchJson } from '../utils/api.js';

function fetchRfJson<T>(path: string, signal: AbortSignal): Promise<T> {
  return fetchJson<T>(path, { cache: 'no-store', credentials: 'same-origin', signal }, {
    timeoutMs: POLLING.ordinaryTimeout,
    maxBytes: 4 * 1024 * 1024,
  });
}

export function useRfCoverage(enabled: boolean, options: RfPollingOptions = { layerVisible: enabled }): {
  meta: RfCoverageMeta | null;
  progress: RfCoverageProgress | null;
  availableTiers: RfCoverageTierName[];
} {
  const [meta, setMeta] = useState<RfCoverageMeta | null>(null);
  const [progress, setProgress] = useState<RfCoverageProgress | null>(null);
  const [discovered, setDiscovered] = useState(false);
  const policy = rfCoveragePollingPolicy(enabled, options, meta, discovered);
  const intervalRef = useCommittedRef(policy.intervalMs);
  const interval = useCallback(() => intervalRef.current, []);

  useEffect(() => {
    if (enabled) return;
    setMeta(null); setProgress(null); setDiscovered(false);
  }, [enabled]);

  useVisibilityPoll(async signal => {
    const stamp = Date.now();
    const [nextMeta, nextProgress] = await Promise.allSettled([
      fetchRfJson<RfCoverageMeta>(`/rf-coverage/meta.json?poll=${stamp}`, signal),
      fetchRfJson<RfCoverageProgress>(`/rf-coverage/progress.json?poll=${stamp}`, signal),
    ]);
    if (signal.aborted) return;
    // The scheduling policy reads this ref before React's next render too.
    if (nextMeta.status === 'fulfilled' && typeof nextMeta.value?.generated_at === 'string') {
      setMeta(nextMeta.value);
      intervalRef.current = rfCoveragePollingPolicy(enabled, options, nextMeta.value, true).intervalMs;
    }
    if (nextProgress.status === 'fulfilled' && typeof nextProgress.value?.stage === 'string') setProgress(nextProgress.value);
    setDiscovered(true);
    if (nextMeta.status === 'rejected') throw nextMeta.reason;
    if (nextProgress.status === 'rejected') throw nextProgress.reason;
  }, {
    enabled: policy.enabled,
    scopeKey: `rf:${options.layerVisible}:${options.nodeKey ?? 'network'}:${!!options.nodeRequestVisible}`,
    intervalMs: interval,
    timeoutMs: POLLING.ordinaryTimeout,
    maxBackoffMs: POLLING.rfIdle * 4,
  });

  const availableTiers = useMemo(() => availableRfCoverageTiers(meta), [meta]);
  return { meta, progress, availableTiers };
}
