import { useCallback, useEffect, useLayoutEffect, useMemo, useRef } from 'react';
import { useCommittedRef } from './useCommittedRef.js';

import { createVisibilityPoller } from '../utils/visibilityPoller.js';

export type UseVisibilityPollOptions = {
  enabled?: boolean;
  scopeKey: string;
  intervalMs: number | (() => number);
  timeoutMs: number;
  maxBackoffMs?: number;
  jitterRatio?: number;
  immediate?: boolean;
  onError?: (error: unknown) => void;
};

export function useVisibilityPoll(
  poll: (signal: AbortSignal) => Promise<void>,
  options: UseVisibilityPollOptions,
): () => void {
  const pollerRef = useRef<{ owner: object; poller: ReturnType<typeof createVisibilityPoller> } | null>(null);
  const pollRef = useCommittedRef(poll);
  const onErrorRef = useCommittedRef(options.onError);
  const owner = useMemo(() => ({ active: false }), [options.enabled, options.immediate,
    options.intervalMs, options.jitterRatio, options.maxBackoffMs, options.scopeKey, options.timeoutMs]);
  useLayoutEffect(() => {
    owner.active = true;
    return () => {
      owner.active = false;
      if (pollerRef.current?.owner === owner) pollerRef.current.poller.stop();
    };
  }, [owner]);

  useEffect(() => {
    if (options.enabled === false || !owner.active) return undefined;
    const poller = createVisibilityPoller({
      poll: (signal) => owner.active ? pollRef.current(signal) : Promise.resolve(),
      intervalMs: options.intervalMs,
      timeoutMs: options.timeoutMs,
      maxBackoffMs: options.maxBackoffMs,
      jitterRatio: options.jitterRatio,
      immediate: options.immediate,
      isVisible: () => document.visibilityState === 'visible',
      subscribeVisibility: (listener) => {
        document.addEventListener('visibilitychange', listener);
        return () => document.removeEventListener('visibilitychange', listener);
      },
      onError: (error) => { if (owner.active) onErrorRef.current?.(error); },
    });
    pollerRef.current = { owner, poller };
    return () => {
      poller.stop();
      if (pollerRef.current?.poller === poller) pollerRef.current = null;
    };
  }, [owner]);
  return useCallback(() => { void pollerRef.current?.poller.trigger(); }, []);
}
