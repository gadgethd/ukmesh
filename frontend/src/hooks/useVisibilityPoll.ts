import { useCallback, useEffect, useRef } from 'react';

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
  const pollerRef = useRef<ReturnType<typeof createVisibilityPoller> | null>(null);
  const pollRef = useRef(poll);
  const onErrorRef = useRef(options.onError);
  pollRef.current = poll;
  onErrorRef.current = options.onError;

  useEffect(() => {
    if (options.enabled === false) return undefined;
    const poller = createVisibilityPoller({
      poll: (signal) => pollRef.current(signal),
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
      onError: (error) => onErrorRef.current?.(error),
    });
    pollerRef.current = poller;
    return () => {
      poller.stop();
      if (pollerRef.current === poller) pollerRef.current = null;
    };
  }, [
    options.enabled,
    options.immediate,
    options.intervalMs,
    options.jitterRatio,
    options.maxBackoffMs,
    options.scopeKey,
    options.timeoutMs,
  ]);
  return useCallback(() => { void pollerRef.current?.trigger(); }, []);
}
